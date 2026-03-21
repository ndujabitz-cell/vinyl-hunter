import os
import asyncio
import io
import json
import base64
import re
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# 1. INIZIALIZZAZIONE
app = FastAPI()

# 2. CONFIGURAZIONE (Railway Env Vars)
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL     = os.getenv("ADMIN_EMAIL", "")

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

def DISCOGS_HEADERS():
    return {"Authorization": f"Discogs token={DISCOGS_TOKEN}", "User-Agent": "VinylHunter/1.0"}

def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

# 3. MODELLI DATI
class LoginData(BaseModel):
    email: str
    password: str

class VinylData(BaseModel):
    user_id: str
    access_token: str
    artista: str
    titolo: Optional[str] = ""
    formato: Optional[str] = ""
    stile: Optional[str] = ""
    anno: Optional[str] = ""
    etichetta: Optional[str] = ""
    stampa: Optional[str] = ""
    stampa_costosa: Optional[str] = ""
    prezzo_max: Optional[str] = ""

class VinylUpdate(BaseModel):
    token: Optional[str] = ""
    artista: Optional[str] = None
    titolo: Optional[str] = None
    formato: Optional[str] = None
    stile: Optional[str] = None
    anno: Optional[str] = None
    etichetta: Optional[str] = None
    stampa: Optional[str] = None

# 4. IL CUORE: MOTORE DI RICERCA DISCOGS AVANZATO
def extract_barcode(s: str) -> str:
    s_clean = re.sub(r'[\s\-]', '', str(s or ''))
    matches = re.findall(r'\d{8,13}', s_clean)
    for m in matches:
        if len(m) in (8, 12, 13): return m
    return ""

async def cerca_su_discogs_full(data: dict, barcode: str = ""):
    artista = str(data.get("artista") or "").strip()
    titolo = str(data.get("titolo") or "").strip()
    stampa = str(data.get("stampa") or "").strip()
    
    async with httpx.AsyncClient(timeout=25) as client:
        # TENTATIVO 1: BARCODE
        if barcode:
            r = await client.get("https://api.discogs.com/database/search", 
                                 headers=DISCOGS_HEADERS(), params={"barcode": barcode, "type": "release"})
            res = r.json().get("results", [])
            if res: return res[0]

        # TENTATIVO 2: CATALOGO (STAMPA) con pulizia OCR
        if stampa:
            stampa_clean = stampa.replace("O", "0")
            r = await client.get("https://api.discogs.com/database/search", 
                                 headers=DISCOGS_HEADERS(), params={"catno": stampa_clean, "type": "release"})
            res = r.json().get("results", [])
            if res: return res[0]

        # TENTATIVO 3: ARTISTA + TITOLO
        if artista and titolo:
            art_clean = artista.lower().replace("the ", "").strip()
            r = await client.get("https://api.discogs.com/database/search", 
                                 headers=DISCOGS_HEADERS(), params={"artist": art_clean, "release_title": titolo, "type": "release"})
            res = r.json().get("results", [])
            if res: return res[0]
    return None

# 5. ENDPOINTS
@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    b64 = base64.b64encode(content).decode()
    prompt = "Analizza l'etichetta del vinile e rispondi SOLO JSON: {\"artista\":\"\",\"titolo\":\"\",\"formato\":\"\",\"stile\":\"\",\"anno\":\"\",\"etichetta\":\"\",\"stampa\":\"\",\"barcode\":\"\",\"lato\":\"\"}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": file.content_type or "image/jpeg", "data": b64}}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.1}
    }

    async with httpx.AsyncClient(timeout=45) as client:
        r = await client.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload)
        if r.status_code != 200: return {"_error": "gemini_fail"}
        try:
            g_data = json.loads(r.json()['candidates'][0]['content']['parts'][0]['text'].strip().replace("```json", "").replace("```", ""))
        except: return {"_error": "parse_fail"}

    barcode_val = extract_barcode(g_data.get("barcode", ""))
    match = await cerca_su_discogs_full(g_data, barcode=barcode_val)
    if match:
        g_data.update({
            "artista": match.get("title", "").split(" - ")[0],
            "titolo": match.get("title", "").split(" - ")[-1],
            "anno": match.get("year", ""),
            "stampa": match.get("catno", ""),
            "stile": ", ".join(match.get("style", [])[:2])
        })
    
    g_data["catno"] = g_data.get("stampa", "")
    return g_data

@app.post("/api/login")
async def login(data: LoginData):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{SUPABASE_URL}/auth/v1/token?grant_type=password", headers=supa_headers(), json=data.dict())
    if r.status_code != 200: raise HTTPException(401, "Login fallito")
    res = r.json()
    is_admin = (ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())
    return {"access_token": res["access_token"], "user_id": res["user"]["id"], "is_admin": is_admin}

@app.get("/api/vinili/{user_id}")
async def get_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=created_at.desc", headers=supa_headers(token))
    return r.json()

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        await client.post(f"{SUPABASE_URL}/rest/v1/vinili", headers=supa_headers(v.access_token), json=v.dict(exclude={"access_token"}))
    return {"status": "ok"}

@app.get("/api/export/excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=artista.asc", headers=supa_headers(token))
    
    vinyls = r.json()
    wb = Workbook()
    ws = wb.active
    ws.title = "Collezione"
    ws.freeze_panes = "A2" # BLOCCO RIGA 1

    headers = ["ARTISTA", "TITOLO", "FORMATO", "ETICHETTA", "ANNO", "STAMPA", "STILE", "VALUTAZIONE"]
    ws.append(headers)
    
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="333333", end_color="333333", fill_type="solid")
        cell.alignment = Alignment(horizontal="center")

    for v in vinyls:
        ws.append([v.get("artista"), v.get("titolo"), v.get("formato"), v.get("etichetta"), v.get("anno"), v.get("stampa"), v.get("stile"), v.get("prezzo_max")])

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return StreamingResponse(out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename=collezione.xlsx"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
