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
from typing import Optional
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from collections import Counter

app = FastAPI()

# ── CONFIGURAZIONE ────────────────────────────────────────────────────────────
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL     = os.getenv("ADMIN_EMAIL", "")

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

DISCOGS_HEADERS = lambda: {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "VinylHunter/1.0"
}

# ── MODELLI DATI ─────────────────────────────────────────────────────────────
class RegisterData(BaseModel):
    email: str
    password: str
    nome: str

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
    stampa_costosa: Optional[str] = None
    prezzo_max: Optional[str] = None

class EnrichRequest(BaseModel):
    artista: Optional[str] = ""
    titolo: Optional[str] = ""
    formato: Optional[str] = ""
    stile: Optional[str] = ""
    anno: Optional[str] = ""
    etichetta: Optional[str] = ""
    stampa: Optional[str] = ""
    token: Optional[str] = ""

# ── HELPERS SUPABASE ──────────────────────────────────────────────────────────
def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

# ── LOGICA CACHE ─────────────────────────────────────────────────────────────
def cache_key(artista: str, titolo: str) -> str:
    return f"{(artista or '').strip().lower()}|{(titolo or '').strip().lower()}"

async def cache_get(key: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={**supa_headers(use_secret=True), "Accept": "application/json"},
                params={"cache_key": f"eq.{key}", "limit": "1"}
            )
            if r.status_code == 200 and r.json(): return r.json()[0]
    except: pass
    return None

async def cache_set(key: str, data: dict):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={**supa_headers(use_secret=True), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={"cache_key": key, **{k: v for k, v in data.items() if k != "access_token"}}
            )
    except: pass

# ── HELPERS DISCOGS AVANZATI ──────────────────────────────────────────────────
def formato_to_discogs(fmt: str) -> str:
    f = fmt.lower()
    if "12" in f: return '12"'
    if "7" in f or "45" in f: return '7"'
    if "lp" in f or "33" in f: return "LP"
    return "Vinyl"

def extract_barcode(s: str) -> str:
    s_clean = re.sub(r'[\s\-]', '', s or '')
    match = re.search(r'\d{8,13}', s_clean)
    return match.group(0) if match else ""

async def _discogs_search(client, params: dict):
    params.update({"type": "release", "per_page": 3})
    try:
        r = await client.get("https://api.discogs.com/database/search", headers=DISCOGS_HEADERS(), params=params)
        if r.status_code == 429: await asyncio.sleep(2)
        res = r.json().get("results", [])
        return res[0] if res else None
    except: return None

async def cerca_prezzo_max_discogs(master_id: int):
    if not master_id: return "", ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"https://api.discogs.com/masters/{master_id}/versions", headers=DISCOGS_HEADERS())
            if r.status_code == 200:
                vers = r.json().get("versions", [])
                if vers: return vers[0].get("catno", ""), "Valutazione disponibile"
    except: pass
    return "", ""

async def cerca_su_discogs(data: dict, use_cache=True, barcode="", fast_mode=False):
    artista, titolo = data.get("artista", ""), data.get("titolo", "")
    ck = cache_key(artista, titolo)
    if use_cache and artista:
        cached = await cache_get(ck)
        if cached: return cached

    async with httpx.AsyncClient(timeout=20) as client:
        match = None
        if barcode: match = await _discogs_search(client, {"barcode": barcode})
        if not match: match = await _discogs_search(client, {"artist": artista, "release_title": titolo})
        
        if match:
            data.update({
                "artista": match.get("title", "").split(" - ")[0],
                "titolo": match.get("title", "").split(" - ")[-1],
                "anno": match.get("year", ""),
                "stampa": match.get("catno", ""),
                "stile": ", ".join(match.get("style", [])[:2])
            })
            if not fast_mode and match.get("master_id"):
                p_cat, p_val = await cerca_prezzo_max_discogs(match["master_id"])
                data["stampa_costosa"], data["prezzo_max"] = p_cat, p_val
            await cache_set(ck, data)
    return data

# ── API ENDPOINTS ────────────────────────────────────────────────────────────
@app.post("/api/register")
async def register(data: RegisterData):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{SUPABASE_URL}/auth/v1/signup", headers=supa_headers(), 
                              json={"email": data.email, "password": data.password, "data": {"nome": data.nome}})
    if r.status_code not in (200, 201): raise HTTPException(400, "Errore registrazione")
    uid = r.json().get("user", {}).get("id")
    is_admin = bool(ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())
    if uid:
        async with httpx.AsyncClient() as c2:
            await c2.patch(f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{uid}", headers=supa_headers(use_secret=True),
                           json={"approved": is_admin, "email": data.email, "nome": data.nome})
    return {"status": "ok"}

@app.post("/api/login")
async def login(data: LoginData):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{SUPABASE_URL}/auth/v1/token?grant_type=password", headers=supa_headers(), json=data.dict())
    if r.status_code != 200: raise HTTPException(401, "Login fallito")
    res = r.json()
    is_admin = bool(ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())
    return {"access_token": res["access_token"], "user_id": res["user"]["id"], "is_admin": is_admin}

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    b64 = base64.b64encode(content).decode()
    prompt = "Analizza l'etichetta del vinile e rispondi SOLO JSON: {\"artista\":\"\",\"titolo\":\"\",\"formato\":\"\",\"stile\":\"\",\"anno\":\"\",\"etichetta\":\"\",\"stampa\":\"\",\"barcode\":\"\",\"lato\":\"\"}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": file.content_type, "data": b64}}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.1}
    }

    async with httpx.AsyncClient(timeout=45) as client:
        r = await client.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload)
        if r.status_code != 200: return {"_error": "api_fail"}
        try:
            raw = r.json()['candidates'][0]['content']['parts'][0]['text']
            g_data = json.loads(raw.strip().replace("```json", "").replace("```", ""))
        except: return {"_error": "parse_fail"}

    res = await cerca_su_discogs(g_data, barcode=extract_barcode(g_data.get("barcode", "")))
    res["catno"] = res.get("stampa", "")
    return res

@app.get("/api/vinili/{user_id}")
async def get_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=created_at.desc", headers=supa_headers(token))
    return r.json()

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        await client.post(f"{SUPABASE_URL}/rest/v1/vinili", headers=supa_headers(v.access_token), json=v.dict())
    return {"status": "ok"}

@app.delete("/api/vinile/{vinyl_id}")
async def delete_vinyl(vinyl_id: int, token: str):
    async with httpx.AsyncClient() as client:
        await client.delete(f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vinyl_id}", headers=supa_headers(token))
    return {"status": "ok"}

# ── LOGICA EXCEL ─────────────────────────────────────────────────────────────
@app.get("/api/export/excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}", headers=supa_headers(token))
    data = r.json()
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Collezione Vinili"
    
    headers = ["Artista", "Titolo", "Formato", "Etichetta", "Anno", "Stampa", "Stile", "Valutazione"]
    ws.append(headers)
    
    for v in data:
        ws.append([v.get("artista"), v.get("titolo"), v.get("formato"), v.get("etichetta"), 
                   v.get("anno"), v.get("stampa"), v.get("stile"), v.get("prezzo_max")])
    
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return StreamingResponse(out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                             headers={"Content-Disposition": "attachment; filename=collezione.xlsx"})

@app.get("/api/admin/users")
async def admin_users(token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/profiles", headers=supa_headers(use_secret=True))
    return r.json()

@app.post("/api/admin/approve/{uid}")
async def approve_user(uid: str):
    async with httpx.AsyncClient() as client:
        await client.patch(f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{uid}", headers=supa_headers(use_secret=True), json={"approved": True})
    return {"status": "ok"}
