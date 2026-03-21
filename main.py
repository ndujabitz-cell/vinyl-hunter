import os
import asyncio
import io
import json
import base64
import re
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Depends
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from collections import Counter

app = FastAPI()

# ── CONFIGURAZIONE AMBIENTE ──────────────────────────────────────────────────
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL     = os.getenv("ADMIN_EMAIL", "")

GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

def DISCOGS_HEADERS():
    return {
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

# ── LOGICA DI PULIZIA E OCR ──────────────────────────────────────────────────
def fix_catno_ocr(s: str) -> str:
    if not s: return ""
    s = s.upper().replace(" ", "")
    # Correzione comune OCR: la 'O' scambiata per lo zero '0' vicino ai numeri
    chars = list(s)
    for i, c in enumerate(chars):
        if c == 'O':
            if (i > 0 and chars[i-1].isdigit()) or (i < len(chars)-1 and chars[i+1].isdigit()):
                chars[i] = '0'
    return "".join(chars)

def extract_barcode(s: str) -> str:
    s_clean = re.sub(r'[\s\-]', '', str(s or ''))
    matches = re.findall(r'\d{8,13}', s_clean)
    for m in matches:
        if len(m) in (8, 12, 13): return m
    return ""

# ── LOGICA DISCOGS AVANZATA (SEARCH ENGINE) ──────────────────────────────────
async def _discogs_api_call(client, endpoint, params):
    try:
        r = await client.get(f"https://api.discogs.com/{endpoint}", headers=DISCOGS_HEADERS(), params=params)
        if r.status_code == 429:
            await asyncio.sleep(2) # Rate limit protection
            r = await client.get(f"https://api.discogs.com/{endpoint}", headers=DISCOGS_HEADERS(), params=params)
        return r.json() if r.status_code == 200 else None
    except: return None

async def cerca_su_discogs_full(data: dict, barcode: str = ""):
    artista = (data.get("artista") or "").strip()
    titolo = (data.get("titolo") or "").strip()
    stampa = fix_catno_ocr(data.get("stampa") or "")
    
    async with httpx.AsyncClient(timeout=20) as client:
        # 1. Ricerca per Barcode
        if barcode:
            res = await _discogs_api_call(client, "database/search", {"barcode": barcode, "type": "release"})
            if res and res.get("results"): return res["results"][0]

        # 2. Ricerca per Catalogo (molto precisa per i vinili)
        if stampa:
            res = await _discogs_api_call(client, "database/search", {"catno": stampa, "type": "release"})
            if res and res.get("results"): return res["results"][0]

        # 3. Ricerca Artista + Titolo
        if artista and titolo:
            res = await _discogs_api_call(client, "database/search", {"artist": artista, "release_title": titolo, "type": "release"})
            if res and res.get("results"): return res["results"][0]

    return None
    # ── LOGICA GEMINI OTTIMIZZATA ────────────────────────────────────────────────
@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    if not GEMINI_KEY:
        raise HTTPException(500, "Gemini API Key non configurata.")

    b64 = base64.b64encode(content).decode()
    
    # Prompt ultra-preciso per evitare allucinazioni
    prompt = """Analizza l'etichetta del vinile. Estrai: artista, titolo, formato (LP, 12", 7"), stile, anno, etichetta, stampa (numero catalogo), barcode, lato (A/B).
    Rispondi ESCLUSIVAMENTE con un oggetto JSON valido. Se un dato è incerto, usa stringa vuota."""

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": file.content_type or "image/jpeg", "data": b64}}
            ]
        }],
        "generationConfig": {
            "response_mime_type": "application/json",
            "temperature": 0.1
        }
    }

    async with httpx.AsyncClient(timeout=45) as client:
        try:
            r = await client.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload)
            if r.status_code == 429:
                return {"_error": "quota_exceeded", "message": "Troppe richieste. Riprova tra 60 secondi."}
            
            rj = r.json()
            raw_text = rj['candidates'][0]['content']['parts'][0]['text']
            # Pulizia per sicurezza nel caso Gemini aggiunga markdown
            clean_json = raw_text.strip().replace("```json", "").replace("```", "")
            gemini_data = json.loads(clean_json)
        except Exception as e:
            print(f"Errore Gemini: {e}")
            return {"_error": "ocr_failed"}

    # Integrazione con Discogs (usando la logica della Parte 1)
    barcode_val = extract_barcode(gemini_data.get("barcode", ""))
    match = await cerca_su_discogs_full(gemini_data, barcode=barcode_val)
    
    if match:
        gemini_data.update({
            "artista": match.get("title", "").split(" - ")[0],
            "titolo": match.get("title", "").split(" - ")[-1],
            "anno": match.get("year", gemini_data.get("anno")),
            "stampa": match.get("catno", gemini_data.get("stampa")),
            "stile": ", ".join(match.get("style", [])[:2])
        })
    
    gemini_data["catno"] = gemini_data.get("stampa", "")
    return gemini_data

# ── GESTIONE VINILI (CRUD) ────────────────────────────────────────────────────
@app.get("/api/vinili/{user_id}")
async def get_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=created_at.desc",
            headers=supa_headers(token)
        )
    return r.json()

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/vinili",
            headers=supa_headers(v.access_token),
            json=v.dict(exclude={"access_token"})
        )
    return {"status": "ok", "id": r.json()}

@app.patch("/api/vinile/{vinyl_id}")
async def update_vinyl(vinyl_id: int, data: VinylUpdate):
    update_fields = {k: v for k, v in data.dict().items() if k != "token" and v is not None}
    async with httpx.AsyncClient() as client:
        await client.patch(
            f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vinyl_id}",
            headers=supa_headers(data.token),
            json=update_fields
        )
    return {"status": "updated"}

@app.delete("/api/vinile/{vinyl_id}")
async def delete_vinyl(vinyl_id: int, token: str):
    async with httpx.AsyncClient() as client:
        await client.delete(
            f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vinyl_id}",
            headers=supa_headers(token)
        )
    return {"status": "deleted"}

# ── LOGICA EXCEL AVANZATA ─────────────────────────────────────────────────────
@app.get("/api/export/excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}",
            headers=supa_headers(token)
        )
    
    vinyls = r.json()
    wb = Workbook()
    ws = wb.active
    ws.title = "Mia Collezione"

    # Header con stile
    headers = ["ARTISTA", "TITOLO", "FORMATO", "ETICHETTA", "ANNO", "STAMPA", "STILE", "VALUTAZIONE"]
    ws.append(headers)

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="333333", end_color="333333", fill_type="solid")
    
    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    # Inserimento dati
    for v in vinyls:
        ws.append([
            v.get("artista", ""), v.get("titolo", ""), v.get("formato", ""),
            v.get("etichetta", ""), v.get("anno", ""), v.get("stampa", ""),
            v.get("stile", ""), v.get("prezzo_max", "")
        ])

    # Auto-adjust larghezza colonne
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = max_length + 2

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=collezione_vinili.xlsx"}
    )

# ── AUTH & ADMIN ─────────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(data: LoginData):
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{SUPABASE_URL}/auth/v1/token?grant_type=password", 
                              headers=supa_headers(), json=data.dict())
    if r.status_code != 200:
        raise HTTPException(401, "Credenziali non valide")
    
    res = r.json()
    is_admin = (ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())
    return {
        "access_token": res["access_token"],
        "user_id": res["user"]["id"],
        "is_admin": is_admin
    }

@app.get("/api/admin/users")
async def get_all_users(token: str):
    # Verifica admin rudimentale o tramite secret
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/profiles", headers=supa_headers(use_secret=True))
    return r.json()

@app.post("/api/admin/approve/{user_id}")
async def approve_user(user_id: str):
    async with httpx.AsyncClient() as client:
        await client.patch(
            f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}",
            headers=supa_headers(use_secret=True),
            json={"approved
                  # ── LOGICA EXCEL AVANZATA (VERSIONE PRO CON FREEZE PANES) ──────────────────────
@app.get("/api/export/excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=artista.asc",
            headers=supa_headers(token)
        )
    
    if r.status_code != 200:
        raise HTTPException(r.status_code, "Errore nel recupero dati da Supabase")

    vinyls = r.json()
    wb = Workbook()
    ws = wb.active
    ws.title = "Collezione Vinili"

    # 1. Definizione Intestazioni
    headers = ["ARTISTA", "TITOLO", "FORMATO", "ETICHETTA", "ANNO", "STAMPA", "STILE", "VALUTAZIONE", "DATA AGGIUNTA"]
    ws.append(headers)

    # 2. BLOCCA LA PRIMA RIGA (Freeze Panes)
    ws.freeze_panes = "A2" 

    # 3. STILIZZAZIONE HEADER (Colori, Font, Bordi)
    header_fill = PatternFill(start_color="2C3E50", end_color="2C3E50", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=12)
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                         top=Side(style='thin'), bottom=Side(style='thin'))

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border

    # 4. INSERIMENTO DATI CON LOGICA COLORE PER FORMATO
    for v in vinyls:
        row_data = [
            str(v.get("artista", "")).upper(),
            str(v.get("titolo", "")).capitalize(),
            v.get("formato", ""),
            v.get("etichetta", ""),
            v.get("anno", ""),
            v.get("stampa", ""),
            v.get("stile", ""),
            v.get("prezzo_max", "N/D"),
            v.get("created_at", "")[:10] # Solo la data YYYY-MM-DD
        ]
        ws.append(row_data)
        
        # Colorazione riga in base al formato (Esempio: LP vs 7")
        current_row = ws.max_row
        fmt = str(v.get("formato", "")).upper()
        if "LP" in fmt:
            color = "EBF5FB" # Blu chiaro
        elif '7"' in fmt or "45" in fmt:
            color = "FEF9E7" # Giallo chiaro
        else:
            color = "FFFFFF"
            
        for cell in ws[current_row]:
            cell.fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
            cell.border = thin_border

    # 5. AUTO-FILTRO SU TUTTE LE COLONNE
    ws.auto_filter.ref = ws.dimensions

    # 6. OTTIMIZZAZIONE LARGHEZZA COLONNE (Auto-Size)
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except: pass
        adjusted_width = (max_length + 4)
        ws.column_dimensions[column].width = adjusted_width

    # 7. GENERAZIONE FILE
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    
    filename = f"collezione_{user_id[:5]}.xlsx"
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
