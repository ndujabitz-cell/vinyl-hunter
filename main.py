import os
import asyncio
import io
import json
import base64
import re
import httpx
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ── CONFIGURAZIONE LOGGING ──────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VinylHunter")

app = FastAPI(title="Vinyl Hunter Pro")

# ── VARIABILI D'AMBIENTE ────────────────────────────────────────────────────
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL     = os.getenv("ADMIN_EMAIL", "")

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

# ── HELPER HEADERS ──────────────────────────────────────────────────────────
def get_supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    headers = {"apikey": key, "Content-Type": "application/json"}
    headers["Authorization"] = f"Bearer {token if token else key}"
    return headers

def get_discogs_headers():
    return {"Authorization": f"Discogs token={DISCOGS_TOKEN}", "User-Agent": "VinylHunter/2.0"}

# ── MODELLI DATI ─────────────────────────────────────────────────────────────
class VinylEntry(BaseModel):
    user_id: str
    access_token: str
    artista: str
    titolo: str
    formato: Optional[str] = "Vinyl"
    etichetta: Optional[str] = ""
    anno: Optional[str] = ""
    stampa: Optional[str] = ""
    prezzo_max: Optional[str] = ""

# ── PAGINA HOME (PER EVITARE SCHERMATA NERA) ────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Vinyl Hunter Backend</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #1a1a1a; color: #eee; text-align: center; padding: 50px; }
            .status { color: #00ff00; font-weight: bold; border: 1px solid #00ff00; padding: 10px; display: inline-block; border-radius: 5px; }
            .btn { background: #3498db; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; margin-top: 20px; display: inline-block; }
        </style>
    </head>
    <body>
        <h1>Vinyl Hunter API</h1>
        <div class="status">● SISTEMA ATTIVO</div>
        <p>Il server Railway sta rispondendo correttamente.</p>
        <a href="/docs" class="btn">Vai alla Documentazione (Swagger)</a>
    </body>
    </html>
    """

# ── MOTORE DI RICERCA DISCOGS (PROFONDO) ────────────────────────────────────
async def deep_discogs_search(client, query_data: dict):
    artista = query_data.get("artista", "").strip()
    titolo = query_data.get("titolo", "").strip()
    barcode = query_data.get("barcode", "").strip()
    catno = query_data.get("stampa", "").strip()

    search_params = [
        {"barcode": barcode} if barcode else None,
        {"catno": catno} if catno else None,
        {"artist": artista, "release_title": titolo},
        {"q": f"{artista} {titolo}"}
    ]

    for params in filter(None, search_params):
        try:
            params.update({"type": "release", "per_page": 1})
            r = await client.get("https://api.discogs.com/database/search", 
                                 headers=get_discogs_headers(), params=params)
            if r.status_code == 200 and r.json().get("results"):
                return r.json()["results"][0]
        except Exception as e:
            logger.error(f"Errore ricerca Discogs: {e}")
    return None

# ── ENDPOINT SCAN (GEMINI + DISCOGS) ────────────────────────────────────────
@app.post("/api/scan")
async def scan_vinyl(file: UploadFile = File(...)):
    try:
        content = await file.read()
        img_b64 = base64.b64encode(content).decode()
        
        prompt = """Analizza l'etichetta del vinile. Estrai: artista, titolo, formato, anno, etichetta, stampa (cat number), barcode.
        Rispondi SOLO in formato JSON puro senza commenti."""

        payload = {
            "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": file.content_type, "data": img_b64}}]}],
            "generationConfig": {"response_mime_type": "application/json", "temperature": 0.1}
        }

        async with httpx.AsyncClient(timeout=60) as client:
            # 1. Chiamata Gemini
            g_res = await client.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload)
            if g_res.status_code != 200:
                raise HTTPException(500, f"Gemini Error: {g_res.text}")
            
            raw_data = g_res.json()['candidates'][0]['content']['parts'][0]['text']
            vinyl_info = json.loads(raw_data)

            # 2. Arricchimento con Discogs
            match = await deep_discogs_search(client, vinyl_info)
            if match:
                vinyl_info.update({
                    "artista": match.get("title", "").split(" - ")[0],
                    "titolo": match.get("title", "").split(" - ")[-1],
                    "anno": match.get("year", vinyl_info.get("anno")),
                    "stampa": match.get("catno", vinyl_info.get("stampa")),
                    "label_url": match.get("resource_url")
                })
            
            return vinyl_info
    except Exception as e:
        logger.error(f"Crash in scan: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# ── GESTIONE VINILI (CRUD) ────────────────────────────────────────────────────
@app.get("/api/vinili/{user_id}")
async def list_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=created_at.desc", 
                             headers=get_supa_headers(token))
        return r.json()

@app.post("/api/vinile")
async def save_vinyl(v: VinylEntry):
    async with httpx.AsyncClient() as client:
        payload = v.dict(exclude={"access_token"})
        r = await client.post(f"{SUPABASE_URL}/rest/v1/vinili", 
                              headers=get_supa_headers(v.access_token), json=payload)
        return {"status": "ok", "db_response": r.status_code}

# ── EXCEL CON FREEZE PANES (IMPORTANTE) ──────────────────────────────────────
@app.get("/api/export/excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}", headers=get_supa_headers(token))
        data = r.json()

    wb = Workbook()
    ws = wb.active
    ws.title = "Collezione"
    
    # Blocca la prima riga
    ws.freeze_panes = "A2"

    headers = ["ARTISTA", "TITOLO", "FORMATO", "ETICHETTA", "ANNO", "STAMPA", "VALUTAZIONE"]
    ws.append(headers)

    # Stile Header
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="444444", end_color="444444", fill_type="solid")
        cell.alignment = Alignment(horizontal="center")

    for row in data:
        ws.append([row.get("artista"), row.get("titolo"), row.get("formato"), row.get("etichetta"), row.get("anno"), row.get("stampa"), row.get("prezzo_max")])

    # Auto-adjust larghezza
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f"attachment; filename=collezione_{user_id[:5]}.xlsx"})

# ── AVVIO ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
