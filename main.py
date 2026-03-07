import os
import io
import json
import base64
import httpx
import subprocess
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment

app = FastAPI()

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")  # Aggiungi questa variabile su Railway

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
    catno: Optional[str] = ""

def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

async def cerca_su_discogs(gemini_data: dict) -> dict:
    """Cerca su Discogs usando i dati di Gemini e arricchisce i campi."""
    if not DISCOGS_TOKEN:
        print("DISCOGS: token non configurato")
        return gemini_data

    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "VinylHunter/1.0"
    }

    # Costruisci query di ricerca con i dati disponibili
    query_parts = []
    if gemini_data.get("catno"):
        query_parts.append(gemini_data["catno"])
    if gemini_data.get("artista"):
        query_parts.append(gemini_data["artista"])
    if gemini_data.get("titolo"):
        query_parts.append(gemini_data["titolo"])
    if gemini_data.get("etichetta"):
        query_parts.append(gemini_data["etichetta"])

    if not query_parts:
        print("DISCOGS: nessun dato da cercare")
        return gemini_data

    query = " ".join(query_parts)
    print(f"DISCOGS QUERY: {query}")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Prima cerca per catno se disponibile
            params = {"q": query, "type": "release", "per_page": 3}
            if gemini_data.get("catno"):
                params["catno"] = gemini_data["catno"]

            r = await client.get(
                "https://api.discogs.com/database/search",
                headers=headers,
                params=params
            )
            print(f"DISCOGS STATUS: {r.status_code}")

            if r.status_code != 200:
                print(f"DISCOGS ERROR: {r.text[:200]}")
                return gemini_data

            results = r.json().get("results", [])
            print(f"DISCOGS RESULTS: {len(results)} trovati")

            if not results:
                return gemini_data

            # Prendi il primo risultato
            match = results[0]
            print(f"DISCOGS MATCH: {match.get('title')} - {match.get('year')}")

            # Estrai artista e titolo dal campo title (formato: "Artista - Titolo")
            title_full = match.get("title", "")
            artista = gemini_data.get("artista", "")
            titolo = gemini_data.get("titolo", "")
            if " - " in title_full:
                parts = title_full.split(" - ", 1)
                artista = parts[0].strip()
                titolo = parts[1].strip()

            # Stile/genere
            styles = match.get("style", []) or match.get("genre", [])
            stile = ", ".join(styles[:2]) if styles else gemini_data.get("stile", "")

            # Formato
            formats = match.get("format", [])
            formato = formats[0] if formats else gemini_data.get("formato", "")

            # Etichetta
            labels = match.get("label", [])
            etichetta = labels[0] if labels else gemini_data.get("etichetta", "")

            # Anno
            anno = str(match.get("year", "")) or gemini_data.get("anno", "")

            # Catno
            catno = match.get("catno", "") or gemini_data.get("catno", "")

            result = {
                "artista": artista or gemini_data.get("artista", ""),
                "titolo": titolo or gemini_data.get("titolo", ""),
                "formato": formato or gemini_data.get("formato", ""),
                "stile": stile or gemini_data.get("stile", ""),
                "anno": anno or gemini_data.get("anno", ""),
                "etichetta": etichetta or gemini_data.get("etichetta", ""),
                "catno": catno or gemini_data.get("catno", ""),
            }
            print(f"DISCOGS ENRICHED: {result}")
            return result

    except Exception as e:
        print(f"DISCOGS EXCEPTION: {e}")
        return gemini_data

@app.post("/api/register")
async def register(data: RegisterData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/signup",
            headers=supa_headers(),
            json={"email": data.email, "password": data.password, "data": {"nome": data.nome}}
        )
    res = r.json()
    print(f"REGISTER STATUS: {r.status_code} RESPONSE: {res}")
    if r.status_code not in (200, 201) or "error" in res:
        raise HTTPException(400, res.get("msg") or res.get("error_description") or res.get("message") or str(res))
    return {"status": "ok", "message": "Registrazione completata"}

@app.post("/api/login")
async def login(data: LoginData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers=supa_headers(),
            json={"email": data.email, "password": data.password}
        )
    res = r.json()
    if r.status_code != 200 or "access_token" not in res:
        raise HTTPException(401, "Email o password errati")
    return {
        "access_token": res["access_token"],
        "user_id": res["user"]["id"],
        "nome": res["user"].get("user_metadata", {}).get("nome", data.email)
    }

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    gemini_data = {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "catno": ""}

    # Gemini OCR
    if GEMINI_KEY:
        try:
            b64 = base64.b64encode(content).decode()
            mime = file.content_type or "image/jpeg"
            gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
            prompt = """Analizza questa immagine di un'etichetta o copertina di disco vinile.
Estrai le informazioni e rispondi SOLO con JSON valido senza markdown:
{"artista":"","titolo":"","formato":"","stile":"","anno":"","etichetta":"","catno":""}
Se un campo non è visibile lascialo stringa vuota."""
            payload = {"contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime, "data": b64}}]}]}
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(gemini_url, json=payload)
            print(f"GEMINI STATUS: {r.status_code}")
            if r.status_code == 200:
                text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                print(f"GEMINI TEXT: {text[:500]}")
                text = text.strip().replace("```json", "").replace("```", "").strip()
                try:
                    gemini_data = json.loads(text)
                except Exception as pe:
                    print(f"JSON PARSE ERROR: {pe}")
            else:
                print(f"GEMINI ERROR: {r.text[:300]}")
        except Exception as e:
            print(f"GEMINI EXCEPTION: {e}")

    # Arricchisci con Discogs
    result = await cerca_su_discogs(gemini_data)
    return result

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/vinili",
            headers=supa_headers(v.access_token),
            json={"user_id": v.user_id, "artista": v.artista, "titolo": v.titolo,
                  "formato": v.formato, "stile": v.stile, "anno": v.anno,
                  "etichetta": v.etichetta, "catno": v.catno}
        )
    print(f"SAVE STATUS: {r.status_code} RESPONSE: {r.text[:200]}")
    if r.status_code not in (200, 201):
        raise HTTPException(400, f"Errore salvataggio: {r.status_code} - {r.text[:100]}")
    return {"status": "ok"}

@app.get("/api/vinili/{user_id}")
async def get_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=created_at.desc",
            headers=supa_headers(token)
        )
    if r.status_code != 200:
        raise HTTPException(400, "Errore recupero dati")
    return r.json()

@app.delete("/api/vinile/{vinyl_id}")
async def delete_vinyl(vinyl_id: int, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vinyl_id}",
            headers=supa_headers(token)
        )
    if r.status_code not in (200, 204):
        raise HTTPException(400, "Errore eliminazione")
    return {"status": "deleted"}

@app.post("/api/import_excel")
async def import_excel(user_id: str, token: str, file: UploadFile = File(...)):
    content = await file.read()
    wb = load_workbook(io.BytesIO(content))
    ws = wb.active
    imported = 0
    async with httpx.AsyncClient() as client:
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or not row[0]:
                continue
            await client.post(
                f"{SUPABASE_URL}/rest/v1/vinili",
                headers=supa_headers(token),
                json={"user_id": user_id, "artista": str(row[0] or ""),
                      "titolo": str(row[1] or ""), "formato": str(row[2] or ""),
                      "stile": str(row[3] or ""), "anno": str(row[4] or ""),
                      "etichetta": str(row[5] or ""), "catno": str(row[6] or "")}
            )
            imported += 1
    return {"status": "ok", "imported": imported}

@app.get("/api/export_excel/{user_id}")
async def export_excel(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=artista.asc",
            headers=supa_headers(token)
        )
    if r.status_code != 200:
        raise HTTPException(400, "Errore recupero dati")
    vinili = r.json()
    wb = Workbook()
    ws = wb.active
    ws.title = "Catalogo Vinili"
    header_fill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    headers = ["Artista", "Titolo", "Formato", "Stile", "Anno", "Etichetta", "Cat. No."]
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
    for row_idx, v in enumerate(vinili, 2):
        for col, field in enumerate(["artista","titolo","formato","stile","anno","etichetta","catno"], 1):
            ws.cell(row=row_idx, column=col, value=v.get(field, ""))
    for col, width in zip(range(1, 8), [25, 30, 10, 20, 8, 20, 15]):
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = width
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp.name)
    return FileResponse(tmp.name, filename="Catalogo_Vinili.xlsx",
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()
