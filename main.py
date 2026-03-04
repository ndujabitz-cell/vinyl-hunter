import os
import io
import json
import base64
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
import tempfile

app = FastAPI()

# ── Config ──────────────────────────────────────────────
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"

# ── Models ───────────────────────────────────────────────
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

# ── Supabase helpers ─────────────────────────────────────
def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {
        "apikey": key,
        "Content-Type": "application/json",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    else:
        h["Authorization"] = f"Bearer {key}"
    return h

# ── Auth endpoints ───────────────────────────────────────
@app.post("/api/register")
async def register(data: RegisterData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/auth/v1/signup",
            headers=supa_headers(),
            json={
                "email": data.email,
                "password": data.password,
                "data": {"nome": data.nome}
            }
        )
    res = r.json()
    print(f"SUPABASE STATUS: {r.status_code}")
    print(f"SUPABASE RESPONSE: {res}")
    if r.status_code not in (200, 201) or "error" in res:
        error_msg = res.get("msg") or res.get("error_description") or res.get("message") or str(res)
        raise HTTPException(400, error_msg)
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

# ── OCR endpoint ─────────────────────────────────────────
@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    b64 = base64.b64encode(content).decode()
    mime = file.content_type or "image/jpeg"

    prompt = """Analizza questa immagine di un'etichetta o copertina di disco vinile.
Estrai le seguenti informazioni e rispondi SOLO con un JSON valido, senza markdown:
{
  "artista": "nome artista",
  "titolo": "titolo album",
  "formato": "LP o 7\" o 12\" o 2xLP ecc",
  "stile": "genere musicale",
  "anno": "anno di pubblicazione",
  "etichetta": "nome etichetta discografica",
  "catno": "numero di catalogo"
}
Se un campo non è visibile o leggibile, lascialo come stringa vuota "".
"""

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": mime, "data": b64}}
            ]
        }]
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(GEMINI_URL, json=payload)

    if r.status_code != 200:
        raise HTTPException(500, "Errore Gemini API")

    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
    text = text.strip().replace("```json", "").replace("```", "").strip()

    try:
        data = json.loads(text)
    except Exception:
        data = {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "catno": ""}

    return data

# ── Vinyl CRUD ───────────────────────────────────────────
@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/vinili",
            headers=supa_headers(v.access_token),
            json={
                "user_id": v.user_id,
                "artista": v.artista,
                "titolo": v.titolo,
                "formato": v.formato,
                "stile": v.stile,
                "anno": v.anno,
                "etichetta": v.etichetta,
                "catno": v.catno
            }
        )
    if r.status_code not in (200, 201):
        raise HTTPException(400, "Errore salvataggio")
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

# ── Excel import ─────────────────────────────────────────
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
                json={
                    "user_id": user_id,
                    "artista": str(row[0] or ""),
                    "titolo": str(row[1] or ""),
                    "formato": str(row[2] or ""),
                    "stile": str(row[3] or ""),
                    "anno": str(row[4] or ""),
                    "etichetta": str(row[5] or ""),
                    "catno": str(row[6] or "")
                }
            )
            imported += 1
    return {"status": "ok", "imported": imported}

# ── Excel export ─────────────────────────────────────────
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

    # Header style
    header_fill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    headers = ["Artista", "Titolo", "Formato", "Stile", "Anno", "Etichetta", "Cat. No."]

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    # Data rows
    for row_idx, v in enumerate(vinili, 2):
        ws.cell(row=row_idx, column=1, value=v.get("artista", ""))
        ws.cell(row=row_idx, column=2, value=v.get("titolo", ""))
        ws.cell(row=row_idx, column=3, value=v.get("formato", ""))
        ws.cell(row=row_idx, column=4, value=v.get("stile", ""))
        ws.cell(row=row_idx, column=5, value=v.get("anno", ""))
        ws.cell(row=row_idx, column=6, value=v.get("etichetta", ""))
        ws.cell(row=row_idx, column=7, value=v.get("catno", ""))

    # Column widths
    for col, width in zip(range(1, 8), [25, 30, 10, 20, 8, 20, 15]):
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = width

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    wb.save(tmp.name)
    return FileResponse(tmp.name, filename="Catalogo_Vinili.xlsx",
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── Frontend ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()
