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

def parse_ocr_text(text: str) -> dict:
    """Parse raw OCR text into vinyl fields using heuristics."""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    result = {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "catno": ""}
    
    import re
    # Find year
    for line in lines:
        year = re.search(r'\b(19[5-9]\d|20[0-2]\d)\b', line)
        if year:
            result["anno"] = year.group()
            break
    
    # Find catalog number (common patterns)
    for line in lines:
        catno = re.search(r'\b([A-Z]{1,5}[-\s]?\d{3,7}[A-Z]?)\b', line)
        if catno:
            result["catno"] = catno.group()
            break
    
    # Find format
    for line in lines:
        if re.search(r'\b(LP|EP|7"|12"|45|33|RPM|STEREO|MONO)\b', line, re.IGNORECASE):
            result["formato"] = re.search(r'\b(LP|EP|7"|12"|45|33|RPM)\b', line, re.IGNORECASE).group() if re.search(r'\b(LP|EP|7"|12"|45|33|RPM)\b', line, re.IGNORECASE) else ""
            break
    
    # First meaningful lines likely artist/title
    meaningful = [l for l in lines if len(l) > 2 and not l.isdigit()]
    if meaningful:
        result["artista"] = meaningful[0]
    if len(meaningful) > 1:
        result["titolo"] = meaningful[1]
    if len(meaningful) > 2:
        result["etichetta"] = meaningful[2]
    
    return result

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    
    # Try Gemini first if key available
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
            print(f"GEMINI RESPONSE: {r.text[:500]}")
            if r.status_code == 200:
                resp_json = r.json()
                print(f"GEMINI FULL: {str(resp_json)[:800]}")
                text = resp_json["candidates"][0]["content"]["parts"][0]["text"]
                print(f"GEMINI TEXT: {text[:500]}")
                text = text.strip().replace("```json", "").replace("```", "").strip()
                try:
                    return json.loads(text)
                except Exception as pe:
                    print(f"JSON PARSE ERROR: {pe}")
                    return {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "catno": ""}
        except Exception as e:
            print(f"GEMINI ERROR: {e}")
    
    # Fallback: Tesseract OCR
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        
        result = subprocess.run(
            ["tesseract", tmp_path, "stdout", "-l", "ita+eng", "--psm", "3"],
            capture_output=True, text=True, timeout=30
        )
        os.unlink(tmp_path)
        
        if result.returncode == 0 and result.stdout.strip():
            print(f"TESSERACT OUTPUT: {result.stdout[:300]}")
            return parse_ocr_text(result.stdout)
        else:
            print(f"TESSERACT ERROR: {result.stderr}")
    except Exception as e:
        print(f"TESSERACT EXCEPTION: {e}")
    
    # Last resort: return empty fields
    return {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "catno": ""}

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
