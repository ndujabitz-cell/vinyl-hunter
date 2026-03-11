mport os
import io
import json
import base64
import httpx
import subprocess
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel
from typing import Optional
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from collections import Counter

app = FastAPI()

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")

DISCOGS_HEADERS = lambda: {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "VinylHunter/1.0"
}

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

def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

async def cerca_prezzo_max_discogs(master_id: int) -> tuple[str, str]:
    """Cerca tutte le stampe di un master e trova quella col prezzo medio pi alto."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"https://api.discogs.com/masters/{master_id}/versions",
                headers=DISCOGS_HEADERS(),
                params={"per_page": 50}
            )
            if r.status_code != 200:
                return "", ""
            
            versions = r.json().get("versions", [])
            best_price = 0
            best_catno = ""
            
            for v in versions:
                release_id = v.get("id")
                catno = v.get("catno", "")
                if not release_id:
                    continue
                stats_r = await client.get(
                    f"https://api.discogs.com/marketplace/stats/{release_id}",
                    headers=DISCOGS_HEADERS()
                )
                if stats_r.status_code == 200:
                    stats = stats_r.json()
                    avg = stats.get("lowest_price", {})
                    if isinstance(avg, dict):
                        price = avg.get("value", 0) or 0
                    else:
                        price = avg or 0
                    if price > best_price:
                        best_price = price
                        best_catno = catno
            
            if best_price > 0:
                return best_catno, f"{best_price:.2f}"
            return "", ""
    except Exception as e:
        print(f"DISCOGS PRICE ERROR: {e}")
        return "", ""

async def cerca_su_discogs(gemini_data: dict) -> dict:
    if not DISCOGS_TOKEN:
        return gemini_data

    query_parts = []
    if gemini_data.get("stampa") or gemini_data.get("catno"):
        query_parts.append(gemini_data.get("stampa") or gemini_data.get("catno", ""))
    if gemini_data.get("artista"):
        query_parts.append(gemini_data["artista"])
    if gemini_data.get("titolo"):
        query_parts.append(gemini_data["titolo"])
    if gemini_data.get("etichetta"):
        query_parts.append(gemini_data["etichetta"])

    if not query_parts:
        return gemini_data

    query = " ".join(query_parts)
    print(f"DISCOGS QUERY: {query}")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            params = {"q": query, "type": "release", "per_page": 3}
            catno = gemini_data.get("stampa") or gemini_data.get("catno", "")
            if catno:
                params["catno"] = catno

            r = await client.get(
                "https://api.discogs.com/database/search",
                headers=DISCOGS_HEADERS(),
                params=params
            )
            if r.status_code != 200:
                return gemini_data

            results = r.json().get("results", [])
            if not results:
                return gemini_data

            match = results[0]
            print(f"DISCOGS MATCH: {match.get('title')} {match.get('year')}")

            title_full = match.get("title", "")
            artista = gemini_data.get("artista", "")
            titolo = gemini_data.get("titolo", "")
            if " - " in title_full:
                parts = title_full.split(" - ", 1)
                artista = parts[0].strip()
                titolo = parts[1].strip()

            styles = match.get("style", []) or match.get("genre", [])
            stile = ", ".join(styles[:2]) if styles else gemini_data.get("stile", "")

            formats = match.get("format", [])
            formato = formats[0] if formats else gemini_data.get("formato", "")

            labels = match.get("label", [])
            etichetta = labels[0] if labels else gemini_data.get("etichetta", "")

            anno = str(match.get("year", "")) or gemini_data.get("anno", "")
            stampa = gemini_data.get("stampa", "") or match.get("catno", "")

            stampa_costosa = ""
            prezzo_max = ""
            master_id = match.get("master_id")
            if master_id:
                stampa_costosa, prezzo_max = await cerca_prezzo_max_discogs(master_id)

            result = {
                "artista": artista or gemini_data.get("artista", ""),
                "titolo": titolo or gemini_data.get("titolo", ""),
                "formato": formato or gemini_data.get("formato", ""),
                "stile": stile or gemini_data.get("stile", ""),
                "anno": anno or gemini_data.get("anno", ""),
                "etichetta": etichetta or gemini_data.get("etichetta", ""),
                "stampa": stampa,
                "stampa_costosa": stampa_costosa,
                "prezzo_max": prezzo_max,
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
    gemini_data = {"artista": "", "titolo": "", "formato": "", "stile": "", "anno": "", "etichetta": "", "stampa": "", "stampa_costosa": "", "prezzo_max": ""}

    if GEMINI_KEY:
        try:
            b64 = base64.b64encode(content).decode()
            mime = file.content_type or "image/jpeg"
            gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
            prompt = """Analizza questa immagine di un'etichetta o copertina di disco vinile.
Estrai TUTTE le informazioni visibili e rispondi SOLO con JSON valido senza markdown:
{"artista":"","titolo":"","formato":"","stile":"","anno":"","etichetta":"","stampa":""}

Istruzioni importanti:
- "stampa" = numero di catalogo (catalog number / cat no / cat. no. / matrix number). Esempi: CBS 1234, CLMN-126, HS032, ATL-50234, 2C 006-93752, nr 015/B. Cercalo sul bordo, in basso, vicino al logo etichetta o inciso nella plastica.
- "stile" = genere musicale visibile sull'etichetta (es: Soul, Jazz, Rock, Funk, Electronic, Classical). Se non visibile, deducilo dall'etichetta discografica (es: Blue Note  Jazz, Motown  Soul).
- "anno" = anno di pubblicazione (cerca numeri a 4 cifre tipo 1975, 2009 ecc)
- "formato" = 7", 10", 12", LP, EP, 45rpm, 33rpm ecc
- "etichetta" = nome dell'etichetta discografica (es: Atlantic, Fania, Columbia, Blue Note)
- Se un campo non  visibile lascialo stringa vuota
- NON inventare dati non visibili nell'immagine"""
            payload = {"contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime, "data": b64}}]}]}
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(gemini_url, json=payload)
            print(f"GEMINI STATUS: {r.status_code}")
            if r.status_code == 200:
                text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                print(f"GEMINI TEXT: {text[:500]}")
                text = text.strip().replace("```json", "").replace("```", "").strip()
                try:
                    gemini_data.update(json.loads(text))
                except Exception as pe:
                    print(f"JSON PARSE ERROR: {pe}")
            else:
                print(f"GEMINI ERROR: {r.text[:300]}")
        except Exception as e:
            print(f"GEMINI EXCEPTION: {e}")

    result = await cerca_su_discogs(gemini_data)
    # Assicura che catno sia sempre presente (il frontend legge 'catno')
    result["catno"] = result.get("stampa", "")
    return result

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/vinili",
            headers=supa_headers(v.access_token),
            json={"user_id": v.user_id, "artista": v.artista, "titolo": v.titolo,
                  "formato": v.formato, "stile": v.stile, "anno": v.anno,
                  "etichetta": v.etichetta, "stampa": v.stampa,
                  "stampa_costosa": v.stampa_costosa, "prezzo_max": v.prezzo_max}
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

@app.delete("/api/catalogo/{user_id}")
async def delete_catalog(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}",
            headers=supa_headers(token)
        )
    if r.status_code not in (200, 204):
        raise HTTPException(400, "Errore eliminazione catalogo")
    return {"status": "deleted"}


@app.post("/api/import_excel")
async def import_excel(user_id: str = Form(...), token: str = Form(...), file: UploadFile = File(...)):
    content = await file.read()
    wb = load_workbook(io.BytesIO(content))
    ws = wb.active
    imported = 0
    TOTALI = {'7"', '10"', '12"', '4"', 'lp', '2xlp', '2x lp', 'ep', 'single', 'riepilogo formati', 'totale vinili'}

    async with httpx.AsyncClient() as client:
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or not row[0]:
                continue
            artista = str(row[0] or "").strip()
            if artista.lower() in TOTALI or (len(artista) <= 5 and str(row[1] or "").isdigit()):
                continue
            await client.post(
                f"{SUPABASE_URL}/rest/v1/vinili",
                headers=supa_headers(token),
                json={"user_id": user_id,
                      "artista": artista,
                      "titolo": str(row[1] or ""),
                      "formato": str(row[2] or ""),
                      "stile": str(row[3] or ""),
                      "anno": str(row[4] or ""),
                      "etichetta": str(row[5] or ""),
                      "stampa": str(row[6] or ""),
                      "stampa_costosa": str(row[7] or "") if len(row) > 7 else "",
                      "prezzo_max": str(row[8] or "") if len(row) > 8 else ""}
            )
            imported += 1
    return {"status": "ok", "imported": imported}


#  EXPORT EXCEL: GET con token come query param (compatibile mobile/Safari) 
@app.get("/api/export_excel/{user_id}")
async def export_excel(user_id: str, token: str = ""):
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
    headers = ["Artista", "Titolo", "Formato", "Stile", "Anno", "Etichetta", "Stampa", "Stampa pi Costosa", "Prezzo medio pi alto"]
    col_widths = [25, 30, 12, 20, 8, 20, 15, 18, 20]

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for row_idx, v in enumerate(vinili, 2):
        ws.cell(row=row_idx, column=1, value=v.get("artista", ""))
        ws.cell(row=row_idx, column=2, value=v.get("titolo", ""))
        ws.cell(row=row_idx, column=3, value=v.get("formato", ""))
        ws.cell(row=row_idx, column=4, value=v.get("stile", ""))
        ws.cell(row=row_idx, column=5, value=v.get("anno", ""))
        ws.cell(row=row_idx, column=6, value=v.get("etichetta", ""))
        ws.cell(row=row_idx, column=7, value=v.get("stampa", "") or v.get("catno", ""))
        ws.cell(row=row_idx, column=8, value=v.get("stampa_costosa", ""))
        ws.cell(row=row_idx, column=9, value=v.get("prezzo_max", ""))

    for col, width in enumerate(col_widths, 1):
        ws.column_dimensions[ws.cell(row=1, column=col).column_letter].width = width

    # Riepilogo formati in fondo
    formati_count = Counter()
    for v in vinili:
        fmt = str(v.get("formato", "") or "").strip().lower()
        if fmt:
            if '7' in fmt: formati_count['7"'] += 1
            elif '10' in fmt: formati_count['10"'] += 1
            elif '12' in fmt: formati_count['12"'] += 1
            elif '2xlp' in fmt or '2x lp' in fmt or 'double' in fmt: formati_count['2xLP'] += 1
            elif 'lp' in fmt: formati_count['LP'] += 1
            else: formati_count[fmt[:10]] += 1

    last_row = ws.max_row + 2
    total_fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
    total_font = Font(bold=True, color="FFFFFF")

    title_cell = ws.cell(row=last_row, column=1, value="RIEPILOGO FORMATI")
    title_cell.font = total_font
    title_cell.fill = total_fill
    last_row += 1

    totale = 0
    for fmt in ['4"', '7"', '10"', '12"', 'LP', '2xLP']:
        count = formati_count.get(fmt, 0)
        c1 = ws.cell(row=last_row, column=1, value=fmt)
        c2 = ws.cell(row=last_row, column=2, value=count)
        c1.font = total_font; c1.fill = total_fill
        c2.font = total_font; c2.fill = total_fill
        totale += count
        last_row += 1

    c1 = ws.cell(row=last_row, column=1, value="TOTALE VINILI")
    c2 = ws.cell(row=last_row, column=2, value=totale)
    c1.font = Font(bold=True, color="FFFFFF", size=12)
    c1.fill = PatternFill(start_color="4a0080", end_color="4a0080", fill_type="solid")
    c2.font = Font(bold=True, color="FFFFFF", size=12)
    c2.fill = PatternFill(start_color="4a0080", end_color="4a0080", fill_type="solid")

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Catalogo_Vinili.xlsx"}
    )

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()
