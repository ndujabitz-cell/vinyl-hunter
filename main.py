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

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_ANON   = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY      = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN   = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL     = os.getenv("ADMIN_EMAIL", "")

DISCOGS_HEADERS = lambda: {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "VinylHunter/1.0"
}

# ── Pydantic Models ───────────────────────────────────────────────────────────

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
    stampa_costosa: Optional[str] = ""
    prezzo_max: Optional[str] = ""
    token: Optional[str] = ""

# ── Supabase helpers ──────────────────────────────────────────────────────────

def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

# ── Discogs cache ─────────────────────────────────────────────────────────────

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
            if r.status_code == 200:
                rows = r.json()
                if rows:
                    print(f"CACHE HIT: {key}")
                    return rows[0]
    except Exception as e:
        print(f"CACHE GET ERROR: {e}")
    return None

async def cache_set(key: str, data: dict):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={**supa_headers(use_secret=True), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={
                    "cache_key":      key,
                    "artista":        data.get("artista", ""),
                    "titolo":         data.get("titolo", ""),
                    "etichetta":      data.get("etichetta", ""),
                    "stile":          data.get("stile", ""),
                    "anno":           data.get("anno", ""),
                    "formato":        data.get("formato", ""),
                    "stampa":         data.get("stampa", ""),
                    "stampa_costosa": data.get("stampa_costosa", ""),
                    "prezzo_max":     data.get("prezzo_max", ""),
                }
            )
            print(f"CACHE SET: {key}")
    except Exception as e:
        print(f"CACHE SET ERROR: {e}")

# ── Discogs helpers ───────────────────────────────────────────────────────────

def formato_to_discogs(fmt: str) -> str:
    f = fmt.lower().strip()
    if "12" in f:                                      return '12"'
    if "7" in f or "45" in f:                         return '7"'
    if "10" in f:                                      return '10"'
    if "2xlp" in f or "2x lp" in f or "double" in f: return "LP"
    if "lp" in f or "33" in f:                        return "LP"
    if "ep" in f:                                      return "EP"
    return "Vinyl"

def formati_alternativi(fmt: str) -> list[str]:
    if fmt == '7"':  return ["45 RPM", "Single", "Vinyl"]
    if fmt == '12"': return ["Maxi-Single", "Vinyl"]
    if fmt in ("LP", "EP"): return ["Vinyl"]
    return []

def is_7inch(fmt: str) -> bool:
    return "7" in fmt.lower() or "45" in fmt.lower()

def split_lati(titolo: str, fmt: str) -> tuple[str, str]:
    if not is_7inch(fmt):
        return titolo.strip(), ""
    for sep in [" / ", "/", " - "]:
        if sep in titolo:
            parts = titolo.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    return titolo.strip(), ""

def extract_barcode(s: str) -> str:
    s_clean = re.sub(r'[\s\-]', '', s or '')
    for m in re.findall(r'\d{8,13}', s_clean):
        if len(m) in (8, 12, 13):
            return m
    return ""

def fix_catno_ocr(s: str) -> str:
    """Corregge errori OCR: O->0 e I->1 adiacenti a cifre."""
    if not s:
        return s
    chars = list(s)
    for i, c in enumerate(chars):
        if c in ('O', 'o'):
            pd = i > 0 and chars[i-1].isdigit()
            nd = i < len(chars)-1 and chars[i+1].isdigit()
            nb = any(chars[j].isdigit() for j in range(max(0,i-3), min(len(chars),i+4)))
            sp = any(chars[j]==' ' for j in range(max(0,i-2), min(len(chars),i+3)))
            if pd or nd or (nb and not sp):
                chars[i] = '0'
        elif c in ('I', 'l'):
            pd = i > 0 and chars[i-1].isdigit()
            nd = i < len(chars)-1 and chars[i+1].isdigit()
            if pd or nd:
                chars[i] = '1'
    return ''.join(chars)

def catno_variants(catno: str) -> list[str]:
    variants = [catno]
    if catno and ' ' not in catno and len(catno) > 3:
        m = re.match(r'^([A-Za-z-]+)(\d+)$', catno)
        if m:
            variants.append(f"{m.group(1)} {m.group(2)}")
    elif catno and ' ' in catno:
        variants.append(catno.replace(' ', ''))
    return variants

async def _discogs_search(client, params: dict) -> dict | None:
    params.setdefault("type", "release")
    params.setdefault("per_page", 5)
    try:
        r = await client.get(
            "https://api.discogs.com/database/search",
            headers=DISCOGS_HEADERS(), params=params
        )
        if r.status_code == 429:
            print("DISCOGS RATE LIMIT - attendo 10s")
            await asyncio.sleep(10)
            r = await client.get(
                "https://api.discogs.com/database/search",
                headers=DISCOGS_HEADERS(), params=params
            )
        if r.status_code != 200:
            return None
        results = r.json().get("results", [])
        vinyl = [x for x in results if any(
            f.lower() in ['vinyl', '7"', '10"', '12"', 'lp', 'ep']
            for f in (x.get("format") or [])
        )]
        chosen = vinyl[0] if vinyl else (results[0] if results else None)
        if chosen:
            print(f"  → {chosen.get('title')} | fmt={chosen.get('format')} | catno={chosen.get('catno')} | year={chosen.get('year')}")
        return chosen
    except Exception as e:
        print(f"DISCOGS SEARCH EXC: {e}")
        return None

async def cerca_prezzo_max_discogs(master_id: int) -> tuple[str, str]:
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(
                f"https://api.discogs.com/masters/{master_id}/versions",
                headers=DISCOGS_HEADERS(), params={"per_page": 100}
            )
            if r.status_code != 200:
                return "", ""
            versions = r.json().get("versions", [])[:15]
            print(f"DISCOGS VERSIONS: {len(versions)} for master {master_id}")
            best_price, best_catno = 0.0, ""
            for v in versions:
                release_id = v.get("id")
                if not release_id:
                    continue
                await asyncio.sleep(1.5)
                sr = await client.get(
                    f"https://api.discogs.com/marketplace/stats/{release_id}",
                    headers=DISCOGS_HEADERS()
                )
                if sr.status_code == 429:
                    print("RATE LIMIT stats - skip")
                    continue
                if sr.status_code != 200:
                    continue
                stats = sr.json()
                lp = stats.get("lowest_price")
                if lp is None:
                    continue
                price = float(lp.get("value", 0) if isinstance(lp, dict) else lp or 0)
                if price > best_price:
                    best_price = price
                    best_catno = v.get("catno", "") or f"ID:{release_id}"
            if best_price > 0:
                return best_catno, f"EUR {best_price:.2f}"
            for v in versions:
                c = v.get("catno", "")
                if c and c.lower() != "none":
                    return c, ""
            return "", ""
    except Exception as e:
        print(f"DISCOGS PRICE ERROR: {e}")
        return "", ""

async def cerca_su_discogs(data: dict, use_cache: bool = True, barcode: str = "",
                            skip_prices: bool = False, fast_mode: bool = False) -> dict:
    if not DISCOGS_TOKEN:
        return data

    artista   = (data.get("artista")   or "").strip()
    titolo    = (data.get("titolo")    or "").strip()
    fmt_raw   = (data.get("formato")   or "").strip()
    etichetta = (data.get("etichetta") or "").strip()
    anno      = (data.get("anno")      or "").strip()
    catno     = fix_catno_ocr((data.get("stampa") or "").strip())
    ck        = cache_key(artista, titolo)

    # Cache
    if use_cache and artista:
        cached = await cache_get(ck)
        if cached:
            return {
                "artista":        artista,
                "titolo":         titolo,
                "formato":        data.get("formato")   or cached.get("formato", ""),
                "stile":          data.get("stile")     or cached.get("stile", ""),
                "anno":           data.get("anno")      or cached.get("anno", ""),
                "etichetta":      data.get("etichetta") or cached.get("etichetta", ""),
                "stampa":         data.get("stampa")    or cached.get("stampa", ""),
                "stampa_costosa": cached.get("stampa_costosa", ""),
                "prezzo_max":     cached.get("prezzo_max", ""),
            }

    fmt_discogs = formato_to_discogs(fmt_raw)
    lato_a, lato_b = split_lati(titolo, fmt_raw)
    ha_lati = bool(lato_b)
    cvs = catno_variants(catno)  # varianti catno (con/senza spazio)

    print(f"DISCOGS CERCA: artista={artista!r} lato_a={lato_a!r} lato_b={lato_b!r} "
          f"fmt={fmt_discogs!r} etichetta={etichetta!r} anno={anno!r} catno={catno!r}")

    match = None
    async with httpx.AsyncClient(timeout=15) as client:

        # 0. Barcode → match esatto
        if barcode:
            print(f"Tentativo 0: barcode={barcode}")
            match = await _discogs_search(client, {"barcode": barcode})

        # 1. catno + etichetta + formato
        if not match and catno and not catno.isdigit():
            for cv in cvs:
                p = {"catno": cv, "format": fmt_discogs}
                if etichetta: p["label"] = etichetta
                print(f"Tentativo 1: catno={cv!r} + etichetta + formato")
                match = await _discogs_search(client, p)
                if match: break

        # 2. catno + formato (senza etichetta)
        if not match and catno and not catno.isdigit():
            for cv in cvs:
                print(f"Tentativo 2: catno={cv!r} + formato")
                match = await _discogs_search(client, {"catno": cv, "format": fmt_discogs})
                if match: break

        # 2b. catno + formati alternativi
        if not match and catno and not catno.isdigit():
            for cv in cvs:
                for fa in formati_alternativi(fmt_discogs):
                    print(f"Tentativo 2b: catno={cv!r} + '{fa}'")
                    match = await _discogs_search(client, {"catno": cv, "format": fa})
                    if match: break
                if match: break

        # 3-4. lato A + lato B (solo 7"/45rpm con split)
        if not match and ha_lati:
            if etichetta:
                print("Tentativo 3: latoA+latoB + etichetta + formato")
                match = await _discogs_search(client, {
                    "artist": artista, "title": f"{lato_a} {lato_b}",
                    "label": etichetta, "format": fmt_discogs
                })
            if not match:
                print("Tentativo 4: latoA+latoB + formato")
                match = await _discogs_search(client, {
                    "artist": artista, "title": f"{lato_a} {lato_b}",
                    "format": fmt_discogs
                })

        # 5. artista + titolo + etichetta + formato
        if not match and artista and lato_a:
            p = {"artist": artista, "title": lato_a, "format": fmt_discogs}
            if etichetta: p["label"] = etichetta
            print("Tentativo 5: artista + titolo + etichetta + formato")
            match = await _discogs_search(client, p)

        # 5b. formati alternativi
        if not match and artista and lato_a:
            for fa in formati_alternativi(fmt_discogs):
                p = {"artist": artista, "title": lato_a, "format": fa}
                if etichetta: p["label"] = etichetta
                print(f"Tentativo 5b: formato alternativo '{fa}'")
                match = await _discogs_search(client, p)
                if match: break

        # 6-7. solo se non fast_mode
        if not fast_mode:
            if not match and artista and lato_a and anno:
                p = {"artist": artista, "title": lato_a, "format": fmt_discogs, "year": anno}
                if etichetta: p["label"] = etichetta
                print("Tentativo 6: +anno")
                match = await _discogs_search(client, p)

            if not match and artista and lato_a:
                print("Tentativo 7: fallback senza etichetta")
                match = await _discogs_search(client, {
                    "artist": artista, "title": lato_a, "format": fmt_discogs
                })

            if not match and not artista:
                if etichetta and lato_a:
                    print("Tentativo 7b: etichetta + titolo")
                    match = await _discogs_search(client, {"q": f"{etichetta} {lato_a}", "format": fmt_discogs})
                if not match and etichetta and catno:
                    for cv in cvs:
                        print(f"Tentativo 7c: etichetta + catno={cv!r}")
                        match = await _discogs_search(client, {"label": etichetta, "catno": cv})
                        if match: break
                if not match and catno:
                    for cv in cvs:
                        print(f"Tentativo 7d: solo catno={cv!r}")
                        match = await _discogs_search(client, {"catno": cv})
                        if match: break

        if not match:
            print("DISCOGS: nessun match trovato")
            return data

        # Estrai dati
        print(f"DISCOGS MATCH FINALE: {match.get('title')} | {match.get('format')} | {match.get('year')}")

        title_full = match.get("title", "")
        disc_artista, disc_titolo = artista, titolo
        if " - " in title_full:
            parts = title_full.split(" - ", 1)
            if not disc_artista: disc_artista = parts[0].strip()
            if not disc_titolo:  disc_titolo  = parts[1].strip()
        elif not disc_artista and title_full:
            disc_artista = title_full.strip()

        styles    = match.get("style", []) or match.get("genre", [])
        stile     = ", ".join(styles[:2]) if styles else data.get("stile", "")
        formats   = match.get("format", [])
        formato   = data.get("formato") or (formats[0] if formats else "")
        labels    = match.get("label", [])
        etich_out = data.get("etichetta") or (labels[0] if labels else "")
        anno_out  = data.get("anno") or str(match.get("year", ""))
        stampa    = catno or match.get("catno", "")

        if stampa and stampa.isdigit() and len(stampa) >= 10:
            stampa = ""

        if not stampa and match.get("id"):
            try:
                rel_r = await client.get(
                    f"https://api.discogs.com/releases/{match['id']}",
                    headers=DISCOGS_HEADERS()
                )
                if rel_r.status_code == 200:
                    rel_labels = rel_r.json().get("labels", [])
                    stampa = rel_labels[0].get("catno", "") if rel_labels else ""
                    if stampa and stampa.lower() == "none":
                        stampa = ""
            except Exception:
                pass

        stampa_costosa, prezzo_max = "", ""
        if not skip_prices:
            master_id  = match.get("master_id")
            release_id = match.get("id")
            if master_id:
                stampa_costosa, prezzo_max = await cerca_prezzo_max_discogs(master_id)
            if not prezzo_max and release_id:
                try:
                    async with httpx.AsyncClient(timeout=10) as pc:
                        sr = await pc.get(
                            f"https://api.discogs.com/marketplace/stats/{release_id}",
                            headers=DISCOGS_HEADERS()
                        )
                        if sr.status_code == 200:
                            lp = sr.json().get("lowest_price")
                            if lp is not None:
                                price = float(lp.get("value", 0) if isinstance(lp, dict) else lp or 0)
                                if price > 0:
                                    prezzo_max = f"EUR {price:.2f}"
                                    if not stampa_costosa:
                                        stampa_costosa = stampa or match.get("catno", "")
                                    print(f"PREZZO FALLBACK release {release_id}: {prezzo_max}")
                except Exception as e:
                    print(f"PREZZO FALLBACK ERROR: {e}")

        result = {
            "artista":        disc_artista,
            "titolo":         disc_titolo,
            "formato":        formato,
            "stile":          stile,
            "anno":           anno_out,
            "etichetta":      etich_out,
            "stampa":         stampa,
            "stampa_costosa": stampa_costosa,
            "prezzo_max":     prezzo_max,
        }
        print(f"DISCOGS RESULT: {result}")

        await cache_set(ck, result)
        ck2 = cache_key(disc_artista, disc_titolo)
        if ck2 != ck and disc_artista:
            await cache_set(ck2, result)
        return result

# ── Auth ──────────────────────────────────────────────────────────────────────

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

    user_id  = (res.get("user") or {}).get("id") or res.get("id")
    is_admin = bool(ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())

    if user_id:
        async with httpx.AsyncClient() as c2:
            await c2.patch(
                f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}",
                headers={**supa_headers(use_secret=True), "Prefer": "return=minimal"},
                json={"approved": is_admin, "email": data.email, "nome": data.nome}
            )

    msg = "Registrazione completata." if is_admin else "Registrazione completata. Attendi l'approvazione dell'amministratore."
    return {"status": "ok", "message": msg}

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

    user_id  = res["user"]["id"]
    nome     = res["user"].get("user_metadata", {}).get("nome", data.email)
    is_admin = bool(ADMIN_EMAIL and data.email.lower() == ADMIN_EMAIL.lower())

    # Verifica approvazione (non per admin)
    if not is_admin:
        try:
            async with httpx.AsyncClient() as c2:
                prof = await c2.get(
                    f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}&select=approved",
                    headers=supa_headers(use_secret=True)
                )
                profiles = prof.json() if prof.status_code == 200 else []
                # Blocca solo se il profilo esiste E approved è esplicitamente False
                if profiles and profiles[0].get("approved") is False:
                    raise HTTPException(403, "Account in attesa di approvazione.")
        except HTTPException:
            raise
        except Exception as e:
            print(f"APPROVAL CHECK ERROR: {e}")

    return {
        "access_token": res["access_token"],
        "user_id":      user_id,
        "nome":         nome,
        "is_admin":     is_admin,
    }

# ── Admin ─────────────────────────────────────────────────────────────────────

@app.get("/api/admin/users")
async def admin_get_users(token: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/profiles?select=id,email,nome,approved,created_at&order=created_at.desc",
            headers=supa_headers(use_secret=True)
        )
    if r.status_code != 200:
        raise HTTPException(400, "Errore recupero utenti")
    return r.json()

@app.post("/api/admin/approve/{user_id}")
async def admin_approve(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}",
            headers={**supa_headers(use_secret=True), "Prefer": "return=minimal"},
            json={"approved": True}
        )
    if r.status_code not in (200, 204):
        raise HTTPException(400, "Errore approvazione")
    return {"status": "approved"}

@app.post("/api/admin/block/{user_id}")
async def admin_block(user_id: str, token: str):
    async with httpx.AsyncClient() as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}",
            headers={**supa_headers(use_secret=True), "Prefer": "return=minimal"},
            json={"approved": False}
        )
    if r.status_code not in (200, 204):
        raise HTTPException(400, "Errore blocco")
    return {"status": "blocked"}

# ── Scan ──────────────────────────────────────────────────────────────────────

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    gemini_data = {
        "artista": "", "titolo": "", "formato": "", "stile": "",
        "anno": "", "etichetta": "", "stampa": "", "stampa_costosa": "", "prezzo_max": ""
    }

    if GEMINI_KEY:
        try:
            b64  = base64.b64encode(content).decode()
            mime = file.content_type or "image/jpeg"
            url  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
            prompt = """Sei un esperto di dischi vinile. Analizza questa etichetta e rispondi SOLO con JSON valido senza markdown:
{"artista":"","titolo":"","formato":"","stile":"","anno":"","etichetta":"","stampa":"","barcode":"","lato":""}

REGOLE:
- "artista" = nome artista/band ovunque sull'etichetta. Se lato B senza artista visibile lascia vuoto.
- "titolo" = titolo brano/album. Lato A: titolo A. Lato B: titolo B. Entrambi: "Titolo A / Titolo B".
- "lato" = "A", "B", o "".
- "formato" = 7", 10", 12", LP, EP. Deducilo dalla dimensione: etichette piccole=7", grandi=LP.
- "stile" = genere musicale. Deducilo dall'etichetta (Blue Note=Jazz, Motown=Soul, Trojan=Reggae).
- "anno" = anno a 4 cifre. Cerca nel copyright ©. NON confondere con numeri catalogo.
- "etichetta" = nome etichetta discografica vicino al logo.
- "stampa" = catalog number (es: CBS 1234, MAF008). NON è il barcode EAN numerico lungo.
- "barcode" = sequenza di 8, 12 o 13 cifre sotto il grafico a barre.
- Deduci campi non visibili dal contesto. Solo se impossibile lascia vuoto."""

            async with httpx.AsyncClient(timeout=45) as client:
                r = await client.post(url, json={"contents": [{"parts": [
                    {"text": prompt},
                    {"inline_data": {"mime_type": mime, "data": b64}}
                ]}]})

            print(f"GEMINI STATUS: {r.status_code}")

            if r.status_code == 429:
                print(f"GEMINI RATE LIMIT: {r.text[:200]}")
                gemini_data["_error"] = "quota_gemini"
            elif r.status_code != 200:
                print(f"GEMINI ERROR {r.status_code}: {r.text[:300]}")
                gemini_data["_error"] = f"gemini_{r.status_code}"
            else:
                rj = r.json()
                candidates = rj.get("candidates", [])
                if not candidates:
                    reason = rj.get("promptFeedback", {}).get("blockReason", "UNKNOWN")
                    print(f"GEMINI BLOCKED: {reason}")
                    gemini_data["_error"] = f"blocked_{reason}"
                else:
                    finish = candidates[0].get("finishReason", "")
                    parts  = candidates[0].get("content", {}).get("parts", [])
                    print(f"GEMINI FINISH: {finish}")
                    if parts:
                        text = parts[0].get("text", "").strip().replace("```json","").replace("```","").strip()
                        print(f"GEMINI TEXT: {text[:400]}")
                        try:
                            parsed = json.loads(text)
                            # Sanity: barcode come catno
                            sv = str(parsed.get("stampa", "") or "")
                            if sv.isdigit() and len(sv) >= 10:
                                parsed["stampa"] = ""
                            else:
                                fixed = fix_catno_ocr(sv)
                                if fixed != sv:
                                    print(f"CATNO NORMALIZED: {sv!r} -> {fixed!r}")
                                    parsed["stampa"] = fixed
                            gemini_data.update(parsed)
                        except Exception as pe:
                            print(f"JSON PARSE ERROR: {pe}")
        except Exception as e:
            print(f"GEMINI EXCEPTION: {e}")
            gemini_data["_error"] = "exception"

    # Estrai barcode e lato (non salvati nel DB)
    barcode_scan = extract_barcode(str(gemini_data.pop("barcode", "") or ""))
    lato_scan    = str(gemini_data.pop("lato", "") or "").strip().upper()

    # Se stampa sembra barcode, spostalo
    bc_from_stampa = extract_barcode(str(gemini_data.get("stampa", "") or ""))
    if bc_from_stampa and not barcode_scan:
        barcode_scan = bc_from_stampa
        gemini_data["stampa"] = ""

    print(f"BARCODE SCAN: {barcode_scan!r} LATO: {lato_scan!r}")

    if lato_scan == "B" and not gemini_data.get("artista"):
        gemini_data["_titolo_lato_b"] = gemini_data.get("titolo", "")
        gemini_data["titolo"] = ""

    result = await cerca_su_discogs(gemini_data, use_cache=True, barcode=barcode_scan)

    if lato_scan == "B" and not result.get("artista") and gemini_data.get("_titolo_lato_b"):
        result["titolo"] = gemini_data["_titolo_lato_b"]

    result["catno"] = result.get("stampa", "")
    if gemini_data.get("_error"):
        result["_error"] = gemini_data["_error"]
    return result

# ── Enrich single ─────────────────────────────────────────────────────────────

@app.post("/api/enrich_single")
async def enrich_single(req: EnrichRequest):
    data = {k: getattr(req, k) or "" for k in
            ["artista","titolo","formato","stile","anno","etichetta","stampa","stampa_costosa","prezzo_max"]}
    enriched = await cerca_su_discogs(data, use_cache=True, fast_mode=True)
    enriched["catno"] = enriched.get("stampa", "")
    return enriched

# ── Vinili CRUD ───────────────────────────────────────────────────────────────

@app.post("/api/vinile")
async def add_vinyl(v: VinylData):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/vinili",
            headers=supa_headers(v.access_token),
            json={
                "user_id": v.user_id, "artista": v.artista, "titolo": v.titolo,
                "formato": v.formato, "stile": v.stile, "anno": v.anno,
                "etichetta": v.etichetta, "stampa": v.stampa,
                "stampa_costosa": v.stampa_costosa, "prezzo_max": v.prezzo_max
            }
        )
    if r.status_code not in (200, 201):
        raise HTTPException(400, f"Errore salvataggio: {r.status_code}")
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

@app.patch("/api/vinile/{vinyl_id}")
async def update_vinyl(vinyl_id: int, data: VinylUpdate):
    token  = data.token or ""
    update = {k: v for k, v in data.dict().items() if k != "token" and v is not None}
    print(f"PATCH vinyl {vinyl_id}: fields={list(update.keys())}")
    if not update:
        return {"status": "nothing_to_update"}
    async with httpx.AsyncClient() as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vinyl_id}",
            headers={**supa_headers(token), "Prefer": "return=minimal"},
            json=update
        )
    print(f"PATCH vinyl {vinyl_id}: status={r.status_code}")
    if r.status_code not in (200, 204):
        raise HTTPException(400, f"Errore aggiornamento: {r.status_code}")
    return {"status": "updated"}

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
            headers={**supa_headers(token), "Prefer": "return=minimal"}
        )
    print(f"DELETE CATALOG: {r.status_code}")
    if r.status_code not in (200, 204):
        raise HTTPException(400, f"Errore eliminazione catalogo: {r.status_code}")
    return {"status": "deleted"}

# ── Import Excel ──────────────────────────────────────────────────────────────

EXCEL_SKIP = {'7"','10"','12"','4"','lp','2xlp','2x lp','ep','single','riepilogo formati','totale vinili','artista'}

def parse_excel_rows(ws) -> list[dict]:
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        artista = str(row[0] or "").strip()
        if artista.lower() in EXCEL_SKIP:
            continue
        if len(artista) <= 5 and len(row) > 1 and str(row[1] or "").strip().isdigit():
            continue
        stampa_raw = str(row[6] or "").strip()
        barcode_raw = str(row[9] or "").strip() if len(row) > 9 else ""
        if stampa_raw and stampa_raw.isdigit() and len(stampa_raw) >= 8:
            barcode_raw = barcode_raw or stampa_raw
            stampa_raw = ""
        rows.append({
            "artista":        artista,
            "titolo":         str(row[1] or "").strip(),
            "formato":        str(row[2] or "").strip(),
            "stile":          str(row[3] or "").strip(),
            "anno":           str(row[4] or "").strip(),
            "etichetta":      str(row[5] or "").strip(),
            "stampa":         stampa_raw,
            "stampa_costosa": str(row[7] or "").strip() if len(row) > 7 else "",
            "prezzo_max":     str(row[8] or "").strip() if len(row) > 8 else "",
        })
    return rows

@app.post("/api/parse_excel")
async def parse_excel(user_id: str = Form(...), token: str = Form(...), file: UploadFile = File(...)):
    content_bytes = await file.read()
    ws = load_workbook(io.BytesIO(content_bytes)).active
    rows = parse_excel_rows(ws)
    return {"rows": rows, "total": len(rows)}

@app.post("/api/import_batch")
async def import_batch(user_id: str = Form(...), token: str = Form(...), rows_json: str = Form(...)):
    rows = json.loads(rows_json)
    saved = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for v in rows:
            try:
                r = await client.post(
                    f"{SUPABASE_URL}/rest/v1/vinili",
                    headers=supa_headers(token),
                    json={"user_id": user_id, **{k: v.get(k,"") for k in
                          ["artista","titolo","formato","stile","anno","etichetta","stampa","stampa_costosa","prezzo_max"]}}
                )
                if r.status_code in (200, 201):
                    saved += 1
            except Exception as e:
                print(f"IMPORT BATCH ERROR: {e}")
    return {"saved": saved}

# Mantieni import_excel SSE per compatibilità
@app.post("/api/import_excel")
async def import_excel(user_id: str = Form(...), token: str = Form(...), file: UploadFile = File(...)):
    content_bytes = await file.read()
    ws   = load_workbook(io.BytesIO(content_bytes)).active
    rows = parse_excel_rows(ws)
    total = len(rows)

    async def generate():
        imported = 0
        async with httpx.AsyncClient(timeout=20) as client:
            for idx, v in enumerate(rows):
                yield f"data: {json.dumps({'done':False,'current':idx+1,'total':total,'artista':v['artista']},ensure_ascii=False)}\n\n"
                try:
                    r = await client.post(
                        f"{SUPABASE_URL}/rest/v1/vinili",
                        headers=supa_headers(token),
                        json={"user_id": user_id, **{k: v.get(k,"") for k in
                              ["artista","titolo","formato","stile","anno","etichetta","stampa","stampa_costosa","prezzo_max"]}}
                    )
                    if r.status_code in (200, 201):
                        imported += 1
                except Exception as e:
                    print(f"SAVE ERROR row {idx}: {e}")
        yield f"data: {json.dumps({'done':True,'imported':imported})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# ── Export Excel ──────────────────────────────────────────────────────────────

@app.post("/api/export_excel")
async def export_excel_post(user_id: str = Form(...), token: str = Form(...)):
    return await _build_excel_response(user_id, token)

@app.get("/api/export_excel/{user_id}")
async def export_excel(user_id: str, token: str = ""):
    return await _build_excel_response(user_id, token)

async def _build_excel_response(user_id: str, token: str):
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

    hfill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
    hfont = Font(color="FFFFFF", bold=True, size=11)
    headers = ["Artista","Titolo","Formato","Stile","Anno","Etichetta","Stampa","Stampa piu Costosa","Prezzo medio piu alto"]
    widths  = [25, 30, 12, 20, 8, 20, 15, 22, 22]

    for col, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col, value=h)
        c.fill = hfill; c.font = hfont
        c.alignment = Alignment(horizontal="center")

    for ri, v in enumerate(vinili, 2):
        for ci, key in enumerate(["artista","titolo","formato","stile","anno","etichetta","stampa","stampa_costosa","prezzo_max"], 1):
            val = v.get(key, "") or (v.get("catno","") if key == "stampa" else "")
            ws.cell(row=ri, column=ci, value=val)

    for col, w in enumerate(widths, 1):
        ws.column_dimensions[ws.cell(row=1,column=col).column_letter].width = w

    ws.freeze_panes = "A2"

    # Riepilogo formati
    fc = Counter()
    for v in vinili:
        fmt = str(v.get("formato","") or "").strip().lower()
        if not fmt:                                        fc["altro"] += 1
        elif '7' in fmt or '45' in fmt:                   fc['7"'] += 1
        elif '10' in fmt:                                  fc['10"'] += 1
        elif '12' in fmt:                                  fc['12"'] += 1
        elif '2xlp' in fmt or '2x lp' in fmt:            fc['2xLP'] += 1
        elif 'lp' in fmt or '33' in fmt:                  fc['LP'] += 1
        elif 'ep' in fmt:                                  fc['EP'] += 1
        else:                                              fc[fmt[:10]] += 1

    lr = ws.max_row + 2
    tc = ws.cell(row=lr, column=1, value="RIEPILOGO FORMATI")
    tc.font = Font(bold=True, color="FFFFFF")
    tc.fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
    lr += 1

    for fmt, cnt in sorted(fc.items()):
        if not cnt: continue
        for ci, val in enumerate([fmt, cnt], 1):
            c = ws.cell(row=lr, column=ci, value=val)
            c.font = Font(bold=True, color="FFFFFF")
            c.fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
        lr += 1

    for ci, val in enumerate(["TOTALE VINILI", len(vinili)], 1):
        c = ws.cell(row=lr, column=ci, value=val)
        c.font = Font(bold=True, color="FFFFFF", size=12)
        c.fill = PatternFill(start_color="4a0080", end_color="4a0080", fill_type="solid")

    out = io.BytesIO()
    wb.save(out); out.seek(0)
    return Response(
        content=out.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=Catalogo_Vinili.xlsx",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        }
    )

# ── Enrich batch ──────────────────────────────────────────────────────────────

@app.post("/api/enrich_batch")
async def enrich_batch(user_id: str = Form(...), token: str = Form(...),
                       offset: int = Form(default=0), batch_size: int = Form(default=30),
                       skip_prices: int = Form(default=0)):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&order=id.asc&limit={batch_size}&offset={offset}",
            headers=supa_headers(token)
        )
    if r.status_code != 200:
        raise HTTPException(400, "Errore recupero dati")

    vinili = r.json()
    total_batch = len(vinili)

    async def generate():
        enriched_count = 0
        for idx, v in enumerate(vinili):
            vid       = v.get("id")
            artista   = v.get("artista","") or ""
            stile     = v.get("stile","") or ""
            anno      = v.get("anno","") or ""
            etichetta = v.get("etichetta","") or ""
            stampa    = v.get("stampa","") or ""
            sc        = v.get("stampa_costosa","") or ""
            pm        = v.get("prezzo_max","") or ""

            if all([stile, anno, etichetta, stampa, sc, pm]):
                yield f"data: {json.dumps({'done':False,'current':idx+1,'total':total_batch,'artista':artista,'skipped':True},ensure_ascii=False)}\n\n"
                continue

            yield f"data: {json.dumps({'done':False,'current':idx+1,'total':total_batch,'artista':artista,'skipped':False},ensure_ascii=False)}\n\n"
            await asyncio.sleep(1.2)

            try:
                enriched = await cerca_su_discogs({k: v.get(k,"") or "" for k in
                    ["artista","titolo","formato","stile","anno","etichetta","stampa","stampa_costosa","prezzo_max"]
                }, use_cache=True, skip_prices=bool(skip_prices))

                update = {}
                if not stile     and enriched.get("stile"):          update["stile"]          = enriched["stile"]
                if not anno      and enriched.get("anno"):           update["anno"]           = enriched["anno"]
                if not etichetta and enriched.get("etichetta"):      update["etichetta"]      = enriched["etichetta"]
                if not stampa    and enriched.get("stampa"):         update["stampa"]         = enriched["stampa"]
                if not sc        and enriched.get("stampa_costosa"): update["stampa_costosa"] = enriched["stampa_costosa"]
                if not pm        and enriched.get("prezzo_max"):     update["prezzo_max"]     = enriched["prezzo_max"]
                if not v.get("artista") and enriched.get("artista"): update["artista"]        = enriched["artista"]
                if not v.get("titolo")  and enriched.get("titolo"):  update["titolo"]         = enriched["titolo"]
                if not v.get("formato") and enriched.get("formato"): update["formato"]        = enriched["formato"]

                if update:
                    async with httpx.AsyncClient(timeout=10) as upd:
                        await upd.patch(
                            f"{SUPABASE_URL}/rest/v1/vinili?id=eq.{vid}",
                            headers={**supa_headers(token), "Prefer": "return=minimal"},
                            json=update
                        )
                    enriched_count += 1
            except Exception as e:
                print(f"ENRICH BATCH ERROR id={vid}: {e}")

        next_offset = offset + total_batch
        yield f"data: {json.dumps({'done':True,'enriched':enriched_count,'total':total_batch,'next_offset':next_offset,'has_more':total_batch==batch_size})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.get("/api/count/{user_id}")
async def count_vinyls(user_id: str, token: str):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/vinili?user_id=eq.{user_id}&select=id",
            headers={**supa_headers(token), "Prefer": "count=exact", "Range": "0-0"}
        )
    total = int(r.headers.get("content-range", "0/0").split("/")[-1] or 0)
    return {"total": total}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()
