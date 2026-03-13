import os
import io
import json
import base64
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

# ── Cache Discogs su Supabase ─────────────────────────────────────────────────

def cache_key(artista: str, titolo: str) -> str:
    """Chiave normalizzata per la cache: artista|titolo in minuscolo."""
    a = (artista or "").strip().lower()
    t = (titolo or "").strip().lower()
    return f"{a}|{t}"

async def cache_get(key: str) -> dict | None:
    """Legge un record dalla cache Discogs su Supabase."""
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
    """Salva un record nella cache Discogs su Supabase (upsert)."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            payload = {
                "cache_key": key,
                "artista": data.get("artista", ""),
                "titolo": data.get("titolo", ""),
                "etichetta": data.get("etichetta", ""),
                "stile": data.get("stile", ""),
                "anno": data.get("anno", ""),
                "formato": data.get("formato", ""),
                "stampa": data.get("stampa", ""),
                "stampa_costosa": data.get("stampa_costosa", ""),
                "prezzo_max": data.get("prezzo_max", ""),
            }
            await client.post(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={
                    **supa_headers(use_secret=True),
                    "Prefer": "resolution=merge-duplicates,return=minimal"
                },
                json=payload
            )
            print(f"CACHE SET: {key}")
    except Exception as e:
        print(f"CACHE SET ERROR: {e}")

# ── Discogs ───────────────────────────────────────────────────────────────────

async def cerca_prezzo_max_discogs(master_id: int) -> tuple[str, str]:
    """Trova la versione con prezzo più alto tra quelle disponibili su Discogs."""
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.get(
                f"https://api.discogs.com/masters/{master_id}/versions",
                headers=DISCOGS_HEADERS(),
                params={"per_page": 100}
            )
            if r.status_code != 200:
                return "", ""

            versions = r.json().get("versions", [])
            print(f"DISCOGS VERSIONS: {len(versions)} for master {master_id}")

            best_price = 0.0
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
                if stats_r.status_code != 200:
                    continue

                stats = stats_r.json()
                lp = stats.get("lowest_price")
                if lp is None:
                    continue
                price = float(lp.get("value", 0) if isinstance(lp, dict) else lp or 0)

                if price > best_price:
                    best_price = price
                    best_catno = catno if catno else f"ID:{release_id}"

            if best_price > 0:
                return best_catno, f"EUR {best_price:.2f}"
            # Fallback: restituisci il catno della prima versione con catno valido
            for v in versions:
                catno = v.get("catno", "")
                if catno and catno.lower() != "none":
                    return catno, ""
            return "", ""

    except Exception as e:
        print(f"DISCOGS PRICE ERROR: {e}")
        return "", ""


# ── Helpers ricerca Discogs ───────────────────────────────────────────────────

def formato_to_discogs(fmt: str) -> str:
    """Converte il formato utente nel tipo Discogs corrispondente."""
    f = fmt.lower().strip()
    if "12" in f:                          return '12"'
    if "7" in f or "45" in f:             return '7"'
    if "10" in f:                          return '10"'
    if "2xlp" in f or "2x lp" in f or "double" in f: return "LP"
    if "lp" in f or "33" in f:            return "LP"
    if "ep" in f:                          return "EP"
    return "Vinyl"

def formati_alternativi_discogs(fmt_discogs: str) -> list[str]:
    """
    Restituisce i formati alternativi Discogs da provare se il primo fallisce.
    Discogs registra i 7" come: '7"', '45 RPM', 'Single', 'Vinyl'.
    Discogs registra i 12" come: '12"', 'Maxi-Single', 'Vinyl'.
    """
    if fmt_discogs == '7"':
        return ["45 RPM", "Single", "Vinyl"]
    if fmt_discogs == '12"':
        return ["Maxi-Single", "Vinyl"]
    if fmt_discogs == "LP":
        return ["Vinyl"]
    if fmt_discogs == "EP":
        return ["Vinyl"]
    return []

def is_7inch(fmt: str) -> bool:
    f = fmt.lower()
    return "7" in f or "45" in f

def extract_barcode(s: str) -> str:
    """Estrae EAN-8/12/13 o UPC da stringa. Solo cifre, lunghezza 8/12/13."""
    import re
    s_clean = re.sub(r'[\s\-]', '', s or '')
    matches = re.findall(r'\d{8,13}', s_clean)
    for m in matches:
        if len(m) in (8, 12, 13):
            return m
    return ""

def fix_catno_ocr(s: str) -> str:
    """Corregge errori OCR nei catno: O->0 e I->1 adiacenti a cifre."""
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

def split_lati(titolo: str, fmt: str) -> tuple[str, str]:
    """
    Separa lato A e lato B solo per 7"/45rpm.
    Restituisce (lato_a, lato_b). lato_b è vuoto se non applicabile.
    """
    if not is_7inch(fmt):
        return titolo.strip(), ""
    for sep in [" / ", "/", " - "]:
        if sep in titolo:
            parts = titolo.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    return titolo.strip(), ""

async def _discogs_search(client, params: dict) -> dict | None:
    """Esegue una ricerca Discogs e restituisce il primo risultato vinile."""
    try:
        params.setdefault("type", "release")
        params.setdefault("per_page", 5)
        r = await client.get(
            "https://api.discogs.com/database/search",
            headers=DISCOGS_HEADERS(),
            params=params
        )
        if r.status_code == 429:
            print("DISCOGS RATE LIMIT")
            return None
        if r.status_code != 200:
            return None
        results = r.json().get("results", [])
        # Preferisci risultati vinile
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

async def cerca_su_discogs(data: dict, use_cache: bool = True, barcode: str = "") -> dict:
    """
    Arricchisce i dati con Discogs seguendo una cascata di ricerche
    dal più preciso al più generico. Formato sempre specificato.
    Lato A + Lato B usati insieme per i 7"/45rpm se presenti nel titolo.
    Artista e titolo originali non vengono mai sovrascritti.
    """
    if not DISCOGS_TOKEN:
        return data

    artista   = (data.get("artista")   or "").strip()
    titolo    = (data.get("titolo")    or "").strip()
    fmt_raw   = (data.get("formato")   or "").strip()
    etichetta = (data.get("etichetta") or "").strip()
    anno      = (data.get("anno")      or "").strip()
    catno     = fix_catno_ocr((data.get("stampa") or "").strip())

    ck = cache_key(artista, titolo)

    # ── Cache ────────────────────────────────────────────────────────────────
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
    ha_lati = bool(lato_b)  # True solo se 7"/45rpm con split trovato

    # Normalizza catno: prova sia con che senza spazi (es. MAF008 e MAF 008)
    catno_variants = [catno]
    if catno and ' ' not in catno and len(catno) > 3:
        # Prova a inserire uno spazio prima degli ultimi 3 digit (es. MAF008 -> MAF 008)
        import re as _re
        m = _re.match(r'^([A-Za-z-]+)(\d+)$', catno)
        if m:
            catno_variants.append(f"{m.group(1)} {m.group(2)}")
    elif catno and ' ' in catno:
        catno_variants.append(catno.replace(' ', ''))

    print(f"DISCOGS CERCA: artista={artista!r} lato_a={lato_a!r} lato_b={lato_b!r} "
          f"fmt={fmt_discogs!r} etichetta={etichetta!r} anno={anno!r} catno={catno!r} variants={catno_variants!r}")

    match = None
    async with httpx.AsyncClient(timeout=15) as client:

        # 0. barcode → match esatto sulla pressatura
        if barcode:
            print(f"Tentativo 0: barcode={barcode}")
            match = await _discogs_search(client, {"barcode": barcode})
            if match: print(f"MATCH via barcode!")

        # 1. catno + etichetta + formato — prova tutte le varianti del catno
        if not match and catno and not catno.isdigit():
            for cv in catno_variants:
                print(f"Tentativo 1: catno={cv!r} + etichetta + formato")
                p = {"catno": cv, "format": fmt_discogs}
                if etichetta: p["label"] = etichetta
                match = await _discogs_search(client, p)
                if match: break

        # 2. catno + formato (senza etichetta)
        if not match and catno and not catno.isdigit():
            for cv in catno_variants:
                print(f"Tentativo 2: catno={cv!r} + formato")
                match = await _discogs_search(client, {"catno": cv, "format": fmt_discogs})
                if match: break

        # 2b. catno + formati alternativi Discogs (es. 7" -> 45 RPM, Single, Vinyl)
        if not match and catno and not catno.isdigit():
            for cv in catno_variants:
                for fmt_alt in formati_alternativi_discogs(fmt_discogs):
                    print(f"Tentativo 2b: catno={cv!r} + formato alternativo '{fmt_alt}'")
                    match = await _discogs_search(client, {"catno": cv, "format": fmt_alt})
                    if match: break
                if match: break

        # 3. lato A + lato B + etichetta + formato (solo 7"/45rpm con split)
        if not match and ha_lati and etichetta:
            print("Tentativo 3: latoA + latoB + etichetta + formato")
            match = await _discogs_search(client, {
                "artist": artista, "title": f"{lato_a} {lato_b}",
                "label": etichetta, "format": fmt_discogs
            })

        # 4. lato A + lato B + formato (senza etichetta, solo 7"/45rpm)
        if not match and ha_lati:
            print("Tentativo 4: latoA + latoB + formato")
            match = await _discogs_search(client, {
                "artist": artista, "title": f"{lato_a} {lato_b}",
                "format": fmt_discogs
            })

        # 5. artista + titolo + etichetta + formato specifico
        if not match and artista and lato_a:
            print("Tentativo 5: artista + titolo + etichetta + formato")
            p = {"artist": artista, "title": lato_a, "format": fmt_discogs}
            if etichetta: p["label"] = etichetta
            match = await _discogs_search(client, p)

        # 5b. artista + titolo + formati alternativi (es. 45 RPM invece di 7")
        if not match and artista and lato_a:
            for fmt_alt in formati_alternativi_discogs(fmt_discogs):
                print(f"Tentativo 5b: artista + titolo + formato alternativo '{fmt_alt}'")
                p = {"artist": artista, "title": lato_a, "format": fmt_alt}
                if etichetta: p["label"] = etichetta
                match = await _discogs_search(client, p)
                if match: break

        # 6. artista + titolo + etichetta + anno + formato
        if not match and artista and lato_a and anno:
            print("Tentativo 6: artista + titolo + etichetta + anno + formato")
            p = {"artist": artista, "title": lato_a, "format": fmt_discogs, "year": anno}
            if etichetta: p["label"] = etichetta
            match = await _discogs_search(client, p)

        # 7. artista + titolo + formato (fallback minimo)
        if not match and artista and lato_a:
            print("Tentativo 7: artista + titolo + formato (fallback)")
            match = await _discogs_search(client, {
                "artist": artista, "title": lato_a, "format": fmt_discogs
            })

        # 7b. se artista vuoto ma etichetta presente: q=etichetta + titolo
        if not match and not artista and etichetta and lato_a:
            print("Tentativo 7b: etichetta + titolo (artista mancante)")
            match = await _discogs_search(client, {
                "q": f"{etichetta} {lato_a}", "format": fmt_discogs
            })

        # 7c. solo etichetta + catno (artista e titolo entrambi vuoti), tutte le varianti
        if not match and not artista and not lato_a and etichetta and catno:
            for cv in catno_variants:
                print(f"Tentativo 7c: etichetta={etichetta!r} + catno={cv!r}")
                match = await _discogs_search(client, {"label": etichetta, "catno": cv})
                if match: break

        # 7d. solo catno senza etichetta (ultimo tentativo se abbiamo solo catno)
        if not match and not artista and not lato_a and catno:
            for cv in catno_variants:
                print(f"Tentativo 7d: solo catno={cv!r} senza altri filtri")
                match = await _discogs_search(client, {"catno": cv})
                if match: break

        if not match:
            print("DISCOGS: nessun match trovato")
            return data

        # ── Estrai dati dal match ─────────────────────────────────────────────
        print(f"DISCOGS MATCH FINALE: {match.get('title')} | {match.get('format')} | {match.get('year')}")

        title_full = match.get("title", "")
        # Se artista o titolo sono vuoti, estraili sempre dal title_full di Discogs
        disc_artista = artista
        disc_titolo  = titolo
        if " - " in title_full:
            parts = title_full.split(" - ", 1)
            if not disc_artista:
                disc_artista = parts[0].strip()
            if not disc_titolo:
                disc_titolo = parts[1].strip()
        elif not disc_artista and title_full:
            disc_artista = title_full.strip()

        styles    = match.get("style", []) or match.get("genre", [])
        stile     = ", ".join(styles[:2]) if styles else data.get("stile", "")
        formats   = match.get("format", [])
        formato   = data.get("formato") or (formats[0] if formats else "")
        labels    = match.get("label", [])
        etich_out = data.get("etichetta") or (labels[0] if labels else "")
        anno_out  = data.get("anno") or str(match.get("year", ""))
        # Catno: quello dell'utente ha priorità, poi quello del match
        stampa    = catno or match.get("catno", "")
        # Se catno dal match sembra un barcode, scartalo
        if stampa and stampa.isdigit() and len(stampa) >= 10:
            stampa = ""
        # Se ancora vuoto, prova a recuperarlo dalla release diretta
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

        # Stampa costosa e prezzo: prova master, poi release diretta
        stampa_costosa = ""
        prezzo_max     = ""
        master_id  = match.get("master_id")
        release_id = match.get("id")
        if master_id:
            stampa_costosa, prezzo_max = await cerca_prezzo_max_discogs(master_id)
        # Fallback: se no master o prezzo non trovato, prova la release stessa
        if not prezzo_max and release_id:
            try:
                async with httpx.AsyncClient(timeout=10) as pc:
                    sr = await pc.get(
                        f"https://api.discogs.com/marketplace/stats/{release_id}",
                        headers=DISCOGS_HEADERS()
                    )
                    if sr.status_code == 200:
                        stats = sr.json()
                        lp = stats.get("lowest_price")
                        if lp is not None:
                            price = float(lp.get("value", 0) if isinstance(lp, dict) else lp or 0)
                            if price > 0:
                                prezzo_max = f"EUR {price:.2f}"
                                # catno della release come stampa_costosa se non abbiamo già un master
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

        # Salva in cache (con artista/titolo da Discogs per lookup futuri)
        await cache_set(ck, result)
        # Salva anche con chiave disc_artista|disc_titolo se diversa
        ck2 = cache_key(disc_artista, disc_titolo)
        if ck2 != ck and disc_artista:
            await cache_set(ck2, result)
        return result



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

# ── Scan ──────────────────────────────────────────────────────────────────────

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    content = await file.read()
    gemini_data = {
        "artista": "", "titolo": "", "formato": "", "stile": "",
        "anno": "", "etichetta": "", "stampa": "",
        "stampa_costosa": "", "prezzo_max": ""
    }

    if GEMINI_KEY:
        try:
            b64 = base64.b64encode(content).decode()
            mime = file.content_type or "image/jpeg"
            gemini_url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
            )
            prompt = """Analizza questa immagine di un'etichetta di disco vinile.
Estrai le informazioni visibili e rispondi SOLO con JSON valido senza markdown:
{"artista":"","titolo":"","formato":"","stile":"","anno":"","etichetta":"","stampa":"","barcode":"","lato":""}

REGOLE FONDAMENTALI:
- "stampa" = numero di catalogo (catalog number). Esempi: CBS 1234, HS-032, MAF008, CLMN-126.
  Si trova vicino al logo etichetta, sul bordo, o inciso nella plastica.
  NON e' il codice EAN/barcode numerico lungo. NON e' il numero ISRC.
  Se non trovi un catalog number chiaro, lascia "stampa" vuoto.
- "artista" = nome dell'artista o band principale. Se e' il lato B e non e' visibile, lascia vuoto.
- "titolo" = titolo del brano/album.
  Se e' il LATO A: usa il titolo del lato A.
  Se e' il LATO B: metti il titolo del lato B nel campo titolo.
  Se entrambi i lati sono visibili: usa formato "Titolo A / Titolo B".
- "lato" = "A" se e' chiaramente il lato A, "B" se e' il lato B, "" se non visibile o irrilevante (LP).
- "formato" = 7", 10", 12", LP, EP, 45rpm, 33rpm. Per i 7" il formato e' quasi sempre 7".
- "stile" = genere musicale. Se non visibile deducilo dall'etichetta (Blue Note=Jazz, Motown=Soul).
- "anno" = anno a 4 cifre. NON confondere con numeri di catalogo.
- "etichetta" = nome etichetta discografica.
- "barcode" = sequenza numerica EAN/UPC di 8, 12 o 13 cifre sotto il barcode grafico.
- Lascia vuoto qualsiasi campo non chiaramente visibile. NON inventare."""
            payload = {"contents": [{"parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": mime, "data": b64}}
            ]}]}
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(gemini_url, json=payload)
            if r.status_code == 200:
                text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                text = text.strip().replace("```json", "").replace("```", "").strip()
                try:
                    parsed = json.loads(text)
                    stampa_val = str(parsed.get("stampa", "")).strip()
                    if stampa_val.isdigit() and len(stampa_val) >= 10:
                        parsed["stampa"] = ""
                    # Gemini confonde O->0 e I->1 nei catno (errore OCR)
                    sv = str(parsed.get("stampa", "") or "")
                    if sv:
                        fixed = fix_catno_ocr(sv)
                        if fixed != sv:
                            print(f"CATNO NORMALIZED: {sv!r} -> {fixed!r}")
                            parsed["stampa"] = fixed
                    gemini_data.update(parsed)
                except Exception as pe:
                    print(f"JSON PARSE ERROR: {pe}")
        except Exception as e:
            print(f"GEMINI EXCEPTION: {e}")

    # Estrai barcode (non salvato nel DB)
    barcode_scan = extract_barcode(str(gemini_data.pop("barcode", "") or ""))
    lato_scan = str(gemini_data.pop("lato", "") or "").strip().upper()

    # Se stampa sembra un barcode, spostalo
    bc_from_stampa = extract_barcode(str(gemini_data.get("stampa", "") or ""))
    if bc_from_stampa and not barcode_scan:
        barcode_scan = bc_from_stampa
        gemini_data["stampa"] = ""

    print(f"BARCODE SCAN: {barcode_scan!r} LATO: {lato_scan!r}")

    # Se e' lato B senza artista: il titolo lato B va messo come lato B nella ricerca
    # Discogs cercherà il 7" per catno/etichetta, non per titolo lato B
    # Il titolo viene tenuto come lato B ma non viene sovrascritto l'artista
    if lato_scan == "B" and not gemini_data.get("artista"):
        # Sposta titolo lato B in un campo temporaneo, la ricerca userà catno/etichetta
        gemini_data["_titolo_lato_b"] = gemini_data.get("titolo", "")
        gemini_data["titolo"] = ""  # titolo lato B da solo non è utile per la ricerca principale

    result = await cerca_su_discogs(gemini_data, use_cache=True, barcode=barcode_scan)

    # Se la ricerca ha trovato qualcosa ma artista/titolo sono vuoti
    # e avevamo un titolo lato B, non sovrascriverlo
    if lato_scan == "B" and not result.get("artista") and gemini_data.get("_titolo_lato_b"):
        result["titolo"] = gemini_data["_titolo_lato_b"]
    result["catno"] = result.get("stampa", "")
    return result

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
            headers={**supa_headers(token), "Prefer": "return=minimal"}
        )
    print(f"DELETE CATALOG STATUS: {r.status_code} BODY: {r.text[:200]}")
    if r.status_code not in (200, 204):
        raise HTTPException(400, f"Errore eliminazione catalogo: {r.status_code}")
    return {"status": "deleted"}

# ── Import Excel con SSE progress + arricchimento + cache ─────────────────────

@app.post("/api/import_excel")
async def import_excel(
    user_id: str = Form(...),
    token: str = Form(...),
    file: UploadFile = File(...)
):
    content_bytes = await file.read()
    wb = load_workbook(io.BytesIO(content_bytes))
    ws = wb.active
    SKIP = {
        '7"', '10"', '12"', '4"', 'lp', '2xlp', '2x lp', 'ep',
        'single', 'riepilogo formati', 'totale vinili', 'artista'
    }

    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        artista = str(row[0] or "").strip()
        if artista.lower() in SKIP:
            continue
        if len(artista) <= 5 and len(row) > 1 and str(row[1] or "").strip().isdigit():
            continue
        rows.append(row)

    total = len(rows)

    async def generate():
        imported = 0
        async with httpx.AsyncClient(timeout=20) as client:
            for idx, row in enumerate(rows):
                artista        = str(row[0] or "").strip()
                titolo         = str(row[1] or "").strip()
                formato        = str(row[2] or "").strip()
                stile          = str(row[3] or "").strip()
                anno           = str(row[4] or "").strip()
                etichetta      = str(row[5] or "").strip()
                stampa         = str(row[6] or "").strip()
                stampa_costosa = str(row[7] or "").strip() if len(row) > 7 else ""
                prezzo_max     = str(row[8] or "").strip() if len(row) > 8 else ""
                # Colonna 9 opzionale: barcode (solo per ricerca, non salvato)
                barcode_raw    = str(row[9] or "").strip() if len(row) > 9 else ""
                barcode_xl     = extract_barcode(barcode_raw) if barcode_raw else ""
                # Se catno sembra un barcode, usalo come barcode e svuota stampa
                if stampa and stampa.isdigit() and len(stampa) >= 8:
                    if not barcode_xl:
                        barcode_xl = extract_barcode(stampa)
                    stampa = ""

                # Manda progresso al frontend
                yield f"data: {json.dumps({'done': False, 'current': idx+1, 'total': total, 'artista': artista}, ensure_ascii=False)}\n\n"

                # Arricchisci solo se mancano campi (mai artista/titolo)
                needs_enrich = not all([stampa, etichetta, stile, anno, stampa_costosa, prezzo_max])

                if needs_enrich and DISCOGS_TOKEN:
                    try:
                        enriched = await cerca_su_discogs({
                            "artista": artista, "titolo": titolo,
                            "formato": formato, "stile": stile,
                            "anno": anno, "etichetta": etichetta,
                            "stampa": stampa,
                            "stampa_costosa": stampa_costosa,
                            "prezzo_max": prezzo_max,
                        }, use_cache=True, barcode=barcode_xl)
                        # Aggiorna solo campi vuoti, mai artista/titolo
                        if not formato:        formato        = enriched.get("formato", "")
                        if not stile:          stile          = enriched.get("stile", "")
                        if not anno:           anno           = enriched.get("anno", "")
                        if not etichetta:      etichetta      = enriched.get("etichetta", "")
                        if not stampa:         stampa         = enriched.get("stampa", "")
                        if not stampa_costosa: stampa_costosa = enriched.get("stampa_costosa", "")
                        if not prezzo_max:     prezzo_max     = enriched.get("prezzo_max", "")
                    except Exception as e:
                        print(f"ENRICH ERROR row {idx}: {e}")

                try:
                    await client.post(
                        f"{SUPABASE_URL}/rest/v1/vinili",
                        headers=supa_headers(token),
                        json={
                            "user_id": user_id,
                            "artista": artista, "titolo": titolo,
                            "formato": formato, "stile": stile,
                            "anno": anno, "etichetta": etichetta,
                            "stampa": stampa,
                            "stampa_costosa": stampa_costosa,
                            "prezzo_max": prezzo_max,
                        }
                    )
                    imported += 1
                except Exception as e:
                    print(f"SAVE ERROR row {idx}: {e}")

        yield f"data: {json.dumps({'done': True, 'imported': imported})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

# ── Export Excel ──────────────────────────────────────────────────────────────

@app.post("/api/export_excel")
async def export_excel_post(user_id: str = Form(...), token: str = Form(...)):
    """Endpoint POST per Android - evita problemi con token in URL."""
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

    header_fill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True, size=11)
    col_headers = ["Artista", "Titolo", "Formato", "Stile", "Anno",
                   "Etichetta", "Stampa", "Stampa piu Costosa", "Prezzo medio piu alto"]
    col_widths   = [25, 30, 12, 20, 8, 20, 15, 22, 22]

    for col, h in enumerate(col_headers, 1):
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

    formati_count = Counter()
    totale_reale  = len(vinili)

    for v in vinili:
        fmt = str(v.get("formato", "") or "").strip().lower()
        if not fmt:                                                   formati_count["altro"] += 1
        elif '7' in fmt or '45' in fmt:                              formati_count['7"'] += 1
        elif '10' in fmt:                                            formati_count['10"'] += 1
        elif '12' in fmt:                                            formati_count['12"'] += 1
        elif '2xlp' in fmt or '2x lp' in fmt or 'double' in fmt:    formati_count['2xLP'] += 1
        elif 'lp' in fmt or '33' in fmt:                             formati_count['LP'] += 1
        elif 'ep' in fmt:                                            formati_count['EP'] += 1
        else:                                                        formati_count[fmt[:10]] += 1

    last_row = ws.max_row + 2
    title_cell = ws.cell(row=last_row, column=1, value="RIEPILOGO FORMATI")
    title_cell.font = Font(bold=True, color="FFFFFF")
    title_cell.fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
    last_row += 1

    for fmt, count in sorted(formati_count.items()):
        if count == 0: continue
        c1 = ws.cell(row=last_row, column=1, value=fmt)
        c2 = ws.cell(row=last_row, column=2, value=count)
        c1.font = Font(bold=True, color="FFFFFF")
        c1.fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
        c2.font = Font(bold=True, color="FFFFFF")
        c2.fill = PatternFill(start_color="2d2d4e", end_color="2d2d4e", fill_type="solid")
        last_row += 1

    c1 = ws.cell(row=last_row, column=1, value="TOTALE VINILI")
    c2 = ws.cell(row=last_row, column=2, value=totale_reale)
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
        headers={
            "Content-Disposition": "attachment; filename=Catalogo_Vinili.xlsx",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        }
    )

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", "r") as f:
        return f.read()
