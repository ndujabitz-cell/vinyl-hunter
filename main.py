import asyncio
import base64
import json
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
# ... (mantieni gli altri import del tuo file originale)

# --- CONFIGURAZIONE OTTIMIZZATA ---
# Usa la versione v1 per maggiore stabilità se v1beta dà problemi di quota
GEMINI_MODEL = "gemini-1.5-flash" 
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

@app.post("/api/scan")
async def scan_label(file: UploadFile = File(...)):
    # Leggiamo il contenuto del file
    content = await file.read()
    
    # Inizializziamo il dizionario di default
    gemini_data = {
        "artista": "", "titolo": "", "formato": "", "stile": "",
        "anno": "", "etichetta": "", "stampa": "", "barcode": "", "lato": ""
    }

    if not GEMINI_KEY:
        raise HTTPException(status_code=500, detail="Gemini API Key non configurata nell'ambiente.")

    b64 = base64.b64encode(content).decode()
    mime = file.content_type or "image/jpeg"

    # Prompt ottimizzato per evitare errori di parsing JSON
    prompt = """Analizza l'etichetta del vinile e restituisci SOLO un oggetto JSON.
Regole: 
- "formato": scegli tra '7"', '12"', 'LP', 'EP'.
- "lato": 'A' o 'B'.
- "stampa": numero di catalogo.
- Se un dato manca, usa stringa vuota.
Output richiesto: {"artista":"","titolo":"","formato":"","stile":"","anno":"","etichetta":"","stampa":"","barcode":"","lato":""}"""

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": mime, "data": b64}}
            ]
        }],
        "generationConfig": {
            "response_mime_type": "application/json", # Chiediamo esplicitamente JSON (se supportato dal modello)
            "temperature": 0.1 # Più basso è, meno "inventa" e più è preciso
        }
    }

    async with httpx.AsyncClient(timeout=45) as client:
        max_retries = 2
        for attempt in range(max_retries):
            try:
                r = await client.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload)
                
                if r.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = 5 * (attempt + 1)
                        print(f"Quota superata (429). Attendo {wait_time}s e riprovo...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return {"_error": "quota_exceeded", "message": "Limite API raggiunto. Riprova tra un minuto."}

                if r.status_code != 200:
                    print(f"Errore API Gemini {r.status_code}: {r.text}")
                    return {"_error": f"api_error_{r.status_code}"}

                # Parsing della risposta
                rj = r.json()
                text_response = rj['candidates'][0]['content']['parts'][0]['text']
                
                # Pulizia stringa da eventuali rimasugli markdown
                clean_json = text_response.strip().replace("```json", "").replace("```", "")
                parsed = json.loads(clean_json)
                
                # Uniamo i dati ottenuti a quelli di default
                gemini_data.update(parsed)
                break # Successo, usciamo dal loop di retry

            except Exception as e:
                print(f"Eccezione durante chiamata Gemini: {e}")
                if attempt == max_retries - 1:
                    return {"_error": "internal_exception"}

    # --- LOGICA POST-SCAN (MANTIENI LA TUA) ---
    barcode_scan = extract_barcode(str(gemini_data.pop("barcode", "") or ""))
    lato_scan = str(gemini_data.pop("lato", "") or "").strip().upper()

    # Logica per gestire i lati e la ricerca su Discogs
    if lato_scan == "B" and not gemini_data.get("artista"):
        gemini_data["_titolo_lato_b"] = gemini_data.get("titolo", "")
        gemini_data["titolo"] = ""

    # Chiamata a Discogs (usando la tua funzione esistente)
    result = await cerca_su_discogs(gemini_data, use_cache=True, barcode=barcode_scan)

    if lato_scan == "B" and not result.get("artista") and gemini_data.get("_titolo_lato_b"):
        result["titolo"] = gemini_data["_titolo_lato_b"]

    result["catno"] = result.get("stampa", "")
    return result
