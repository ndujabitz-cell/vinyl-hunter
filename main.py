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
from contextlib import asynccontextmanager

# Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON = os.getenv("SUPABASE_ANON")
SUPABASE_SECRET = os.getenv("SUPABASE_SECRET")
GEMINI_KEY = os.getenv("GEMINI_KEY")
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")

DISCOGS_HEADERS = lambda: {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "VinylHunter/2.0"
}

app = FastAPI(title="VinylHunter API", version="2.0")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 VinylHunter avviato!")
    yield
    # Shutdown
    print("👋 VinylHunter spento")

app.router.lifespan_context = lifespan

# ── MODELS ─────────────────────────────────────────────────────────────────────
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

# ── HELPERS ────────────────────────────────────────────────────────────────────
def supa_headers(token: str = None, use_secret: bool = False):
    key = SUPABASE_SECRET if use_secret else SUPABASE_ANON
    h = {"apikey": key, "Content-Type": "application/json"}
    h["Authorization"] = f"Bearer {token}" if token else f"Bearer {key}"
    return h

def cache_key(artista: str, titolo: str) -> str:
    return f"{(artista or '').strip().lower()}|{(titolo or '').strip().lower()}"

def fix_catno_ocr(s: str) -> str:
    if not s: return s
    chars = list(s)
    for i, c in enumerate(chars):
        if c in ('O', 'o'):
            pd = i > 0 and chars[i-1].isdigit()
            nd = i < len(chars)-1 and chars[i+1].isdigit()
            nb = any(chars[j].isdigit() for j in range(max(0,i-3), min(len(chars),i+4)))
            sp = any(chars[j]==' ' for j in range(max(0,i-2), min(len(chars),i+3)))
            if pd or nd or (nb and not sp): chars[i] = '0'
        elif c in ('I', 'l'):
            pd = i > 0 and chars[i-1].isdigit()
            nd = i < len(chars)-1 and chars[i+1].isdigit()
            if pd or nd: chars[i] = '1'
    return ''.join(chars)

# ── DISCOGS HELPERS ────────────────────────────────────────────────────────────
async def cache_get(key: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={**supa_headers(use_secret=True), "Accept": "application/json"},
                params={"cache_key": f"eq.{key}", "limit": "1"}
            )
            if r.status_code == 200 and (rows := r.json()):
                print(f"✅ CACHE HIT: {key}")
                return rows[0]
    except Exception as e:
        print(f"❌ CACHE GET ERROR: {e}")
    return None

async def cache_set(key: str, data: dict):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/discogs_cache",
                headers={**supa_headers(use_secret=True), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={**{"cache_key": key}, **{k: data.get(k, "") for k in 
                     ["artista", "titolo", "formato", "stile", "anno", "etichetta", "stampa", 
                      "stampa_costosa", "prezzo_max"]}}
            )
            print(f"💾 CACHE SET: {key}")
    except Exception as e:
        print(f"❌ CACHE SET ERROR: {e}")

def formato_to_discogs(fmt: str) -> str:
    f = fmt.lower().strip()
    if "
