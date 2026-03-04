# 🎵 Vinyl Hunter — Guida al Deploy

## File del progetto
```
vinili/
├── main.py              ← Backend FastAPI
├── templates/
│   └── index.html       ← Web App (frontend)
├── requirements.txt     ← Dipendenze Python
├── Procfile             ← Comando avvio Railway
├── supabase_setup.sql   ← Script database
└── README.md
```

---

## STEP 1 — Configura Supabase

1. Vai su supabase.com → il tuo progetto `catalogo-vinili`
2. Menu a sinistra → **SQL Editor** → **New query**
3. Copia e incolla tutto il contenuto di `supabase_setup.sql`
4. Clicca **Run** (freccia verde)

---

## STEP 2 — Carica su GitHub

1. Crea account su github.com (se non ce l'hai)
2. Clicca **"New repository"** → nome: `vinyl-hunter` → Public → Create
3. Carica tutti i file del progetto (tranne `supabase_setup.sql`)

---

## STEP 3 — Deploy su Railway

1. Vai su railway.app → **New Project** → **Deploy from GitHub repo**
2. Seleziona `vinyl-hunter`
3. Una volta creato, vai su **Variables** e aggiungi:

```
SUPABASE_URL    = https://lbvzzstvqlptxpjujwfb.supabase.co
SUPABASE_ANON   = (la tua publishable key)
SUPABASE_SECRET = (la tua secret key)
GEMINI_KEY      = (la tua chiave AIza...)
```

4. Vai su **Settings → Networking → Generate Domain**
5. Copia il tuo URL tipo `vinyl-hunter.up.railway.app`

---

## STEP 4 — Usa l'app!

Apri l'URL dal telefono, registrati e inizia a catalogare! 🎵
