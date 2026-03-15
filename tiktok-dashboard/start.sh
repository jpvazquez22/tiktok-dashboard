#!/bin/bash
# ── TikTok Dashboard Startup Script ──────────────────────────────────────────
cd "$(dirname "$0")"

echo ""
echo "  📊 TikTok Analytics Dashboard"
echo "  ─────────────────────────────"

echo "  Checking dependencies..."
pip install -r requirements.txt -q 2>/dev/null
python3 -c "import nltk; nltk.download('vader_lexicon', quiet=True)"

echo ""
echo "  🚀 Starting dashboard → http://localhost:5050"
echo "  Press Ctrl+C to stop."
echo ""
python3 app.py
