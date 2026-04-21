#!/bin/bash
# Rebuild Digital Co — Server Launcher
# Double-click this file in Finder to start the local API server + tracker app.

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  🔨 Rebuild Digital Co — Local API Server"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check if already running
if lsof -i :8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
  echo "  ✅ Server is already running."
  open "https://localhost:8000"
  read -p "  Press Enter to close..."
  exit 0
fi

# Find Python 3
PYTHON=""
for py in python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 "$HOME/.pyenv/shims/python3"; do
  if command -v "$py" >/dev/null 2>&1; then
    PYTHON="$py"
    break
  fi
done

if [ -z "$PYTHON" ]; then
  echo "  ❌ Python 3 not found. Install it from https://python.org"
  read -p "  Press Enter to close..."
  exit 1
fi

echo "  Python:  $PYTHON"

# Install Python packages
echo "  Checking Python dependencies..."
"$PYTHON" -m pip install --quiet --upgrade fastapi uvicorn python-dotenv supabase playwright aiofiles lxml beautifulsoup4 requests 2>&1 \
  | grep -v "already satisfied" | grep -v "^$" | grep -v "WARNING" | sed 's/^/  /'

# Install Playwright browser (show output so we know if it's downloading)
echo "  Checking Playwright browser..."
"$PYTHON" -m playwright install chromium 2>&1 | grep -v "^$" | grep -E "(Downloading|Installing|chromium|already)" | sed 's/^/  /' || true

# ── HTTPS cert setup ──────────────────────────────────────────
if [ ! -f "$DIR/localhost.pem" ]; then
  echo ""
  echo "  Setting up HTTPS (one-time)..."
  MKCERT="$DIR/mkcert"
  if [ ! -f "$MKCERT" ]; then
    ARCH=$(uname -m)
    if [ "$ARCH" = "arm64" ]; then
      MKCERT_URL="https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-darwin-arm64"
    else
      MKCERT_URL="https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-darwin-amd64"
    fi
    curl -fsSL "$MKCERT_URL" -o "$MKCERT" && chmod +x "$MKCERT"
  fi
  if [ -f "$MKCERT" ]; then
    "$MKCERT" -install
    "$MKCERT" -key-file "$DIR/localhost-key.pem" -cert-file "$DIR/localhost.pem" localhost 127.0.0.1
    echo "  ✅ HTTPS cert ready"
  fi
  echo ""
fi

# ── Build tracker app if missing or outdated ──────────────────
DIST="$DIR/tracker-app/dist"
ENV_FILE="$DIR/tracker-app/.env"
DIST_INDEX="$DIST/index.html"
NEEDS_BUILD=false

if [ ! -d "$DIST" ]; then
  NEEDS_BUILD=true
elif [ -f "$ENV_FILE" ] && [ "$ENV_FILE" -nt "$DIST_INDEX" ]; then
  echo "  .env changed — rebuilding tracker app..."
  NEEDS_BUILD=true
fi

if [ "$NEEDS_BUILD" = true ]; then
  echo "  Building tracker app..."
  if command -v npm >/dev/null 2>&1; then
    cd "$DIR/tracker-app" && npm install --silent && npm run build 2>&1 | grep -v "^$" | tail -5 | sed 's/^/  /'
    cd "$DIR"
    echo "  ✅ Tracker app built"
  else
    echo "  ⚠  npm not found — install Node.js from https://nodejs.org"
  fi
  echo ""
fi

PROTOCOL="http"
[ -f "$DIR/localhost.pem" ] && PROTOCOL="https"

echo "  Folder:  $DIR"
echo "  URL:     $PROTOCOL://localhost:8000"
echo ""
echo "  ✅ Starting... Opening browser in 3 seconds."
echo "  Press Ctrl+C to stop."
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Open browser after short delay
(sleep 3 && open "$PROTOCOL://localhost:8000") &

"$PYTHON" server.py
