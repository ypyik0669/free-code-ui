#!/bin/bash

echo ""
echo "╔══════════════════════════════════════╗"
echo "║        free-code UI  Setup           ║"
echo "╚══════════════════════════════════════╝"
echo ""

# Check node
if ! command -v node &> /dev/null; then
  echo "→ Installing Node.js..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
else
  echo "✓ Node.js $(node -v) found"
fi

# Check free-code
if ! command -v free-code &> /dev/null; then
  echo "✗ free-code not found in PATH. Make sure it is installed."
  echo "  Run: which free-code"
  exit 1
else
  echo "✓ free-code found at $(which free-code)"
fi

# Install deps
echo "→ Installing dependencies..."
npm install

echo ""
echo "✓ Setup complete!"
echo ""
echo "  Start the UI:"
echo "    npm start"
echo ""
echo "  Then open in Windows browser:"
echo "    http://localhost:3333"
echo ""
