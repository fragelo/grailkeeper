#!/bin/bash
# Grailkeeper v4.0.18 — Quick Setup Script
# https://github.com/fragelo/grailkeeper
set -e

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║         Grailkeeper v4.0.18              ║"
echo "║   Dynatrace Grail Bulk Extraction Tool   ║"
echo "╚══════════════════════════════════════════╝"
echo ""

if ! command -v docker &>/dev/null; then
  echo "❌  Docker not found. Install Docker Desktop first: https://docs.docker.com/get-docker/"
  exit 1
fi

if [ -d "grailkeeper/.git" ]; then
  echo "📦  Updating existing repo..."
  cd grailkeeper && git pull --ff-only && cd ..
else
  echo "📦  Cloning grailkeeper..."
  git clone https://github.com/fragelo/grailkeeper.git
fi

cd grailkeeper
echo ""
echo "🐳  Building and starting Docker container..."
docker compose up --build -d

echo ""
echo "⏳  Waiting for container to start..."
sleep 4

STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/health 2>/dev/null || echo "000")
if [ "$STATUS" = "200" ]; then
  echo "✅  Grailkeeper is running!"
else
  echo "⚠️  Container started but health check returned HTTP $STATUS — may still be starting."
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Open your browser: http://localhost:8000"
echo "  Go to Settings → configure tenant URL"
echo "  and API token, then start extracting."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Useful commands:"
echo "  docker stats grailkeeper    — live memory/CPU"
echo "  docker logs grailkeeper -f  — live logs"
echo "  docker compose down         — stop container"
echo ""
