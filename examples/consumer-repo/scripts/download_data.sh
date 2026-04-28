#!/usr/bin/env bash
# ==============================================================
#   Descarga los datos procesados desde el ultimo release de
#   djwillichile/chilecompra-data-processor.
#
#   Uso:
#     ./scripts/download_data.sh                # ultimo release
#     ./scripts/download_data.sh data-2026-04-28  # release especifico
#
#   Requiere:
#     - gh CLI instalado y autenticado (gh auth login)
# ==============================================================
set -euo pipefail

REPO="djwillichile/chilecompra-data-processor"
DEST="data/processed"
TAG="${1:-}"

if ! command -v gh >/dev/null 2>&1; then
  echo "Error: gh CLI no esta instalado. Ver https://cli.github.com/" >&2
  exit 1
fi

mkdir -p "$DEST"

if [ -z "$TAG" ]; then
  echo "Descargando ultimo release de $REPO..."
  gh release download --repo "$REPO" \
    --pattern '*.parquet' --pattern '*.json' \
    --dir "$DEST" --clobber
else
  echo "Descargando release $TAG de $REPO..."
  gh release download "$TAG" --repo "$REPO" \
    --pattern '*.parquet' --pattern '*.json' \
    --dir "$DEST" --clobber
fi

echo ""
echo "Datos descargados en $DEST/:"
ls -lh "$DEST" | tail -n +2
