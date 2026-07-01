#!/usr/bin/env bash
# Shop E2E task processor — deterministic file transform and metadata extraction
#
# Reads INPUT_URL env var (or first argument) to locate the file,
# computes SHA-256 digest, file size, line/byte count, and writes
# JSON result to stdout.
#
# Usage: bash task-processor.sh [file-url-or-path]

set -euo pipefail

INPUT="${1:-${INPUT_URL:-}}"
if [ -z "$INPUT" ]; then
  echo '{"error":"no input provided","status":"failed"}'
  exit 1
fi

# If input is a URL, download it to a temp file
TMPFILE=$(mktemp /tmp/shop-task-XXXXXX)
trap 'rm -f "$TMPFILE"' EXIT

if [[ "$INPUT" =~ ^https?:// ]]; then
  if command -v curl &>/dev/null; then
    curl -sSL -o "$TMPFILE" "$INPUT"
  elif command -v wget &>/dev/null; then
    wget -q -O "$TMPFILE" "$INPUT"
  else
    echo '{"error":"no download tool available","status":"failed"}'
    exit 1
  fi
  WORKFILE="$TMPFILE"
elif [ -f "$INPUT" ]; then
  WORKFILE="$INPUT"
else
  echo "{\"error\":\"file not found: $INPUT\",\"status\":\"failed\"}"
  exit 1
fi

# Compute file properties
SHA256=$(sha256sum "$WORKFILE" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$WORKFILE" 2>/dev/null | awk '{print $1}' || echo "unknown")
FILESIZE=$(stat -c%s "$WORKFILE" 2>/dev/null || stat -f%z "$WORKFILE" 2>/dev/null || echo "0")
LINE_COUNT=$(wc -l < "$WORKFILE" 2>/dev/null || echo "0")
BYTE_COUNT=$(wc -c < "$WORKFILE" 2>/dev/null || echo "0")
WORD_COUNT=$(wc -w < "$WORKFILE" 2>/dev/null || echo "0")

# Detect if the file is text or binary
IS_TEXT=1
if file "$WORKFILE" 2>/dev/null | grep -qi "text"; then
  IS_TEXT=1
  FIRST_LINE=$(head -n 1 "$WORKFILE" 2>/dev/null | tr -d '\n' | cut -c1-80 || echo "")
else
  IS_TEXT=0
  FIRST_LINE=""
fi

# Output deterministic JSON result
cat <<EOF
{
  "status": "completed",
  "input": "$INPUT",
  "sha256": "$SHA256",
  "file_size_bytes": $FILESIZE,
  "line_count": $LINE_COUNT,
  "byte_count": $BYTE_COUNT,
  "word_count": $WORD_COUNT,
  "is_text": $IS_TEXT,
  "first_line": "$FIRST_LINE",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
