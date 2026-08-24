#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/certs/mtls}"
FORCE="${FORCE:-0}"

mkdir -p "$OUT_DIR"

write_ext() {
  local name="$1"
  local dns="$2"
  local file="$OUT_DIR/$name.ext"
  cat > "$file" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$dns
EOF
}

generate_leaf() {
  local name="$1"
  local dns="$2"
  local key="$OUT_DIR/$name.key"
  local csr="$OUT_DIR/$name.csr"
  local crt="$OUT_DIR/$name.crt"
  local ext="$OUT_DIR/$name.ext"

  if [[ "$FORCE" != "1" && -f "$key" && -f "$crt" ]]; then
    echo "skip existing $name certificate"
    return
  fi

  write_ext "$name" "$dns"
  openssl genrsa -out "$key" 2048 >/dev/null 2>&1
  openssl req -new -key "$key" -subj "/CN=$dns" -out "$csr" >/dev/null 2>&1
  openssl x509 -req -in "$csr" \
    -CA "$OUT_DIR/ca.crt" \
    -CAkey "$OUT_DIR/ca.key" \
    -CAcreateserial \
    -out "$crt" \
    -days 365 \
    -sha256 \
    -extfile "$ext" >/dev/null 2>&1
  chmod 600 "$key"
  echo "generated $crt"
}

if [[ "$FORCE" != "1" && -f "$OUT_DIR/ca.key" && -f "$OUT_DIR/ca.crt" ]]; then
  echo "skip existing ca certificate"
else
  openssl genrsa -out "$OUT_DIR/ca.key" 4096 >/dev/null 2>&1
  openssl req -x509 -new -nodes \
    -key "$OUT_DIR/ca.key" \
    -sha256 \
    -days 3650 \
    -subj "/CN=lexue-dev-ca" \
    -out "$OUT_DIR/ca.crt" >/dev/null 2>&1
  chmod 600 "$OUT_DIR/ca.key"
  echo "generated $OUT_DIR/ca.crt"
fi

generate_leaf "comment-service" "comment-service"
generate_leaf "comment-student" "comment-student"
generate_leaf "comment-tutor" "comment-tutor"
generate_leaf "comment-operator" "comment-operator"

echo "mTLS certificates are ready in $OUT_DIR"
