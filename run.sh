#!/bin/sh
set -e

(
  cd "$(dirname "$0")"
  go build -o /tmp/rego app/*.go
)

exec /tmp/rego "$@"
