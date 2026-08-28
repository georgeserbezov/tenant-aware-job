#!/usr/bin/env bash
#
# Single command to build and run everything: React UI + Spring Boot API.
#
# The UI is compiled into the Spring Boot app's static/ directory rather than
# served by a separate dev server. One process, one port, no CORS config, and
# the SSE stream is same-origin - which is what makes "one command" honest.
#
set -euo pipefail
cd "$(dirname "$0")"

info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
fail() { printf '\033[1;31mERROR:\033[0m %s\n' "$1" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Resolve a JDK 21+
# ---------------------------------------------------------------------------
# WHY this is not just `java -version`: on macOS, /usr/bin/java is an Apple stub
# that exists on PATH even with no JDK installed - it fails at runtime with
# "Unable to locate a Java Runtime". Homebrew's JDKs are keg-only, so they are
# installed but deliberately absent from PATH. Trusting PATH therefore gives
# both false positives and false negatives; we resolve JAVA_HOME ourselves.
resolve_java_home() {
  local candidate
  for candidate in \
      "${JAVA_HOME:-}" \
      "$(/usr/libexec/java_home -v 21 2>/dev/null || true)" \
      "$(brew --prefix openjdk@21 2>/dev/null || true)/libexec/openjdk.jdk/Contents/Home" \
      "$(brew --prefix openjdk 2>/dev/null || true)/libexec/openjdk.jdk/Contents/Home" \
      "/usr/lib/jvm/java-21-openjdk-amd64"; do
    [[ -n "$candidate" && -x "$candidate/bin/javac" ]] || continue
    local major
    major="$("$candidate/bin/java" -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
    if [[ -n "$major" && "$major" -ge 21 ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

if ! JAVA_HOME="$(resolve_java_home)"; then
  fail "No JDK 21+ found.
  Install one and re-run:
    macOS:  brew install openjdk@21
    Linux:  sudo apt install openjdk-21-jdk
  Or set JAVA_HOME to an existing JDK 21+ installation."
fi
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
info "Using JDK at $JAVA_HOME ($("$JAVA_HOME"/bin/java -version 2>&1 | head -1))"

# Prefer the checked-in Maven wrapper so a reviewer needs no local Maven.
MVN="./mvnw"
[[ -x "$MVN" ]] || MVN="mvn"
command -v "${MVN#./}" >/dev/null 2>&1 || [[ -x "$MVN" ]] \
  || fail "Neither ./mvnw nor mvn is available."

# ---------------------------------------------------------------------------
# 2. Build the React UI into the Spring static directory
# ---------------------------------------------------------------------------
STATIC_DIR="src/main/resources/static"
if [[ -d ui ]]; then
  command -v npm >/dev/null 2>&1 || fail "npm is required to build the UI (https://nodejs.org)."
  info "Building React UI"
  # `npm ci` is reproducible but requires a lockfile; fall back on first run.
  if [[ -f ui/package-lock.json ]]; then (cd ui && npm ci --silent); else (cd ui && npm install --silent); fi
  (cd ui && npm run build --silent)
  rm -rf "$STATIC_DIR"
  mkdir -p "$STATIC_DIR"
  cp -R ui/dist/. "$STATIC_DIR"/
  info "UI built into $STATIC_DIR"
else
  info "No ui/ directory yet - starting API only"
  mkdir -p "$STATIC_DIR"
fi

# ---------------------------------------------------------------------------
# 3. Run the server
# ---------------------------------------------------------------------------
info "Starting on http://localhost:8080"
exec "$MVN" -q spring-boot:run
