#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALLOWLIST_FILE="$ROOT_DIR/.scope-allowlist.txt"
BASELINE_FILE=""
MODE="report"
JSON_OUT=""

usage() {
  cat <<USAGE
Usage: scripts/verify_scope.sh [mode] [--allowlist PATH] [--baseline PATH] [--json-out PATH]

Modes:
  baseline     Capture current changed tracked files as baseline
  report       Print allowlist, changed tracked files, and mismatch report (default)
  check        Exit nonzero if any changed tracked file is outside allowlist
  check-staged Exit nonzero if any staged tracked file is outside allowlist
  precommit    Enforce staged subset + no unstaged out-of-scope tracked edits

Examples:
  scripts/verify_scope.sh report
  scripts/verify_scope.sh baseline --baseline artifacts/scope.baseline
  scripts/verify_scope.sh check --allowlist .scope-allowlist.txt --baseline artifacts/scope.baseline
  scripts/verify_scope.sh precommit --json-out artifacts/scope_report.json
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    baseline|report|check|check-staged|precommit)
      MODE="$1"
      shift
      ;;
    --allowlist)
      ALLOWLIST_FILE="$2"
      shift 2
      ;;
    --baseline)
      BASELINE_FILE="$2"
      shift 2
      ;;
    --json-out)
      JSON_OUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

cd "$ROOT_DIR"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

strip_file() {
  local in_file="$1"
  sed -e 's/#.*$//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$in_file" | sed '/^$/d'
}

collect_changed() {
  local mode="$1"
  if [[ "$mode" == "staged" ]]; then
    git diff --name-only --cached | awk 'NF' | sort -u
  elif [[ "$mode" == "unstaged" ]]; then
    git diff --name-only | awk 'NF' | sort -u
  else
    {
      git diff --name-only
      git diff --name-only --cached
    } | awk 'NF' | sort -u
  fi
}

if [[ "$MODE" == "baseline" ]]; then
  if [[ -z "$BASELINE_FILE" ]]; then
    echo "baseline mode requires --baseline PATH" >&2
    exit 2
  fi
  mkdir -p "$(dirname "$BASELINE_FILE")"
  collect_changed all > "$BASELINE_FILE"
  echo "Baseline captured: $BASELINE_FILE"
  exit 0
fi

if [[ ! -f "$ALLOWLIST_FILE" ]]; then
  echo "Allowlist file not found: $ALLOWLIST_FILE" >&2
  exit 2
fi

strip_file "$ALLOWLIST_FILE" > "$TMP_DIR/allowlist"
if [[ ! -s "$TMP_DIR/allowlist" ]]; then
  echo "Allowlist is empty: $ALLOWLIST_FILE" >&2
  exit 2
fi

collect_changed all > "$TMP_DIR/changed_all_raw"
collect_changed staged > "$TMP_DIR/changed_staged_raw"
collect_changed unstaged > "$TMP_DIR/changed_unstaged_raw"

if [[ -n "$BASELINE_FILE" && -f "$BASELINE_FILE" ]]; then
  strip_file "$BASELINE_FILE" | sort -u > "$TMP_DIR/baseline"
else
  : > "$TMP_DIR/baseline"
fi

comm -23 "$TMP_DIR/changed_all_raw" "$TMP_DIR/baseline" > "$TMP_DIR/changed_all"
comm -23 "$TMP_DIR/changed_staged_raw" "$TMP_DIR/baseline" > "$TMP_DIR/changed_staged"
comm -23 "$TMP_DIR/changed_unstaged_raw" "$TMP_DIR/baseline" > "$TMP_DIR/changed_unstaged"

is_allowed() {
  local path="$1"
  local rule
  while IFS= read -r rule || [[ -n "$rule" ]]; do
    [[ -z "$rule" ]] && continue
    if [[ "$rule" == */ ]]; then
      [[ "$path" == "$rule"* ]] && return 0
    else
      [[ "$path" == "$rule" || "$path" == "$rule"/* ]] && return 0
    fi
  done < "$TMP_DIR/allowlist"
  return 1
}

build_mismatch() {
  local input_file="$1"
  local output_file="$2"
  : > "$output_file"
  local f
  while IFS= read -r f || [[ -n "$f" ]]; do
    [[ -z "$f" ]] && continue
    if ! is_allowed "$f"; then
      echo "$f" >> "$output_file"
    fi
  done < "$input_file"
}

build_mismatch "$TMP_DIR/changed_all" "$TMP_DIR/mismatch_all"
build_mismatch "$TMP_DIR/changed_staged" "$TMP_DIR/mismatch_staged"
build_mismatch "$TMP_DIR/changed_unstaged" "$TMP_DIR/mismatch_unstaged"

print_list() {
  local title="$1"
  local file="$2"
  echo "$title"
  if [[ ! -s "$file" ]]; then
    echo "  (none)"
    return
  fi
  while IFS= read -r item || [[ -n "$item" ]]; do
    [[ -z "$item" ]] && continue
    echo "  - $item"
  done < "$file"
}

if [[ "$MODE" == "report" || "$MODE" == "precommit" ]]; then
  print_list "Allowlist rules:" "$TMP_DIR/allowlist"
  print_list "Changed tracked files (delta from baseline):" "$TMP_DIR/changed_all"
  print_list "Out-of-scope tracked files:" "$TMP_DIR/mismatch_all"
fi

count_lines() {
  local file="$1"
  if [[ -s "$file" ]]; then
    wc -l < "$file" | tr -d ' '
  else
    echo 0
  fi
}

if [[ -n "$JSON_OUT" ]]; then
  mkdir -p "$(dirname "$JSON_OUT")"
  python3 - "$TMP_DIR" "$MODE" "$ALLOWLIST_FILE" "$BASELINE_FILE" <<'PY' > "$JSON_OUT"
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
mode = sys.argv[2]
allowlist = sys.argv[3]
baseline = sys.argv[4]

def lines(name):
    f = p / name
    if not f.exists():
        return []
    return [x.strip() for x in f.read_text(encoding="utf-8").splitlines() if x.strip()]

payload = {
    "mode": mode,
    "allowlist_file": allowlist,
    "baseline_file": baseline,
    "allowlist": lines("allowlist"),
    "changed_all": lines("changed_all"),
    "mismatch_all": lines("mismatch_all"),
    "mismatch_staged": lines("mismatch_staged"),
    "mismatch_unstaged": lines("mismatch_unstaged"),
}
print(json.dumps(payload, indent=2))
PY
fi

MISMATCH_ALL_COUNT="$(count_lines "$TMP_DIR/mismatch_all")"
MISMATCH_STAGED_COUNT="$(count_lines "$TMP_DIR/mismatch_staged")"
MISMATCH_UNSTAGED_COUNT="$(count_lines "$TMP_DIR/mismatch_unstaged")"

case "$MODE" in
  check)
    [[ "$MISMATCH_ALL_COUNT" -eq 0 ]] || exit 1
    ;;
  check-staged)
    [[ "$MISMATCH_STAGED_COUNT" -eq 0 ]] || exit 1
    ;;
  precommit)
    [[ "$MISMATCH_STAGED_COUNT" -eq 0 ]] || {
      echo "Precommit gate failed: staged out-of-scope files detected." >&2
      exit 1
    }
    [[ "$MISMATCH_UNSTAGED_COUNT" -eq 0 ]] || {
      echo "Precommit gate failed: unstaged out-of-scope tracked edits detected." >&2
      exit 1
    }
    ;;
  report)
    ;;
esac
