#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALLOWLIST_FILE="$ROOT_DIR/.scope-allowlist.txt"
BASELINE_FILE=""
RUN_TESTS=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-tests)
      RUN_TESTS=0
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
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

cd "$ROOT_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$ROOT_DIR/dev_artifacts/.evidence/$TS"
mkdir -p "$EVIDENCE_DIR"

if [[ -z "$BASELINE_FILE" ]]; then
  BASELINE_FILE="$EVIDENCE_DIR/scope.baseline"
  scripts/verify_scope.sh baseline --baseline "$BASELINE_FILE" >/dev/null
fi

echo "# Preflight" > "$EVIDENCE_DIR/preflight.txt"
{
  echo "\$ git status --short"
  git status --short
  echo
  echo "\$ git diff --name-only"
  git diff --name-only
  echo
  echo "\$ git check-ignore -v dev_artifacts/IMPLEMENTATION_PLAN.md dev_artifacts/TASKS.md dev_artifacts/CHECKLIST.md"
  git check-ignore -v dev_artifacts/IMPLEMENTATION_PLAN.md dev_artifacts/TASKS.md dev_artifacts/CHECKLIST.md || true
} >> "$EVIDENCE_DIR/preflight.txt"

scripts/verify_scope.sh report --allowlist "$ALLOWLIST_FILE" --baseline "$BASELINE_FILE" --json-out "$EVIDENCE_DIR/scope_report.json" > "$EVIDENCE_DIR/scope_report.txt"

python3 scripts/reconcile_tracking_docs.py --evidence-dir "$EVIDENCE_DIR" --json-out "$EVIDENCE_DIR/reconciliation_report.json" > "$EVIDENCE_DIR/reconciliation_stdout.txt"

if [[ "$RUN_TESTS" -eq 1 ]]; then
  {
    echo "\$ pytest -q"
    pytest -q
    echo
    echo "\$ pytest --cov=rigor_sf --cov-report=term"
    pytest --cov=rigor_sf --cov-report=term
  } > "$EVIDENCE_DIR/test_summary.txt" 2>&1
else
  echo "Tests skipped by --skip-tests" > "$EVIDENCE_DIR/test_summary.txt"
fi

cat > "$EVIDENCE_DIR/README.txt" <<TXT
Evidence bundle generated at: $TS (UTC)
Baseline file: $BASELINE_FILE

Files:
- preflight.txt
- scope.baseline (if generated in this run)
- scope_report.txt
- scope_report.json
- reconciliation_stdout.txt
- reconciliation_report.json
- doc_hashes.txt
- test_summary.txt
TXT

echo "Evidence bundle written: $EVIDENCE_DIR"
