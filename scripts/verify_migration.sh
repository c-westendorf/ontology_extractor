#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="$ROOT_DIR/artifacts"
LOG_DIR="$ARTIFACT_DIR/logs"
RESULTS_TSV="$ARTIFACT_DIR/migration_verification.tsv"
JSON_OUT="$ARTIFACT_DIR/migration_verification.json"
MD_OUT="$ARTIFACT_DIR/migration_verification.md"
VERIFY_VENV="$ROOT_DIR/.venv.verify"
PROJECT_VENV="$ROOT_DIR/.venv"
PROJECT_PY="$PROJECT_VENV/bin/python"
PROJECT_PIP="$PROJECT_VENV/bin/pip"
PROJECT_RIGOR="$PROJECT_VENV/bin/rigor"
PYTEST_BIN="${PYTEST_BIN:-pytest}"

mkdir -p "$LOG_DIR"
: > "$RESULTS_TSV"

run_check() {
  local check_id="$1"
  local category="$2"
  local description="$3"
  local expected_exit="$4"
  local cmd="$5"
  local log_file="$LOG_DIR/${check_id}.log"

  echo "[CHECK] $check_id - $description"
  set +e
  bash -lc "$cmd" >"$log_file" 2>&1
  local actual_exit=$?
  set -e

  local status="FAIL"
  if [[ "$actual_exit" == "$expected_exit" ]]; then
    status="PASS"
  fi

  printf "%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$check_id" "$category" "$description" "$status" "$expected_exit" "$actual_exit" >> "$RESULTS_TSV"
}

# A. Environment and packaging sanity
run_check "A1" "env" "Create fresh verification virtualenv" "0" "rm -rf '$VERIFY_VENV' && python3 -m venv --system-site-packages '$VERIFY_VENV'"
run_check "A2" "env" "Install package in project venv (.venv)" "0" "'$PROJECT_PIP' install -e '$ROOT_DIR' --no-build-isolation"
run_check "A3" "env" "Import rigor_sf" "0" "'$PROJECT_PY' -c 'import rigor_sf'"
run_check "A4" "env" "Import rigor_sf.pipeline" "0" "'$PROJECT_PY' -c 'import rigor_sf.pipeline'"
run_check "A5" "env" "rigor --help works" "0" "'$PROJECT_RIGOR' --help"
run_check "A6" "env" "python -m rigor_sf.pipeline --help works" "0" "'$PROJECT_PY' -m rigor_sf.pipeline --help"
run_check "A7" "env" "Streamlit target path exists" "0" "test -f '$ROOT_DIR/rigor_sf/ui/app.py'"

# C. Verification checklist items
run_check "C1" "checklist" "Query-gen exits with code 0" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_pipeline_phases.py::TestPhaseExecution::test_query_gen_creates_run_directory' -q"
run_check "C2" "checklist" "Generate exits with code 2 when prerequisites missing" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_pipeline_phases.py::TestPhasePrerequisites::test_generate_fails_without_relationships_csv' -q"
run_check "C3" "checklist" "Validate exits with code 3 on validation failure" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_pipeline_phases.py::TestExitCodes::test_validation_error_returns_code_3' -q"
run_check "C4" "checklist" "core.owl symlink behavior validated" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/test_versioning.py::TestCreateVersionedArtifact::test_creates_symlink' -q"
run_check "C5" "checklist" "Auto-approved edges appear in relationships CSV" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/test_auto_approve.py::TestAutoApproveLogic::test_auto_approve_high_confidence_edges' -q"
run_check "C6" "checklist" "Incremental run skips unchanged tables" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_incremental.py::TestIncrementalGeneration::test_unchanged_tables_skipped' -q"
run_check "C7" "checklist" "--force-regenerate regenerates specified table" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_incremental.py::TestIncrementalGeneration::test_force_regenerate_bypasses_cache' -q"
run_check "C8" "checklist" "--non-interactive auto-skips on LLM failure" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/integration/test_error_recovery.py::TestNonInteractiveMode::test_non_interactive_auto_skips_on_failure' -q"
run_check "C9" "checklist" "validation_report schema is JSON-serializable" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests/test_sparql_validation.py::TestBuildValidationReport::test_json_serializable' -q"

# B + C10. Full suite and coverage
run_check "B1" "tests" "Full test suite passes" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests' -v"
run_check "B2" "tests" "Coverage run passes threshold" "0" "'$PYTEST_BIN' '$ROOT_DIR/rigor_sf/tests' --cov=rigor_sf --cov-report=term-missing"
run_check "C10" "checklist" "Unit and integration tests pass with coverage target" "0" "awk -F '\\t' '\$1==\"B1\"{b1=\$4} \$1==\"B2\"{b2=\$4} END{exit !(b1==\"PASS\" && b2==\"PASS\")}' '$RESULTS_TSV'"

ROOT_ENV="$ROOT_DIR" RESULTS_FILE="$RESULTS_TSV" LOGS_DIR="$LOG_DIR" JSON_FILE="$JSON_OUT" MD_FILE="$MD_OUT" \
python3 - <<'PY'
import csv
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_ENV"])
results_file = Path(os.environ["RESULTS_FILE"])
logs_dir = Path(os.environ["LOGS_DIR"])
json_out = Path(os.environ["JSON_FILE"])
md_out = Path(os.environ["MD_FILE"])

rows = []
with results_file.open("r", encoding="utf-8") as f:
    reader = csv.reader(f, delimiter="\t")
    for r in reader:
        if len(r) != 6:
            continue
        check_id, category, description, status, expected, actual = r
        log_file = logs_dir / f"{check_id}.log"
        evidence = ""
        if log_file.exists():
            text = log_file.read_text(encoding="utf-8", errors="replace")
            evidence = "\n".join(text.splitlines()[:25])
        rows.append(
            {
                "check_id": check_id,
                "category": category,
                "description": description,
                "status": status,
                "expected_exit": int(expected),
                "actual_exit": int(actual),
                "log_path": str(log_file.relative_to(root)),
                "evidence_preview": evidence,
            }
        )

pass_count = sum(1 for r in rows if r["status"] == "PASS")
fail_count = sum(1 for r in rows if r["status"] == "FAIL")

payload = {
    "summary": {
        "total": len(rows),
        "passed": pass_count,
        "failed": fail_count,
    },
    "checks": rows,
}
json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

lines = [
    "# Migration Verification Report",
    "",
    f"- Total checks: {len(rows)}",
    f"- Passed: {pass_count}",
    f"- Failed: {fail_count}",
    "",
    "## Results",
    "",
    "| ID | Category | Status | Expected | Actual | Description |",
    "|---|---|---|---:|---:|---|",
]
for r in rows:
    lines.append(
        f"| {r['check_id']} | {r['category']} | {r['status']} | {r['expected_exit']} | {r['actual_exit']} | {r['description']} |"
    )

if fail_count:
    lines.extend(["", "## Failed Checks", ""])
    for r in rows:
        if r["status"] == "FAIL":
            lines.append(f"### {r['check_id']} - {r['description']}")
            lines.append(f"Log: `{r['log_path']}`")
            lines.append("")
            if r["evidence_preview"]:
                lines.append("```text")
                lines.append(r["evidence_preview"])
                lines.append("```")
            lines.append("")

md_out.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

if rg -q $'\tFAIL\t' "$RESULTS_TSV"; then
  echo "Verification completed with failures. See $MD_OUT and $JSON_OUT"
  exit 1
fi

echo "Verification passed. Reports written to:"
echo "- $JSON_OUT"
echo "- $MD_OUT"
