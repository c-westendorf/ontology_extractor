#!/usr/bin/env python3
"""Reconcile ignored tracking docs and emit local evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
DOCS = {
    "implementation_plan": ROOT / "dev_artifacts" / "IMPLEMENTATION_PLAN.md",
    "tasks": ROOT / "dev_artifacts" / "TASKS.md",
    "checklist": ROOT / "dev_artifacts" / "CHECKLIST.md",
}

# This routine focuses on in-scope parity checks from the current workflow.
COMPLETED_PATTERNS = {
    "implementation_plan": [
        r"\[x\] Delete or deprecate cursor_cli\.py",
        r"\[x\] Add auto-approved badge to relationships tab",
        r"\[x\] Improve table classification suggestions",
        r"\[x\] Add keyboard shortcuts",
        r"\[x\] Add progress indicators",
        r"\[x\] All unit tests pass with coverage targets met",
    ],
    "tasks": [
        r"\| LLM-09 \| .* \| `\[x\]` \|",
        r"\| UI-01 \| .* \| `\[x\]` \|",
        r"\| UI-02 \| .* \| `\[x\]` \|",
        r"\| UI-03 \| .* \| `\[x\]` \|",
        r"\| UI-04 \| .* \| `\[x\]` \|",
        r"\| UI-05 \| .* \| `\[x\]` \|",
        r"\| OPS-01 \| .* \| `\[x\]` \|",
        r"\| OPS-02 \| .* \| `\[x\]` \|",
        r"\| OPS-03 \| .* \| `\[x\]` \|",
    ],
    "checklist": [
        r"\| Auto-approved badge display \| `\[x\]` \| `\[x\]` \|",
        r"\| Secrets management \(env vars\) \| `\[x\]` \|",
        r"\| Metrics/instrumentation \| `\[x\]` \|",
        r"\| CI/CD pipeline \| `\[x\]` \|",
    ],
}

OUT_OF_SCOPE_PATTERNS = {
    "implementation_plan": [
        r"\[~\] SQL precision/recall scorer gate \(SQL-13\) remains open",
        r"\[~\] SQL hard-cutover runtime validation \(SQL-14\) remains open",
    ],
    "tasks": [
        r"\| SQL-13 \| .* \| `\[ \]` \| Out of scope this pass",
        r"\| SQL-14 \| .* \| `\[~\]` \| Out of scope this pass",
    ],
    "checklist": [
        r"\| Precision on variance corpus \| `>=95%` \| `\[ \]` \| Out of scope",
        r"\| Recall on variance corpus \| `>=90%` \| `\[ \]` \| Out of scope",
    ],
}


@dataclass
class CheckResult:
    doc: str
    category: str
    pattern: str
    passed: bool


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_pattern_checks(text: str, doc_name: str, checks: dict[str, list[str]], category: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    for pattern in checks.get(doc_name, []):
        results.append(
            CheckResult(
                doc=doc_name,
                category=category,
                pattern=pattern,
                passed=bool(re.search(pattern, text, flags=re.MULTILINE)),
            )
        )
    return results


def to_dict(results: Iterable[CheckResult]) -> list[dict[str, object]]:
    return [
        {
            "doc": r.doc,
            "category": r.category,
            "pattern": r.pattern,
            "passed": r.passed,
        }
        for r in results
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile ignored tracking docs and emit evidence")
    parser.add_argument("--evidence-dir", default="", help="Evidence output directory")
    parser.add_argument("--json-out", default="", help="Optional explicit JSON output path")
    args = parser.parse_args()

    missing = [name for name, path in DOCS.items() if not path.exists()]
    if missing:
        raise SystemExit(f"Missing tracking docs: {', '.join(missing)}")

    texts = {name: load_text(path) for name, path in DOCS.items()}
    hashes = {name: sha256(path) for name, path in DOCS.items()}

    checks: list[CheckResult] = []
    for name, text in texts.items():
        checks.extend(run_pattern_checks(text, name, COMPLETED_PATTERNS, "completed"))
        checks.extend(run_pattern_checks(text, name, OUT_OF_SCOPE_PATTERNS, "out_of_scope"))

    passed = all(c.passed for c in checks)
    failed = [c for c in checks if not c.passed]

    payload = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "passed": passed,
        "docs": {name: str(path.relative_to(ROOT)) for name, path in DOCS.items()},
        "hashes": hashes,
        "checks": to_dict(checks),
        "failed_count": len(failed),
    }

    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else None
    if evidence_dir:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        (evidence_dir / "reconciliation_report.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        lines = []
        for name, path in DOCS.items():
            mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).isoformat()
            lines.append(f"{name}\t{path}\t{hashes[name]}\t{mtime}")
        (evidence_dir / "doc_hashes.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps({"passed": passed, "failed_count": len(failed)}, indent=2))
    if not passed:
        for item in failed:
            print(f"FAIL [{item.doc}/{item.category}] pattern not found: {item.pattern}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
