# Scope-Safe Delivery Workflow

This workflow enforces path-scoped tracked-file changes while keeping `dev_artifacts/*` ignored.

## 1) Configure allowlist

Edit `.scope-allowlist.txt` with repo-relative paths/prefixes for the current task.

## 2) Capture baseline before work

```bash
scripts/verify_scope.sh baseline --baseline artifacts/scope.baseline
```

## 3) Run periodic scope checks

```bash
scripts/verify_scope.sh report --allowlist .scope-allowlist.txt --baseline artifacts/scope.baseline
scripts/verify_scope.sh check --allowlist .scope-allowlist.txt --baseline artifacts/scope.baseline
scripts/verify_scope.sh precommit --allowlist .scope-allowlist.txt --baseline artifacts/scope.baseline
```

## 4) Reconcile ignored tracking docs

```bash
python3 scripts/reconcile_tracking_docs.py --json-out /tmp/reconcile.json
```

## 5) Generate local evidence bundle

```bash
scripts/generate_local_evidence.sh --allowlist .scope-allowlist.txt --baseline artifacts/scope.baseline
```

The evidence bundle is written under `dev_artifacts/.evidence/<timestamp>/` and includes:
- `preflight.txt`
- `scope_report.txt`
- `scope_report.json`
- `reconciliation_report.json`
- `doc_hashes.txt`
- `test_summary.txt`
