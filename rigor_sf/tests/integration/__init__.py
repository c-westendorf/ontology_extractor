"""Integration tests for the RIGOR-SF pipeline.

These tests verify:
- Phase isolation (each phase has correct prerequisites)
- Incremental generation (cache behavior)
- Error recovery (retry and skip logic)
- Override precedence (human > auto > proposed)
- End-to-end workflow
"""
