from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

try:
    from ..config import load_config
    from ..overrides import OverrideEdge, load_overrides, save_overrides, upsert_edge_override
    from ..relationships import read_relationships_csv, write_relationships_csv
except ImportError:
    # Support `streamlit run rigor_sf/ui/app.py` by ensuring repo root is importable.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from rigor_sf.config import load_config
    from rigor_sf.overrides import OverrideEdge, load_overrides, save_overrides, upsert_edge_override
    from rigor_sf.relationships import read_relationships_csv, write_relationships_csv


def is_auto_approved(evidence: str) -> bool:
    """Return True when evidence contains auto-approval marker."""
    return "[auto-approved]" in str(evidence).lower()


def compute_quality_flag(row: pd.Series) -> str:
    """Classify row quality from profiling indicators."""
    match_rate = pd.to_numeric(row.get("match_rate"), errors="coerce")
    pk_unique = pd.to_numeric(row.get("pk_unique_rate"), errors="coerce")
    fk_null = pd.to_numeric(row.get("fk_null_rate"), errors="coerce")

    mr = float(match_rate) if pd.notna(match_rate) else 0.0
    pk = float(pk_unique) if pd.notna(pk_unique) else 0.0
    fn = float(fk_null) if pd.notna(fk_null) else 1.0

    if mr < 0.50 or fn > 0.50:
        return "critical"
    if mr < 0.90 or pk < 0.90 or fn > 0.20:
        return "warning"
    return "ok"


def suggest_classification(in_deg: float, out_deg: float, total_deg: float, bridge_score: float) -> str:
    """Suggest table class using degree and bridge heuristics."""
    in_d = float(in_deg or 0)
    out_d = float(out_deg or 0)
    total = float(total_deg or 0)
    bridge = float(bridge_score or 0)

    if total == 0:
        return ""
    if total >= 4 and bridge >= 0.45 and in_d >= 2 and out_d >= 2:
        return "bridge"
    if in_d >= 3 and out_d <= 1:
        return "fact"
    if out_d >= 3 and in_d <= 1:
        return "dimension"
    if total <= 2:
        return "entity"
    return ""


def summarize_relationship_progress(df: pd.DataFrame) -> dict[str, object]:
    """Build counts and status distribution for relationship progress."""
    if df.empty:
        return {"total": 0, "approved": 0, "rejected": 0, "proposed": 0, "status_mix": {}}

    if "status" in df.columns:
        statuses = df["status"].fillna("proposed").astype(str).str.lower()
    else:
        statuses = pd.Series(["proposed"] * len(df), dtype=str)
    mix = statuses.value_counts().to_dict()
    return {
        "total": int(len(df)),
        "approved": int(mix.get("approved", 0)),
        "rejected": int(mix.get("rejected", 0)),
        "proposed": int(mix.get("proposed", 0)),
        "status_mix": {k: int(v) for k, v in mix.items()},
    }


def summarize_classification_progress(df: pd.DataFrame) -> dict[str, object]:
    """Build completion counts for table classifications."""
    if df.empty:
        return {"total": 0, "classified": 0, "unclassified": 0, "class_mix": {}}

    cls = df.get("current_class", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    classified = cls[cls != ""]
    mix = classified.value_counts().to_dict()
    return {
        "total": int(len(df)),
        "classified": int(len(classified)),
        "unclassified": int(len(df) - len(classified)),
        "class_mix": {k: int(v) for k, v in mix.items()},
    }


def _inject_design_system() -> None:
    import streamlit as st

    st.markdown(
        """
<style>
:root {
  --glass-bg: rgba(248, 251, 255, 0.58);
  --glass-bg-strong: rgba(247, 249, 254, 0.8);
  --glass-card: rgba(255, 255, 255, 0.68);
  --glass-border: rgba(153, 173, 204, 0.5);
  --glass-highlight: rgba(255, 255, 255, 0.84);
  --text-primary: #0f1728;
  --text-secondary: #31405f;
  --text-tertiary: #4c5d80;
  --accent: #2f6fed;
  --accent-deep: #2054bd;
  --success: #11623c;
  --warning: #835100;
  --critical: #8a2229;
  --focus: #1e56cf;
  --space-1: 0.5rem;
  --space-2: 0.75rem;
  --space-3: 1rem;
  --space-4: 1.5rem;
  --space-5: 2rem;
  --radius-1: 12px;
  --radius-2: 20px;
  --radius-pill: 999px;
  --shadow-1: 0 10px 24px rgba(30, 53, 89, 0.14);
  --shadow-2: 0 22px 52px rgba(21, 43, 80, 0.18);
  --fs-100: 0.875rem;
  --fs-200: 1rem;
  --fs-300: 1.08rem;
  --fs-400: 1.32rem;
  --fs-500: 1.74rem;
  --fs-600: 2.1rem;
  --motion-fast: 150ms;
  --motion-base: 200ms;
  --motion-slow: 250ms;
  --ease-main: cubic-bezier(0.25, 0.9, 0.22, 1);
  --z-10: 10;
  --z-20: 20;
  --z-30: 30;
  --z-50: 50;
}

.stApp {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Helvetica Neue", "Segoe UI", sans-serif;
  color: var(--text-primary);
  background:
    radial-gradient(1200px 640px at -5% -12%, rgba(146, 174, 224, 0.34), rgba(146, 174, 224, 0)),
    radial-gradient(980px 560px at 100% 0%, rgba(190, 210, 255, 0.35), rgba(190, 210, 255, 0)),
    linear-gradient(180deg, #eaf1ff 0%, #e4ebf8 42%, #e8eef8 100%);
}

h1, h2, h3 {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "Helvetica Neue", "Segoe UI", sans-serif;
  letter-spacing: -0.02em;
  color: var(--text-primary);
  font-weight: 630;
}

p, li, label, .stMarkdown, .stCaption {
  font-size: var(--fs-200);
  line-height: 1.55;
}

[data-testid="stAppViewContainer"] > .main {
  max-width: 1240px;
}

.glass-shell {
  animation: fade-rise var(--motion-slow) var(--ease-main);
}

.glass-hero {
  background:
    linear-gradient(140deg, rgba(255, 255, 255, 0.66), rgba(233, 242, 255, 0.46)),
    radial-gradient(200px 120px at 86% 18%, rgba(255, 255, 255, 0.6), rgba(255, 255, 255, 0));
  border: 1px solid var(--glass-border);
  border-radius: var(--radius-2);
  box-shadow: var(--shadow-2);
  padding: var(--space-5);
  margin-bottom: var(--space-4);
  backdrop-filter: blur(20px) saturate(1.2);
  -webkit-backdrop-filter: blur(20px) saturate(1.2);
}

.glass-kicker {
  color: var(--text-tertiary);
  font-size: var(--fs-100);
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  margin-bottom: 0.4rem;
}

.glass-title {
  margin: 0;
  font-size: clamp(1.65rem, 2.4vw, 2.25rem);
  line-height: 1.15;
}

.glass-subtitle {
  margin: 0.65rem 0 0;
  color: var(--text-secondary);
  max-width: 72ch;
}

.glass-card {
  background: linear-gradient(180deg, var(--glass-bg-strong) 0%, var(--glass-card) 100%);
  border: 1px solid var(--glass-border);
  border-radius: var(--radius-2);
  box-shadow: var(--shadow-1);
  padding: var(--space-4);
  margin: 0 0 var(--space-4);
  animation: fade-rise var(--motion-slow) var(--ease-main);
  backdrop-filter: blur(16px) saturate(1.18);
  -webkit-backdrop-filter: blur(16px) saturate(1.18);
}

.glass-section-header {
  margin: 0 0 var(--space-3);
}

.glass-section-header h3 {
  margin: 0;
  font-size: var(--fs-400);
  letter-spacing: -0.015em;
}

.glass-section-header p {
  margin: 0.35rem 0 0;
  color: var(--text-secondary);
  max-width: 72ch;
}

.glass-kpi-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: var(--space-3);
  margin-bottom: var(--space-4);
}

.glass-kpi {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.84) 0%, rgba(238, 244, 255, 0.72) 100%);
  border: 1px solid rgba(154, 177, 213, 0.55);
  border-radius: var(--radius-1);
  padding: var(--space-3);
  min-height: 108px;
  backdrop-filter: blur(12px) saturate(1.18);
  -webkit-backdrop-filter: blur(12px) saturate(1.18);
}

.kpi-label {
  color: var(--text-tertiary);
  font-size: var(--fs-100);
  margin: 0;
}

.kpi-value {
  margin: 0.25rem 0 0;
  font-size: var(--fs-500);
  font-weight: 640;
  color: var(--text-primary);
}

.kpi-note {
  color: var(--text-secondary);
  margin: 0.2rem 0 0;
  font-size: var(--fs-100);
}

.glass-pills {
  display: flex;
  gap: 0.55rem;
  flex-wrap: wrap;
  margin: 0.2rem 0 0.8rem;
}

.glass-pill {
  border-radius: var(--radius-pill);
  padding: 0.38rem 0.7rem;
  border: 1px solid rgba(136, 162, 201, 0.5);
  font-size: var(--fs-100);
  font-weight: 600;
  background: rgba(245, 248, 255, 0.7);
  color: var(--text-secondary);
  backdrop-filter: blur(8px) saturate(1.1);
  -webkit-backdrop-filter: blur(8px) saturate(1.1);
}

.pill-approved { color: #0c5d3a; border-color: rgba(61, 156, 111, 0.5); background: rgba(232, 250, 241, 0.78); }
.pill-proposed { color: #30415f; border-color: rgba(136, 162, 201, 0.5); background: rgba(243, 247, 255, 0.78); }
.pill-rejected { color: #7b2027; border-color: rgba(197, 102, 112, 0.55); background: rgba(252, 234, 236, 0.8); }
.pill-critical { color: var(--critical); border-color: rgba(197, 102, 112, 0.55); background: rgba(252, 234, 236, 0.8); }
.pill-warning { color: var(--warning); border-color: rgba(214, 169, 82, 0.5); background: rgba(255, 245, 225, 0.82); }
.pill-ok { color: var(--success); border-color: rgba(61, 156, 111, 0.5); background: rgba(232, 250, 241, 0.78); }

.glass-alert {
  background: rgba(255, 248, 230, 0.74);
  border: 1px solid rgba(208, 165, 75, 0.58);
  color: #664611;
  border-radius: var(--radius-1);
  padding: 0.7rem 0.9rem;
  margin-top: 0.6rem;
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
}

.glass-empty {
  background: rgba(249, 252, 255, 0.78);
  border: 1px dashed rgba(131, 152, 185, 0.75);
  border-radius: var(--radius-1);
  padding: 1rem;
  color: var(--text-secondary);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
}

button, [role="button"], .stButton > button {
  min-height: 44px;
  cursor: pointer;
  border-radius: var(--radius-1);
  transition:
    transform var(--motion-fast) var(--ease-main),
    box-shadow var(--motion-fast) var(--ease-main),
    background-color var(--motion-base) var(--ease-main);
}

.stButton > button[kind="primary"] {
  background: linear-gradient(180deg, #5e8ff7 0%, var(--accent) 100%);
  border: 1px solid #255ecf;
  color: #ffffff;
  box-shadow: 0 8px 18px rgba(44, 103, 220, 0.32);
}

.stButton > button:hover {
  transform: translateY(-1px);
}

button:focus-visible, input:focus-visible, textarea:focus-visible, select:focus-visible,
[role="tab"]:focus-visible, [data-baseweb="select"] *:focus-visible {
  outline: 3px solid var(--focus) !important;
  outline-offset: 2px !important;
}

.stTextInput input, .stTextArea textarea {
  min-height: 44px;
  background: rgba(255, 255, 255, 0.93);
  color: var(--text-primary);
  border: 1px solid rgba(145, 164, 194, 0.75);
  border-radius: 10px;
}

[data-baseweb="slider"] {
  padding-top: 0.45rem;
  padding-bottom: 0.45rem;
}

[data-baseweb="tab-list"] {
  gap: 0.4rem;
  background: rgba(245, 249, 255, 0.63);
  border: 1px solid var(--glass-border);
  border-radius: var(--radius-1);
  padding: 0.3rem;
  backdrop-filter: blur(14px) saturate(1.15);
  -webkit-backdrop-filter: blur(14px) saturate(1.15);
}

[data-baseweb="tab"] {
  font-size: var(--fs-200);
  border-radius: 10px;
  color: var(--text-secondary) !important;
}

[data-baseweb="tab"][aria-selected="true"] {
  background: rgba(255, 255, 255, 0.82) !important;
  color: var(--text-primary) !important;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 3px 10px rgba(44, 72, 122, 0.18);
  border-bottom-color: transparent !important;
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(241, 247, 255, 0.92), rgba(234, 241, 253, 0.9));
  border-right: 1px solid var(--glass-border);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
}

[data-testid="stDataFrame"], [data-testid="stDataEditor"] {
  border: 1px solid rgba(145, 166, 198, 0.75);
  border-radius: var(--radius-1);
  overflow: hidden;
  background: rgba(255, 255, 255, 0.96);
}

[data-testid="stDataFrame"] *,
[data-testid="stDataEditor"] * {
  color: var(--text-primary);
}

[data-testid="stMarkdownContainer"] code {
  background: rgba(242, 247, 255, 0.95);
  border: 1px solid rgba(154, 177, 213, 0.55);
  border-radius: 8px;
  padding: 0.08rem 0.35rem;
}

@keyframes fade-rise {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}

@media (max-width: 900px) {
  .glass-hero { padding: var(--space-4); }
  .glass-kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 640px) {
  .glass-kpi-grid { grid-template-columns: 1fr; }
  p, li, label, .stMarkdown, .stCaption { font-size: 16px; }
}

@supports not ((backdrop-filter: blur(8px)) or (-webkit-backdrop-filter: blur(8px))) {
  .glass-hero, .glass-card, .glass-kpi, .glass-pill, .glass-alert, .glass-empty, [data-baseweb="tab-list"], [data-testid="stSidebar"] {
    background: rgba(247, 250, 255, 0.95) !important;
  }
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 1ms !important;
    transition-duration: 1ms !important;
    scroll-behavior: auto !important;
  }
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _render_app_shell_header(cfg_path: str, rel_path: Path, ovr_path: Path) -> None:
    import streamlit as st

    st.markdown(
        f"""
<div class="glass-shell">
  <section class="glass-hero">
    <p class="glass-kicker">RIGOR SF • Review Workspace</p>
    <h1 class="glass-title">Ontology Relationship & Classification Review</h1>
    <p class="glass-subtitle">
      Review inferred joins, manage approval decisions, and persist table classifications with auditable overrides.
      This UI keeps inference and override files synchronized as you work.
    </p>
  </section>
</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
<div class="glass-card">
  <div class="glass-section-header">
    <h3>Project Inputs</h3>
    <p>Current file targets loaded from your config path. Update in the sidebar when switching environments.</p>
  </div>
  <p><strong>Config:</strong> <code>{cfg_path}</code></p>
  <p><strong>Relationships CSV:</strong> <code>{rel_path}</code></p>
  <p><strong>Overrides YAML:</strong> <code>{ovr_path}</code></p>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_section_header(title: str, subtitle: str) -> None:
    import streamlit as st

    st.markdown(
        f"""
<div class="glass-section-header">
  <h3>{title}</h3>
  <p>{subtitle}</p>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_kpi_strip(metrics: list[dict[str, str]]) -> None:
    import streamlit as st

    tiles = []
    for item in metrics:
        tiles.append(
            f"""
<article class="glass-kpi">
  <p class="kpi-label">{item["label"]}</p>
  <p class="kpi-value">{item["value"]}</p>
  <p class="kpi-note">{item["note"]}</p>
</article>
            """
        )
    st.markdown('<section class="glass-kpi-grid">' + "".join(tiles) + "</section>", unsafe_allow_html=True)


def _render_status_mix_pills(progress: dict[str, object]) -> None:
    import streamlit as st

    mix = progress.get("status_mix", {})
    approved = int(mix.get("approved", 0))
    proposed = int(mix.get("proposed", 0))
    rejected = int(mix.get("rejected", 0))
    st.markdown(
        f"""
<div class="glass-pills">
  <span class="glass-pill pill-approved">Approved: {approved}</span>
  <span class="glass-pill pill-proposed">Proposed: {proposed}</span>
  <span class="glass-pill pill-rejected">Rejected: {rejected}</span>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_shortcuts_help() -> None:
    import streamlit as st
    import streamlit.components.v1 as components

    st.caption("Shortcuts: Ctrl/Cmd+S = Save CSV, Ctrl/Cmd+O = Write Overrides, Ctrl/Cmd+K = Save Classifications")
    components.html(
        """
<script>
(function () {
  const map = {
    's': { title: 'Hotkey: Ctrl/Cmd+S', label: 'Save CSV' },
    'o': { title: 'Hotkey: Ctrl/Cmd+O', label: 'Write Overrides from visible rows' },
    'k': { title: 'Hotkey: Ctrl/Cmd+K', label: 'Save classifications to overrides.yaml' }
  };
  function clickByTarget(target) {
    const doc = window.parent.document;
    const byTitle = doc.querySelector(`button[title*="${target.title}"]`);
    if (byTitle) {
      byTitle.click();
      return true;
    }
    const buttons = doc.querySelectorAll('button');
    for (const button of buttons) {
      const label = (button.innerText || '').trim();
      if (label === target.label) {
        button.click();
        return true;
      }
    }
    return false;
  }
  window.addEventListener('keydown', function (event) {
    if (!(event.ctrlKey || event.metaKey)) return;
    const key = (event.key || '').toLowerCase();
    const target = map[key];
    if (!target) return;
    event.preventDefault();
    clickByTarget(target);
  });
})();
</script>
        """,
        height=0,
    )


def _merge_back(original: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["from_table", "from_columns", "to_table", "to_columns"]
    out = original.copy()
    if "from_columns" not in out.columns:
        out["from_columns"] = out.get("from_column", "").astype(str)
    if "to_columns" not in out.columns:
        out["to_columns"] = out.get("to_column", "").astype(str)
    out = out.set_index(key_cols)

    edited = edited_df.copy().set_index(key_cols)
    for idx, row in edited.iterrows():
        if idx in out.index:
            for col in ["status", "evidence", "confidence_sql", "match_rate", "pk_unique_rate", "fk_null_rate"]:
                if col in row:
                    out.loc[idx, col] = row[col]

    out = out.reset_index()
    out["from_column"] = out["from_columns"].astype(str).str.split(";").str[0].fillna("")
    out["to_column"] = out["to_columns"].astype(str).str.split(";").str[0].fillna("")
    return out


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="RIGOR Review", layout="wide")
    _inject_design_system()

    cfg_path = st.sidebar.text_input("Config path", value="config/config.yaml")
    st.sidebar.markdown("### Workspace Files")
    if not Path(cfg_path).exists():
        st.sidebar.error("Config file not found.")
        st.markdown(
            """
<div class="glass-alert">
  Config path could not be loaded. Create <code>config/config.yaml</code> from
  <code>config.example.yaml</code> and update the sidebar path.
</div>
            """,
            unsafe_allow_html=True,
        )
        st.stop()

    cfg = load_config(cfg_path)
    rel_path = Path(cfg.paths.inferred_relationships_csv)
    ovr_path = Path(cfg.paths.overrides_yaml)
    _render_app_shell_header(cfg_path, rel_path, ovr_path)

    st.sidebar.code(str(rel_path))
    st.sidebar.code(str(ovr_path))

    tabs = st.tabs(["Relationships", "Table Classification", "How to Run"])

    with tabs[0]:
        _render_shortcuts_help()
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        _render_section_header(
            "Relationship Decisions",
            "Filter, inspect, and decide on inferred relationships before persisting to CSV and overrides.",
        )

        if not rel_path.exists():
            st.markdown(
                """
<div class="glass-empty">
  No inferred_relationships.csv found. Run:
  <code>rigor --config config/config.yaml --sql-dir sql_worksheets/ --phase infer</code>
</div>
                """,
                unsafe_allow_html=True,
            )
            st.stop()

        df = read_relationships_csv(str(rel_path))
        overrides = load_overrides(str(ovr_path))

        for col in ["status", "confidence_sql", "match_rate", "pk_unique_rate", "fk_null_rate", "evidence"]:
            if col not in df.columns:
                df[col] = ""
        if "from_columns" not in df.columns:
            df["from_columns"] = df.get("from_column", "").astype(str)
        if "to_columns" not in df.columns:
            df["to_columns"] = df.get("to_column", "").astype(str)

        df["auto_approved"] = df["evidence"].fillna("").map(is_auto_approved)
        df["quality_flag"] = df.apply(compute_quality_flag, axis=1)

        st.markdown(
            """
<div class="glass-section-header">
  <h3>Control Panel</h3>
  <p>Use filters to reduce visible rows before editing and running write actions.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            statuses = sorted(df["status"].astype(str).unique().tolist())
            default = ["proposed"] if "proposed" in statuses else statuses
            status_filter = st.multiselect("Status", options=statuses, default=default)
        with c2:
            min_conf = st.slider("Min SQL confidence", 0.0, 1.0, 0.7, 0.05)
        with c3:
            min_match = st.slider("Min match_rate (profiling)", 0.0, 1.0, 0.0, 0.05)
        with c4:
            search = st.text_input("Search")

        fdf = df.copy()
        fdf["confidence_sql_num"] = pd.to_numeric(fdf["confidence_sql"], errors="coerce").fillna(0.0)
        fdf["match_rate_num"] = pd.to_numeric(fdf["match_rate"], errors="coerce").fillna(0.0)

        if status_filter:
            fdf = fdf[fdf["status"].astype(str).isin(status_filter)]
        fdf = fdf[fdf["confidence_sql_num"] >= float(min_conf)]
        fdf = fdf[fdf["match_rate_num"] >= float(min_match)]

        if search.strip():
            s = search.strip().upper()
            mask = (
                fdf["from_table"].astype(str).str.upper().str.contains(s)
                | fdf["to_table"].astype(str).str.upper().str.contains(s)
                | fdf["from_columns"].astype(str).str.upper().str.contains(s)
                | fdf["to_columns"].astype(str).str.upper().str.contains(s)
                | fdf["evidence"].astype(str).str.upper().str.contains(s)
            )
            fdf = fdf[mask]

        rel_progress = summarize_relationship_progress(fdf)
        _render_kpi_strip(
            [
                {"label": "Visible Relationships", "value": str(rel_progress["total"]), "note": "Filtered scope"},
                {"label": "Approved", "value": str(rel_progress["approved"]), "note": "Ready for generation"},
                {"label": "Proposed", "value": str(rel_progress["proposed"]), "note": "Needs review"},
                {"label": "Rejected", "value": str(rel_progress["rejected"]), "note": "Excluded edges"},
            ]
        )
        _render_status_mix_pills(rel_progress)

        st.caption("Tip: Use from_columns/to_columns for composite joins (e.g., ORDER_ID;LINE_ID).")

        edited = st.data_editor(
            fdf[
                [
                    "from_table",
                    "from_columns",
                    "to_table",
                    "to_columns",
                    "confidence_sql",
                    "match_rate",
                    "pk_unique_rate",
                    "fk_null_rate",
                    "quality_flag",
                    "auto_approved",
                    "status",
                    "evidence",
                ]
            ],
            use_container_width=True,
            num_rows="fixed",
            key="rel_editor",
        )

        rel_name = st.text_input("Relation name (optional) for overrides on approved edges", value="")
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            if st.button("Save CSV", type="primary", help="Hotkey: Ctrl/Cmd+S"):
                df2 = _merge_back(df, edited)
                write_relationships_csv(df2, str(rel_path))
                st.success("Saved inferred_relationships.csv")

        with col_b:
            if st.button("Flip direction for visible rows", help="Switch from/to table and columns for visible rows."):
                flipped = edited.copy()
                flipped[["from_table", "to_table"]] = flipped[["to_table", "from_table"]]
                flipped[["from_columns", "to_columns"]] = flipped[["to_columns", "from_columns"]]
                flipped["status"] = "proposed"

                df2 = df.copy()
                if "from_columns" not in df2.columns:
                    df2["from_columns"] = df2.get("from_column", "").astype(str)
                if "to_columns" not in df2.columns:
                    df2["to_columns"] = df2.get("to_column", "").astype(str)

                combined = pd.concat([df2, flipped], ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=["from_table", "from_columns", "to_table", "to_columns"], keep="last"
                )
                combined["from_column"] = combined["from_columns"].astype(str).str.split(";").str[0].fillna("")
                combined["to_column"] = combined["to_columns"].astype(str).str.split(";").str[0].fillna("")
                write_relationships_csv(combined, str(rel_path))
                st.success("Flipped rows appended and saved. Review them and then write overrides if needed.")

        with col_c:
            if st.button("Write Overrides from visible rows", help="Hotkey: Ctrl/Cmd+O", type="primary"):
                for _, row in edited.iterrows():
                    status = str(row.get("status", "proposed")).lower()
                    if status not in ("approved", "rejected"):
                        continue
                    from_cols = [c.strip() for c in str(row["from_columns"]).split(";") if c.strip()]
                    to_cols = [c.strip() for c in str(row["to_columns"]).split(";") if c.strip()]
                    edge = OverrideEdge(
                        from_table=str(row["from_table"]),
                        from_column=from_cols,
                        to_table=str(row["to_table"]),
                        to_column=to_cols,
                        relation_name=(rel_name.strip() or None),
                        status=("rejected" if status == "rejected" else "approved"),
                    )
                    overrides = upsert_edge_override(overrides, edge)
                save_overrides(str(ovr_path), overrides)
                st.success("Updated overrides.yaml (supports composite keys).")
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[1]:
        _render_shortcuts_help()
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)

        overrides = load_overrides(str(ovr_path))
        table_class = overrides.get("table_classification", {}) or {}

        _render_section_header(
            "Table Classification",
            "Classify tables to guide ontology generation quality and prompt context.",
        )

        if rel_path.exists():
            df = read_relationships_csv(str(rel_path))
            if "from_table" in df.columns and "to_table" in df.columns:
                out_deg = df.groupby("from_table").size().rename("out_edges")
                in_deg = df.groupby("to_table").size().rename("in_edges")
                deg = pd.concat([out_deg, in_deg], axis=1).fillna(0)
                deg["total_edges"] = deg["out_edges"] + deg["in_edges"]
                deg["bridge_score"] = deg[["in_edges", "out_edges"]].min(axis=1) / deg["total_edges"].replace(0, 1)
                deg = deg.sort_values("total_edges", ascending=False).reset_index().rename(columns={"index": "table"})
            else:
                deg = pd.DataFrame(columns=["table", "out_edges", "in_edges", "total_edges", "bridge_score"])
        else:
            deg = pd.DataFrame(columns=["table", "out_edges", "in_edges", "total_edges", "bridge_score"])

        rows = []
        for _, r in deg.iterrows():
            t = str(r.get("table", ""))
            if not t:
                continue
            in_edges = float(r.get("in_edges", 0))
            out_edges = float(r.get("out_edges", 0))
            total_edges = float(r.get("total_edges", 0))
            bridge_score = float(r.get("bridge_score", 0))
            rows.append(
                {
                    "table": t,
                    "current_class": table_class.get(t.upper(), ""),
                    "suggested": suggest_classification(in_edges, out_edges, total_edges, bridge_score),
                    "in_edges": int(in_edges),
                    "out_edges": int(out_edges),
                    "total_edges": int(total_edges),
                }
            )

        tdf = pd.DataFrame(rows)
        progress = summarize_classification_progress(tdf)
        _render_kpi_strip(
            [
                {
                    "label": "Classified Tables",
                    "value": f"{progress['classified']}/{progress['total']}",
                    "note": f"{progress['unclassified']} remaining",
                },
                {
                    "label": "Fact Tables",
                    "value": str(progress["class_mix"].get("fact", 0)),
                    "note": "Current class mix",
                },
                {
                    "label": "Dimension Tables",
                    "value": str(progress["class_mix"].get("dimension", 0)),
                    "note": "Current class mix",
                },
                {
                    "label": "Bridge Tables",
                    "value": str(progress["class_mix"].get("bridge", 0)),
                    "note": "Current class mix",
                },
            ]
        )
        if progress["class_mix"]:
            st.caption("Class mix: " + ", ".join(f"{k}={v}" for k, v in progress["class_mix"].items()))

        st.dataframe(tdf, use_container_width=True, hide_index=True)

        _render_section_header("Edit Classifications", "Update per-row values or bulk-apply one class to visible rows.")
        if tdf.empty:
            st.markdown(
                '<div class="glass-empty">No relationships yet. Run infer phase first.</div>',
                unsafe_allow_html=True,
            )
        else:
            edited_tbl = st.data_editor(
                tdf[["table", "current_class", "suggested", "in_edges", "out_edges", "total_edges"]],
                use_container_width=True,
                num_rows="fixed",
                key="tbl_editor",
            )
            chosen_class = st.text_input(
                "Set class for selected/visible tables (optional)",
                value="",
                help="If provided, this class will be applied to visible rows on Save.",
            )
            if st.button("Save classifications to overrides.yaml", type="primary", help="Hotkey: Ctrl/Cmd+K"):
                if chosen_class.strip():
                    for _, row in edited_tbl.iterrows():
                        table_class[str(row["table"]).upper()] = chosen_class.strip()
                else:
                    for _, row in edited_tbl.iterrows():
                        cls = str(row.get("current_class", "")).strip()
                        if cls:
                            table_class[str(row["table"]).upper()] = cls
                overrides["table_classification"] = table_class
                save_overrides(str(ovr_path), overrides)
                st.success("Saved table classifications to overrides.yaml")
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[2]:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        _render_section_header(
            "Recommended Workflow",
            "Run these phases in order to build trust in inferred relationships and ontology outputs.",
        )
        st.markdown(
            """1) **Generate profiling SQL from worksheets**
```bash
rigor --config config/config.yaml --phase query-gen --sql-dir sql_worksheets/
```

2) **Infer joins and merge profiling outputs**
```bash
rigor --config config/config.yaml --phase infer --sql-dir sql_worksheets/ --run-dir runs/<run_id>
```

3) **Review relationships and classify tables in this UI**
```bash
streamlit run rigor_sf/ui/app.py
```

4) **Generate ontology**
```bash
rigor --config config/config.yaml --phase generate
```

5) **Validate ontology and coverage gates**
```bash
rigor --config config/config.yaml --phase validate
```
"""
        )
        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
