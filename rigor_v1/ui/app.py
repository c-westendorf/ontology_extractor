from __future__ import annotations
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
try:
    from ..config import load_config
    from ..relationships import read_relationships_csv, write_relationships_csv
    from ..overrides import load_overrides, save_overrides, upsert_edge_override, OverrideEdge
except ImportError:
    # Support `streamlit run rigor_v1/ui/app.py` by ensuring repo root is importable.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from rigor_v1.config import load_config
    from rigor_v1.relationships import read_relationships_csv, write_relationships_csv
    from rigor_v1.overrides import load_overrides, save_overrides, upsert_edge_override, OverrideEdge

st.set_page_config(page_title="RIGOR Review", layout="wide")
st.title("RIGOR Review (Local)")

cfg_path = st.sidebar.text_input("Config path", value="rigor/config.yaml")
if not Path(cfg_path).exists():
    st.sidebar.warning("Config file not found. Create rigor/config.yaml from config.example.yaml.")
    st.stop()

cfg = load_config(cfg_path)
rel_path = Path(cfg.paths.inferred_relationships_csv)
ovr_path = Path(cfg.paths.overrides_yaml)

st.sidebar.markdown("### Files")
st.sidebar.code(str(rel_path))
st.sidebar.code(str(ovr_path))

tabs = st.tabs(["Relationships", "Table Classification", "How to Run"])

# ------------------------
# Relationships tab
# ------------------------
with tabs[0]:
    if not rel_path.exists():
        st.info("No inferred_relationships.csv found. Run:\n\n`python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase infer`")
        st.stop()

    df = read_relationships_csv(str(rel_path))
    overrides = load_overrides(str(ovr_path))

    # Back-compat columns
    for col in ["status","confidence_sql","match_rate","pk_unique_rate","fk_null_rate","evidence"]:
        if col not in df.columns:
            df[col] = ""
    # Composite-key columns (semicolon-separated lists)
    if "from_columns" not in df.columns:
        df["from_columns"] = df.get("from_column","").astype(str)
    if "to_columns" not in df.columns:
        df["to_columns"] = df.get("to_column","").astype(str)

    st.markdown("### Filters")
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
            fdf["from_table"].astype(str).str.upper().str.contains(s) |
            fdf["to_table"].astype(str).str.upper().str.contains(s) |
            fdf["from_columns"].astype(str).str.upper().str.contains(s) |
            fdf["to_columns"].astype(str).str.upper().str.contains(s) |
            fdf["evidence"].astype(str).str.upper().str.contains(s)
        )
        fdf = fdf[mask]

    st.caption("Tip: Use from_columns/to_columns for composite joins. Separate columns with ';' (e.g., ORDER_ID;LINE_ID).")

    edited = st.data_editor(
        fdf[["from_table","from_columns","to_table","to_columns","confidence_sql","match_rate","pk_unique_rate","fk_null_rate","status","evidence"]],
        use_container_width=True,
        num_rows="fixed",
        key="rel_editor",
    )

    rel_name = st.text_input("Relation name (optional) for overrides on approved edges", value="")

    colA, colB, colC = st.columns(3)

    def _merge_back(original: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
        key_cols = ["from_table","from_columns","to_table","to_columns"]
        o = original.copy()
        if "from_columns" not in o.columns:
            o["from_columns"] = o.get("from_column","").astype(str)
        if "to_columns" not in o.columns:
            o["to_columns"] = o.get("to_column","").astype(str)
        o = o.set_index(key_cols)

        e = edited_df.copy().set_index(key_cols)
        # Update matching keys (note: if you changed key fields, treat as new row; handled via bulk actions)
        for idx, row in e.iterrows():
            if idx in o.index:
                for c in ["status","evidence","confidence_sql","match_rate","pk_unique_rate","fk_null_rate"]:
                    if c in row:
                        o.loc[idx, c] = row[c]
        o = o.reset_index()
        # Keep legacy columns aligned (single-col convenience)
        o["from_column"] = o["from_columns"].astype(str).str.split(";").str[0].fillna("")
        o["to_column"] = o["to_columns"].astype(str).str.split(";").str[0].fillna("")
        return o

    with colA:
        if st.button("Save CSV"):
            df2 = _merge_back(df, edited)
            write_relationships_csv(df2, str(rel_path))
            st.success("Saved inferred_relationships.csv")

    with colB:
        if st.button("Flip direction for visible rows"):
            flipped = edited.copy()
            flipped[["from_table","to_table"]] = flipped[["to_table","from_table"]]
            flipped[["from_columns","to_columns"]] = flipped[["to_columns","from_columns"]]
            # Mark as proposed so it gets re-reviewed
            flipped["status"] = "proposed"
            # Append flipped rows to df (dedupe)
            df2 = df.copy()
            if "from_columns" not in df2.columns:
                df2["from_columns"] = df2.get("from_column","").astype(str)
            if "to_columns" not in df2.columns:
                df2["to_columns"] = df2.get("to_column","").astype(str)

            combined = pd.concat([df2, flipped], ignore_index=True)
            combined = combined.drop_duplicates(subset=["from_table","from_columns","to_table","to_columns"], keep="last")
            combined["from_column"] = combined["from_columns"].astype(str).str.split(";").str[0].fillna("")
            combined["to_column"] = combined["to_columns"].astype(str).str.split(";").str[0].fillna("")
            write_relationships_csv(combined, str(rel_path))
            st.success("Flipped rows appended and saved. Review them and then write overrides if needed.")

    with colC:
        if st.button("Write Overrides from visible rows"):
            for _, row in edited.iterrows():
                status = str(row.get("status","proposed")).lower()
                if status not in ("approved","rejected"):
                    continue
                from_cols = [c.strip() for c in str(row["from_columns"]).split(";") if c.strip()]
                to_cols = [c.strip() for c in str(row["to_columns"]).split(";") if c.strip()]
                edge = OverrideEdge(
                    from_table=str(row["from_table"]),
                    from_column=from_cols,   # can be list
                    to_table=str(row["to_table"]),
                    to_column=to_cols,       # can be list
                    relation_name=(rel_name.strip() or None),
                    status=("rejected" if status=="rejected" else "approved"),
                )
                overrides = upsert_edge_override(overrides, edge)
            save_overrides(str(ovr_path), overrides)
            st.success("Updated overrides.yaml (supports composite keys).")

# ------------------------
# Table Classification tab
# ------------------------
with tabs[1]:
    overrides = load_overrides(str(ovr_path))
    table_class = overrides.get("table_classification", {}) or {}

    st.markdown("### Table Classification")
    st.caption("Classify tables to guide ontology generation. Common classes: entity, dimension, fact, bridge, staging, lookup, event, ...")

    # Build candidate bridge table suggestions from relationships CSV
    if rel_path.exists():
        df = read_relationships_csv(str(rel_path))
        if "from_table" in df.columns and "to_table" in df.columns:
            out_deg = df.groupby("from_table").size().rename("out_edges")
            in_deg = df.groupby("to_table").size().rename("in_edges")
            deg = pd.concat([out_deg, in_deg], axis=1).fillna(0)
            deg["total_edges"] = deg["out_edges"] + deg["in_edges"]
            deg = deg.sort_values("total_edges", ascending=False).reset_index().rename(columns={"index":"table"})
        else:
            deg = pd.DataFrame(columns=["table","out_edges","in_edges","total_edges"])
    else:
        deg = pd.DataFrame(columns=["table","out_edges","in_edges","total_edges"])

    # Build editable table
    rows = []
    for _, r in deg.iterrows():
        t = str(r.get("table",""))
        if not t:
            continue
        rows.append({
            "table": t,
            "current_class": table_class.get(t.upper(), ""),
            "suggested": "bridge" if float(r.get("out_edges",0)) >= 2 else "",
            "total_edges": int(r.get("total_edges",0)),
        })
    tdf = pd.DataFrame(rows)
    st.dataframe(tdf, use_container_width=True, hide_index=True)

    st.markdown("### Edit classifications")
    if tdf.empty:
        st.info("No relationships yet. Run infer phase first.")
    else:
        edited_tbl = st.data_editor(
            tdf[["table","current_class","suggested","total_edges"]],
            use_container_width=True,
            num_rows="fixed",
            key="tbl_editor",
        )
        chosen_class = st.text_input("Set class for selected/visible tables (optional)", value="", help="If provided, this class will be applied to visible rows on Save.")
        if st.button("Save classifications to overrides.yaml"):
            if chosen_class.strip():
                for _, row in edited_tbl.iterrows():
                    table_class[str(row["table"]).upper()] = chosen_class.strip()
            else:
                # Use current_class column as truth
                for _, row in edited_tbl.iterrows():
                    cls = str(row.get("current_class","")).strip()
                    if cls:
                        table_class[str(row["table"]).upper()] = cls
            overrides["table_classification"] = table_class
            save_overrides(str(ovr_path), overrides)
            st.success("Saved table classifications to overrides.yaml")

# ------------------------
# How to Run tab
# ------------------------
with tabs[2]:
    st.markdown("""### Recommended trust-building workflow

1) **Infer joins from worksheets**
```bash
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase infer
```

2) (Optional) **Profile joins in Snowflake** (if your pipeline supports it)
```bash
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase profile --sample-limit 200000
```

3) **Review relationships and classify tables in this UI**
```bash
streamlit run -m rigor.ui.app
```

4) **Generate ontology**
```bash
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase generate
```
""")
