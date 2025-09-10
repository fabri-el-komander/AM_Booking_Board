# app.py — Across Mexico Booking Board (simple)
# Features:
# - Supplier & Service master lists with "+ Add new…" option (persisted in SQLite)
# - Minimal fields: supplier, date, start/end time, client, pax, service, status (hold/booked)
# - Export to .ics (all / selected suppliers) for Google Calendar import
# - SQLAlchemy 2.x compatible

import io
from datetime import datetime, date, time

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from ics import Calendar, Event

st.set_page_config(page_title="Across Mexico Booking Board", layout="wide")

# --------- DB (SQLite) ---------
engine = create_engine("sqlite:///data.db", connect_args={"check_same_thread": False})

def ensure_tables():
    with engine.begin() as conn:
        # events
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            supplier_name TEXT,
            date TEXT,            -- YYYY-MM-DD
            start_time TEXT,      -- HH:MM
            end_time TEXT,        -- HH:MM
            client_name TEXT,
            pax INTEGER,
            service TEXT,
            status TEXT           -- hold | booked
        );
        """)
        # masters
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_name TEXT PRIMARY KEY
        );
        """)
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS services (
            service_name TEXT PRIMARY KEY
        );
        """)

def load_df() -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM events", engine)
    except Exception:
        return pd.DataFrame(columns=["event_id","supplier_name","date","start_time","end_time",
                                     "client_name","pax","service","status"])

# ---- masters: list / add ----
def list_suppliers() -> list:
    try:
        df = pd.read_sql("SELECT supplier_name FROM suppliers ORDER BY supplier_name", engine)
        return df["supplier_name"].tolist()
    except Exception:
        return []

def list_services() -> list:
    try:
        df = pd.read_sql("SELECT service_name FROM services ORDER BY service_name", engine)
        return df["service_name"].tolist()
    except Exception:
        return []

def add_supplier(name: str):
    name = name.strip()
    if not name: return
    with engine.begin() as conn:
        conn.execute(text("INSERT OR IGNORE INTO suppliers (supplier_name) VALUES (:n)"), {"n": name})

def add_service(name: str):
    name = name.strip()
    if not name: return
    with engine.begin() as conn:
        conn.execute(text("INSERT OR IGNORE INTO services (service_name) VALUES (:n)"), {"n": name})

# ---- events CRUD / import ----
def upsert_row(row: dict):
    df = pd.DataFrame([row])
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM events WHERE event_id = :id"), {"id": row["event_id"]})
        df.to_sql("events", conn, if_exists="append", index=False)

def delete_event(event_id: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM events WHERE event_id = :id"), {"id": event_id})

def import_csv_df(df: pd.DataFrame):
    required = {"event_id","supplier_name","date","start_time","end_time","client_name","pax","service","status"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Missing columns: {', '.join(missing)}")
    with engine.begin() as conn:
        df.to_sql("events", conn, if_exists="append", index=False)
    # update masters from imported data
    for s in sorted(set(df["supplier_name"].astype(str).str.strip())):
        add_supplier(s)
    for s in sorted(set(df["service"].astype(str).str.strip())):
        add_service(s)

def df_to_ics_bytes(df_in: pd.DataFrame) -> bytes:
    cal_obj = Calendar()
    for _, r in df_in.iterrows():
        start_dt = pd.to_datetime(f"{r['date']} {r['start_time']}").to_pydatetime()
        end_dt   = pd.to_datetime(f"{r['date']} {r['end_time']}").to_pydatetime()
        title = f"{r['supplier_name']} — {r['service']} ({r['client_name']}, {int(r['pax'])} pax)"
        ev = Event()
        ev.name = title
        ev.begin = start_dt
        ev.end   = end_dt
        ev.status = "CONFIRMED" if str(r["status"]).lower() == "booked" else "TENTATIVE"
        ev.description = (
            f"Supplier: {r['supplier_name']} | Client: {r['client_name']} | "
            f"PAX: {int(r['pax'])} | Status: {r['status']}"
        )
        cal_obj.events.add(ev)
    return str(cal_obj).encode("utf-8")

# --------- Init ---------
ensure_tables()
st.title("📒 Across Mexico Booking Board")

df = load_df()

# --------- Sidebar: import CSV ---------
with st.sidebar:
    st.header("Data")
    up = st.file_uploader("Upload CSV (events_simple.csv)", type=["csv"])
    if up is not None:
        try:
            df_new = pd.read_csv(up)
            import_csv_df(df_new)
            st.success("CSV imported.")
        except Exception as e:
            st.error(f"Import failed: {e}")
    if st.button("Load sample CSV (if present in repo)"):
        try:
            df_example = pd.read_csv("events_simple.csv")
            import_csv_df(df_example)
            st.success("Sample loaded.")
        except Exception as e:
            st.error(f"Load failed: {e}")
    st.markdown("---")
    st.caption("Export a .ics and import it in Google Calendar (Settings → Import & export).")

# --------- Simple filters ---------
colf1, colf2 = st.columns([2,1])
with colf1:
    suppliers_filter = ["(All)"] + (sorted(df["supplier_name"].dropna().unique().tolist()) if not df.empty else [])
    supplier_sel = st.selectbox("Filter by supplier", suppliers_filter)
with colf2:
    status_opt = ["(All)","booked","hold"]
    status_sel = st.selectbox("Filter by status", status_opt, index=0)

fdf = df.copy()
if not fdf.empty:
    if supplier_sel != "(All)":
        fdf = fdf[fdf["supplier_name"] == supplier_sel]
    if status_sel != "(All)":
        fdf = fdf[fdf["status"] == status_sel]

# --------- Add / Edit form ---------
st.subheader("Add / Edit")

# current options from masters
sup_options = list_suppliers()
svc_options = list_services()
ADD_NEW = "+ Add new…"
sup_ui = (sup_options + [ADD_NEW]) if sup_options else [ADD_NEW]
svc_ui = (svc_options + [ADD_NEW]) if svc_options else [ADD_NEW]

with st.form("simple_form", clear_on_submit=False):
    c1, c2, c3 = st.columns([1.3,1,1])
    with c1:
        sel_supplier = st.selectbox("Supplier", sup_ui, index=0)
        new_supplier = ""
        if sel_supplier == ADD_NEW:
            new_supplier = st.text_input("New supplier", placeholder="Etien / Gaby / Mondrian Condesa / ...")
        sel_service = st.selectbox("Service", svc_ui, index=0)
        new_service = ""
        if sel_service == ADD_NEW:
            new_service = st.text_input("New service", placeholder="Airport pickup / Food tour / ...")
        client_name = st.text_input("Client", placeholder="Arturo Sánchez")
    with c2:
        the_date = st.date_input("Date", value=date.today())
        start_t  = st.time_input("Start time", value=time(9,0))
        end_t    = st.time_input("End time", value=time(10,0))
    with c3:
        pax = st.number_input("PAX", min_value=0, value=2)
        status = st.selectbox("Status", ["booked","hold"], index=0)
        event_id = st.text_input("ID (optional)", placeholder="E1234 (leave blank to auto-generate)")

    submitted = st.form_submit_button("Save / Update")
    if submitted:
        supplier_name = (new_supplier if sel_supplier == ADD_NEW else sel_supplier).strip()
        service_name  = (new_service if sel_service == ADD_NEW else sel_service).strip()
        eid = event_id.strip() if event_id.strip() else f"E{datetime.now().strftime('%Y%m%d%H%M%S')}"

        row = dict(
            event_id=eid,
            supplier_name=supplier_name,
            date=str(the_date),
            start_time=start_t.strftime("%H:%M"),
            end_time=end_t.strftime("%H:%M"),
            client_name=client_name.strip(),
            pax=int(pax),
            service=service_name,
            status=status
        )

        # minimal validations
        errors = []
        if not row["supplier_name"]: errors.append("Supplier is required.")
        if not row["service"]: errors.append("Service is required.")
        if not row["client_name"]: errors.append("Client is required.")
        if datetime.combine(the_date, end_t) <= datetime.combine(the_date, start_t):
            errors.append("End time must be greater than start time.")

        if errors:
            st.error(" | ".join(errors))
        else:
            # persist new masters if needed
            add_supplier(row["supplier_name"])
            add_service(row["service"])
            upsert_row(row)
            st.success(f"Saved: {eid}")

# --------- Table ----------
st.subheader("Events")
if not fdf.empty:
    show = fdf.copy()
    show["start_dt"] = pd.to_datetime(show["date"] + " " + show["start_time"])
    show = show.sort_values("start_dt")[[
        "event_id","supplier_name","date","start_time","end_time","client_name","pax","service","status"
    ]]
    st.dataframe(show, use_container_width=True, hide_index=True)
else:
    st.info("No events to display yet.")

# Delete by ID
col_del1, col_del2 = st.columns([1,3])
with col_del1:
    del_id = st.text_input("Delete by ID")
with col_del2:
    if st.button("Delete"):
        if del_id.strip():
            delete_event(del_id.strip())
            st.warning(f"Event {del_id.strip()} deleted.")

# --------- Export ICS ----------
st.subheader("Export to .ics (for Google Calendar)")
if not df.empty:
    ics_all = df_to_ics_bytes(df.sort_values(["date","start_time"]))
    st.download_button("Download ALL as .ics", data=ics_all,
                       file_name="across_calendar_all.ics", mime="text/calendar", use_container_width=True)

    sup_multi = st.multiselect("Choose suppliers to export (optional)",
                               sorted(df["supplier_name"].dropna().unique().tolist()))
    if sup_multi:
        df_sup = df[df["supplier_name"].isin(sup_multi)].copy().sort_values(["date","start_time"])
        st.download_button(
            f"Download {len(sup_multi)} supplier(s) as .ics",
            data=df_to_ics_bytes(df_sup),
            file_name=f"suppliers_{len(sup_multi)}.ics",
            mime="text/calendar",
            use_container_width=True
        )
else:
    st.info("Load or add events to export the .ics.")
