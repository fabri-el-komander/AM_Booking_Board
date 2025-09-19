# --- Replace your current import_csv_df with this flexible version ---

from datetime import datetime, timedelta
import pandas as pd

DEFAULT_HOURS = 4.5  # default duration when end_time is missing

def _guess(colnames, *candidates):
    low = {c.lower(): c for c in colnames}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    # try contains
    for c in colnames:
        cl = c.lower()
        if any(tok.lower() in cl for tok in candidates):
            return c
    return None

def _parse_date(val):
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _parse_time(val):
    s = str(val).strip()
    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%H%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    return None

def import_csv_df(df_raw: pd.DataFrame):
    """Flexible Salesforce import → normalize to Booking Board schema."""
    cols = list(df_raw.columns)

    col_supplier = _guess(cols, "Supplier: Supplier Name", "Supplier", "Guide", "Resource")
    col_date     = _guess(cols, "Booking Start Date", "Service Date", "Date")
    col_time     = _guess(cols, "Start Time", "Time")
    col_client   = _guess(cols, "Trip: Client Name", "Client", "Account", "Contact")
    col_service  = _guess(cols, "Service", "Product", "Service Name", "Task")
    col_pax      = _guess(cols, "Number of Travelers", "PAX", "Guests")
    col_status   = _guess(cols, "Status", "Booking Status")

    if not (col_supplier and col_date and col_time and col_client and col_service and col_pax):
        missing = [name for name, val in {
            "supplier": col_supplier, "date": col_date, "time": col_time,
            "client": col_client, "service": col_service, "pax": col_pax
        }.items() if not val]
        raise ValueError(f"Missing required columns in CSV: {', '.join(missing)}")

    df = pd.DataFrame({
        "supplier_name": df_raw[col_supplier].astype(str).str.strip(),
        "date_raw": df_raw[col_date],
        "time_raw": df_raw[col_time],
        "client_name": df_raw[col_client].astype(str).str.strip(),
        "service": df_raw[col_service].astype(str).str.strip(),
        "pax": pd.to_numeric(df_raw[col_pax], errors="coerce").fillna(0).astype(int),
        "status": (df_raw[col_status].astype(str).str.strip().str.lower()
                   if col_status else "booked")
    })

    # parse date/time
    df["date_obj"] = df["date_raw"].apply(_parse_date)
    df["time_obj"] = df["time_raw"].apply(_parse_time)
    df = df.dropna(subset=["date_obj","time_obj"])

    # start/end times
    df["start_dt"] = df.apply(lambda r: datetime.combine(r["date_obj"], r["time_obj"]), axis=1)
    df["end_dt"] = df["start_dt"] + timedelta(hours=DEFAULT_HOURS)

    # status normalize to booked/hold
    def norm_status(s):
        s = str(s).lower()
        if "hold" in s:
            return "hold"
        if "book" in s or s == "booked" or s == "confirmed":
            return "booked"
        return "booked"  # default
    df["status"] = df["status"].apply(norm_status)

    # final normalized
    norm = pd.DataFrame({
        "event_id": [f"E{dt.strftime('%Y%m%d%H%M%S')}{i:02d}" for i, dt in enumerate(df["start_dt"])],
        "supplier_name": df["supplier_name"],
        "date": df["start_dt"].dt.strftime("%Y-%m-%d"),
        "start_time": df["start_dt"].dt.strftime("%H:%M"),
        "end_time": df["end_dt"].dt.strftime("%H:%M"),
        "client_name": df["client_name"],
        "pax": df["pax"],
        "service": df["service"],
        "status": df["status"],
    })

    # save to DB (append) and update masters
    with engine.begin() as conn:
        norm.to_sql("events", conn, if_exists="append", index=False)
    for s in sorted(set(norm["supplier_name"].astype(str).str.strip())):
        add_supplier(s)
    for s in sorted(set(norm["service"].astype(str).str.strip())):
        add_service(s)

            use_container_width=True
        )
else:
    st.info("Load or add events to export the .ics.")
