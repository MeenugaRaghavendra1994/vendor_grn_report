import streamlit as st
import pandas as pd
import io
from datetime import datetime
from sqlalchemy import create_engine, text

# ================= POSTGRES CONNECTION =================
db = st.secrets["postgres"]

engine = create_engine(
    f"postgresql+psycopg2://{db['user']}:{db['password']}"
    f"@{db['host']}:{db['port']}/{db['database']}"
)

# ================= COLUMNS =================
TEMPLATE_COLUMNS = [
    "Vendor Name", "PO Number", "Reference No", "SKU", "Name",
    "Invoice Qty", "Received Qty", "Short Excess Qty", "Damage Qty",
    "Actual GRN Qty", "Warehouse", "Status", "GRN No",
    "Ekart GRN Qty", "Makali GRN Qty",
    "K12 to SSPL PO", "K12 to SSPL GRN",
    "STO Qty", "PO", "Out Bound", "Bill", "GRN"
]

DB_COLUMN_MAP = {
    "Vendor Name": "vendor_name",
    "PO Number": "po_number",
    "Reference No": "reference_no",
    "SKU": "sku",
    "Name": "name",
    "Invoice Qty": "invoice_qty",
    "Received Qty": "received_qty",
    "Short Excess Qty": "short_excess_qty",
    "Damage Qty": "damage_qty",
    "Actual GRN Qty": "actual_grn_qty",
    "Warehouse": "warehouse",
    "Status": "status",
    "GRN No": "grn_no",
    "Ekart GRN Qty": "ekart_grn_qty",
    "Makali GRN Qty": "makali_grn_qty",
    "K12 to SSPL PO": "k12_to_sspl_po",
    "K12 to SSPL GRN": "k12_to_sspl_grn",
    "STO Qty": "sto_qty",
    "PO": "po",
    "Out Bound": "out_bound",
    "Bill": "bill",
    "GRN": "grn",
}

QTY_COLUMNS = [
    "invoice_qty", "received_qty", "short_excess_qty",
    "damage_qty", "actual_grn_qty",
    "ekart_grn_qty", "makali_grn_qty", "sto_qty"
]

# ================= FUNCTIONS =================
def generate_template():
    df = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


def preprocess(df):
    df = df[TEMPLATE_COLUMNS]
    df = df.rename(columns=DB_COLUMN_MAP)

    for col in QTY_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in df.columns:
        if col not in QTY_COLUMNS:
            df[col] = df[col].astype(str).fillna("")

    df["uploaded_at"] = datetime.utcnow()
    return df


def save_to_postgres(df):
    df.to_sql(
        "vendor_grn_raw",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )

# ================= STREAMLIT UI =================
st.set_page_config(page_title="Vendor GRN Upload", layout="wide")
st.title("📦 Vendor GRN Upload (PostgreSQL)")

st.download_button(
    "📥 Download Excel Template",
    generate_template(),
    "Vendor_GRN_Template.xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    st.subheader("📄 Raw Upload Preview")
    st.dataframe(df)

    df_processed = preprocess(df)

    st.subheader("📊 Processed Preview")
    st.dataframe(df_processed)

    if st.button("✅ Save to PostgreSQL"):
        save_to_postgres(df_processed)
        st.success("✅ Data saved to PostgreSQL successfully!")

# ================= VISIBILITY =================
st.subheader("📈 Aggregated View (Reference No + SKU)")

query = """
SELECT
    reference_no,
    sku,
    SUM(invoice_qty) AS invoice_qty,
    SUM(received_qty) AS received_qty,
    SUM(actual_grn_qty) AS actual_grn_qty
FROM vendor_grn_raw
GROUP BY reference_no, sku
ORDER BY reference_no, sku;
"""

summary_df = pd.read_sql(query, engine)
st.dataframe(summary_df)
