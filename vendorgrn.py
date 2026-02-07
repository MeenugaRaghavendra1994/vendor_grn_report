import streamlit as st
import pandas as pd
import io
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# ================= CONFIG =================
PROJECT_ID = "grnreport181922"
DATASET = "vendor_grn"
MAIN_TABLE = "vendor_grn_data"
TEMP_TABLE = "temp_vendor_grn"

# ================= AUTH =================
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

client = bigquery.Client(
    credentials=credentials,
    project=st.secrets["gcp_service_account"]["project_id"],
)

# ================= TEMPLATE COLUMNS =================
BQ_COLUMN_MAP = {
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
    "last_updated": "last_updated",
}

TEMPLATE_COLUMNS = [
    "Vendor Name",
    "PO Number",
    "Reference No",
    "SKU",
    "Name",
    "Invoice Qty",
    "Received Qty",
    "Short Excess Qty",
    "Damage Qty",
    "Actual GRN Qty",
    "Warehouse",
    "Status",
    "GRN No",
    "Ekart GRN Qty",
    "Makali GRN Qty",
    "K12 to SSPL PO",
    "K12 to SSPL GRN",
    "STO Qty",
    "PO",
    "Out Bound",
    "Bill",
    "GRN"
]

REQUIRED_COLUMNS = TEMPLATE_COLUMNS

QTY_COLUMNS = [
    "Invoice Qty",
    "Received Qty",
    "Short Excess Qty",
    "Damage Qty",
    "Actual GRN Qty",
    "Ekart GRN Qty",
    "Makali GRN Qty",
    "STO Qty"
]

# ================= FUNCTIONS =================
def generate_excel_template():
    df = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Vendor_GRN_Template")

    buffer.seek(0)
    return buffer


def validate_columns(df):
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        st.error(f"❌ Missing columns: {missing}")
        st.stop()


def preprocess(df):
    df = df[REQUIRED_COLUMNS]
    df[QTY_COLUMNS] = df[QTY_COLUMNS].fillna(0).astype(int)

    grouped = (
        df.groupby(["Reference No", "SKU"], as_index=False)
        .agg({
            **{col: "sum" for col in QTY_COLUMNS},
            "Vendor Name": "first",
            "PO Number": "first",
            "Name": "first",
            "Warehouse": "first",
            "Status": "first",
            "GRN No": "first",
            "K12 to SSPL PO": "first",
            "K12 to SSPL GRN": "first",
            "PO": "first",
            "Out Bound": "first",
            "Bill": "first",
            "GRN": "first"
        })
    )

    grouped["last_updated"] = datetime.utcnow()
    return grouped

def enforce_dtypes(df):
    # Numeric columns
    for col in QTY_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    # String columns (everything else)
    for col in df.columns:
        if col not in QTY_COLUMNS:
            df[col] = df[col].astype(str).fillna("")

    return df

def load_temp_table(df):
    table_id = f"{PROJECT_ID}.{DATASET}.{TEMP_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("vendor_name", "STRING"),
            bigquery.SchemaField("po_number", "STRING"),
            bigquery.SchemaField("reference_no", "STRING"),
            bigquery.SchemaField("sku", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("invoice_qty", "INTEGER"),
            bigquery.SchemaField("received_qty", "INTEGER"),
            bigquery.SchemaField("short_excess_qty", "INTEGER"),
            bigquery.SchemaField("damage_qty", "INTEGER"),
            bigquery.SchemaField("actual_grn_qty", "INTEGER"),
            bigquery.SchemaField("warehouse", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("grn_no", "STRING"),
            bigquery.SchemaField("ekart_grn_qty", "INTEGER"),
            bigquery.SchemaField("makali_grn_qty", "INTEGER"),
            bigquery.SchemaField("k12_to_sspl_po", "STRING"),
            bigquery.SchemaField("k12_to_sspl_grn", "STRING"),
            bigquery.SchemaField("sto_qty", "INTEGER"),
            bigquery.SchemaField("po", "STRING"),
            bigquery.SchemaField("out_bound", "STRING"),
            bigquery.SchemaField("bill", "STRING"),
            bigquery.SchemaField("grn", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()




def merge_to_main():
    merge_sql = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{MAIN_TABLE}` T
    USING `{PROJECT_ID}.{DATASET}.{TEMP_TABLE}` S
    ON T.reference_no = S.reference_no
       AND T.sku = S.sku

    WHEN MATCHED THEN UPDATE SET
      T.invoice_qty = T.invoice_qty + S.invoice_qty,
      T.received_qty = T.received_qty + S.received_qty,
      T.short_excess_qty = T.short_excess_qty + S.short_excess_qty,
      T.damage_qty = T.damage_qty + S.damage_qty,
      T.actual_grn_qty = T.actual_grn_qty + S.actual_grn_qty,
      T.ekart_grn_qty = T.ekart_grn_qty + S.ekart_grn_qty,
      T.makali_grn_qty = T.makali_grn_qty + S.makali_grn_qty,
      T.sto_qty = T.sto_qty + S.sto_qty,
      T.last_updated = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    client.query(merge_sql).result()

# ================= STREAMLIT UI =================
st.set_page_config(page_title="Vendor GRN Upload", layout="wide")
st.title("📦 Vendor GRN Data Upload & Visibility")

st.subheader("⬇️ Download Excel Upload Template")

template_file = generate_excel_template()

st.download_button(
    label="📥 Download Vendor GRN Excel Template",
    data=template_file,
    file_name="Vendor_GRN_Upload_Template.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.info("📌 Please use this template only. Do not rename columns.")

uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    validate_columns(df)

    st.subheader("📄 Raw Data Preview")
    st.dataframe(df)

    df_processed = preprocess(df)
    df_processed = enforce_dtypes(df_processed)

    st.subheader("📊 Grouped (Reference No + SKU)")
    st.dataframe(df_processed)

    if st.button("✅ Save to BigQuery"):
        df_bq = df_processed.rename(columns=BQ_COLUMN_MAP)
        load_temp_table(df_bq)
        merge_to_main()
        st.success("✅ Data successfully merged into BigQuery!")


# ================= VISIBILITY =================
st.subheader("📈 BigQuery Live Data")

query = f"""
SELECT
  Reference_No,
  SKU,
  Invoice_Qty,
  Received_Qty,
  Actual_GRN_Qty,
  last_updated
FROM `{PROJECT_ID}.{DATASET}.{MAIN_TABLE}`
ORDER BY last_updated DESC
"""

result_df = client.query(query).to_dataframe()

if result_df.empty:
    st.info("No data available yet.")
else:
    st.dataframe(result_df)
