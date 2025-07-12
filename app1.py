import os
import pandas as pd
import streamlit as st
from io import BytesIO
import plotly.express as px

# === Streamlit Setup ===
st.set_page_config(page_title="Business Insights Dashboard", layout="wide")
st.title("Business Insights Dashboard")

# === File Upload ===
folder_path = r"C:\\Users\\Ashish Yadav\\PycharmProjects\\sperio\\data"
files = [f for f in os.listdir(folder_path) if f.endswith(('.csv', '.xlsx'))]

# Load all files into a dict of DataFrames
dataframes = {}
for file in files:
    file_path = os.path.join(folder_path, file)
    try:
        if file.lower().endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file.lower().endswith(".xlsx"):
            df = pd.read_excel(file_path)
        else:
            continue  # skip unsupported formats

        df.columns = df.columns.str.strip()  # clean column names
        dataframes[file] = df
    except Exception as e:
        st.warning(f"❌ Could not load file '{file}': {e}")

# === Tabs ===
tabs = st.tabs(["Inventory Health", "Returns Analysis", "SLA & TAT", "Sales Overview", "Claim Status", "QC & GRN"])

# --- Inventory Tab ---
with tabs[0]:
    st.header("Inventory Health")
    for file, df in dataframes.items():
        if 'Available Quantity' in df.columns:
            st.subheader(f"Inventory Summary – {file}")
            df_numeric = df.select_dtypes(include='number').sum().reset_index()
            df_numeric.columns = ['Metric', 'Total']
            st.dataframe(df_numeric)

            fig = px.bar(df_numeric, x='Metric', y='Total', title='Inventory Totals')
            st.plotly_chart(fig)

            csv = df_numeric.to_csv(index=False).encode('utf-8')
            st.download_button(f"Download Inventory Report ({file})", csv, f"inventory_{file}.csv")

# --- Returns Analysis Tab ---
with tabs[1]:
    st.header("📦 Returns Analysis")

    for file, df in dataframes.items():
        required_cols = {
            'Order_Date', 'Return_Date', 'Parent_Sku', 'Child_Sku', 'Return_Quantity',
            'Return_Type', 'Seller_Return_Reason', 'Channel_return_reason',
            'Total_Credit_Note_Amount', 'Client_Name', 'Client_Location',
            'Marketplace', 'MP_Alias'
        }

        if required_cols.issubset(df.columns):
            st.subheader(f"📁 File: {file}")

            # Clean and convert dates
            df['Return_Date'] = pd.to_datetime(df['Return_Date'], errors='coerce')
            df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')

            # Basic Metrics
            total_return_qty = df['Return_Quantity'].sum()
            total_credit_amt = df['Total_Credit_Note_Amount'].sum()

            st.metric("Total Returned Quantity", int(total_return_qty))
            st.metric("Total Credit Note Amount", f"₹ {total_credit_amt:,.2f}")

            # --- Return Reason Summary ---
            reason_summary = df.groupby('Seller_Return_Reason')['Return_Quantity'].sum().reset_index().sort_values(
                by='Return_Quantity', ascending=False)
            st.subheader("🔄 Return Quantity by Seller Reason")
            st.dataframe(reason_summary)

            fig_reason = px.bar(reason_summary, x='Seller_Return_Reason', y='Return_Quantity',
                                title='Return Qty by Reason', text_auto=True)
            st.plotly_chart(fig_reason)

            # --- Return Type Summary ---
            type_summary = df.groupby('Return_Type')['Return_Quantity'].sum().reset_index()
            st.subheader("📦 Return Quantity by Return Type")
            fig_type = px.pie(type_summary, names='Return_Type', values='Return_Quantity',
                              title='Return Type Distribution')
            st.plotly_chart(fig_type)

            # --- Date-wise Return Summary ---
            daily_returns = df.groupby(df['Return_Date'].dt.date)['Return_Quantity'].sum().reset_index()
            daily_returns.columns = ['Return Date', 'Return Quantity']
            st.subheader("📅 Daily Return Summary")
            fig_daily = px.line(daily_returns, x='Return Date', y='Return Quantity', title='Return Quantity Over Time')
            st.plotly_chart(fig_daily)

            # --- SKU-wise Return Summary ---
            sku_summary = df.groupby('Child_Sku')['Return_Quantity'].sum().reset_index().sort_values(
                by='Return_Quantity', ascending=False).head(10)
            st.subheader("🏷️ Top 10 Returned SKUs")
            st.dataframe(sku_summary)

            # --- Downloadable Table ---
            export_cols = [
                'Client_Name', 'Client_Location', 'Marketplace', 'MP_Alias', 'Order_Date',
                'Return_Date', 'Parent_Sku', 'Child_Sku', 'Return_Quantity',
                'Return_Type', 'Seller_Return_Reason', 'Channel_return_reason',
                'Total_Credit_Note_Amount'
            ]
            export_df = df[export_cols].copy()
            st.subheader("📥 Download Returns Detailed Summary")
            csv = export_df.to_csv(index=False).encode('utf-8')
            st.download_button(f"Download Full Returns Data ({file})", csv, f"returns_summary_{file}.csv")



# --- SLA & TAT Tab ---
# --- SLA & TAT Tab ---
with tabs[2]:
    st.header("SLA & TAT")

    for file, df in dataframes.items():
        required_cols = ['TAT', 'Printed At', 'Handover At', 'Sales Channel', 'Order Date', 'Order Status']
        if all(col in df.columns for col in required_cols):
            st.subheader(f"SLA Summary – {file}")

            # Parse dates safely
            df['TAT'] = pd.to_datetime(df['TAT'], errors='coerce')
            df['Printed At'] = pd.to_datetime(df['Printed At'], errors='coerce')
            df['Handover At'] = pd.to_datetime(df['Handover At'], errors='coerce')
            df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')

            # Filter out unwanted statuses
            df = df[~df['Order Status'].isin(['Cancelled', 'Assigned'])]

            # TAT logic
            df['Processing TAT'] = (df['TAT'] - df['Printed At']).dt.days
            df['Handover TAT'] = (df['Handover At'] - df['Printed At']).dt.days

            # SLA flags
            df['SLA Processing Breach'] = df['Processing TAT'] < 0
            df['SLA Processing Within'] = ~df['SLA Processing Breach']
            df['Handover in SLA'] = df['Printed At'].dt.date == df['Handover At'].dt.date
            df['Handover breached'] = ~df['Handover in SLA']

            # Sales Channel Filter
            channels = df['Sales Channel'].dropna().unique()
            selected_channels = st.multiselect(f"Filter by Sales Channel ({file}):", channels, default=list(channels))
            df = df[df['Sales Channel'].isin(selected_channels)]

            # Add a date column for grouping
            df['Order_Date'] = df['Order Date'].dt.date

            # Perform groupby aggregation
            summary = df.groupby(['Order_Date', 'Sales Channel']).agg({
                'SLA Processing Within': 'sum',
                'SLA Processing Breach': 'sum',
                'Order Status': 'count',
                'Handover in SLA': 'sum',
                'Handover breached': 'sum'
            }).reset_index()

            # Rename columns
            summary = summary.rename(columns={
                'Sales Channel': 'Sales Channels',
                'SLA Processing Within': 'With in SLA',
                'SLA Processing Breach': 'SLA breached',
                'Order Status': 'No. of orders'
            })

            # If order quantity is another column, replace here; else duplicate from count
            summary['Order Quantity'] = summary['No. of orders']

            # Calculate SLA Breach %
            summary['SLA Breached %'] = round((summary['SLA breached'] / summary['No. of orders']) * 100, 2)

            # Final column order
            summary = summary[[
                'Order_Date', 'Sales Channels', 'With in SLA', 'SLA breached',
                'Order Quantity', 'No. of orders', 'Handover in SLA',
                'Handover breached', 'SLA Breached %'
            ]]

            # Display table
            st.dataframe(summary)

            # Bar chart
            fig = px.bar(summary, x='Sales Channels', y='SLA breached', color='Order_Date',
                         title='SLA Breaches by Channel and Date', barmode='group')
            st.plotly_chart(fig)

            # Download option
            csv = summary.to_csv(index=False).encode('utf-8')
            st.download_button(f"Download SLA Summary ({file})", csv, f"sla_summary_{file}.csv")




# --- Sales Overview Tab ---
with tabs[3]:
    st.header("Sales Overview")
    for file, df in dataframes.items():
        if 'gross_quantity_sold' in df.columns:
            st.subheader(f"Sales Summary – {file}")
            sales_summary = df.groupby('sku').agg(
                Gross_Sold=('gross_quantity_sold', 'sum'),
                Net_Sold=('net_quantity_sold', 'sum'),
                Returns=('returned_quantity', 'sum')
            ).sort_values(by='Gross_Sold', ascending=False).head(10).reset_index()

            st.dataframe(sales_summary)
            fig = px.bar(sales_summary, x='sku', y='Gross_Sold', title='Top 10 SKUs by Gross Sales')
            st.plotly_chart(fig)

            csv = sales_summary.to_csv(index=False).encode('utf-8')
            st.download_button(f"Download Sales Summary ({file})", csv, f"sales_{file}.csv")


# --- Claim Summary Tab ---
with tabs[4]:
    st.header("Claim Summary by Date")

    for file, df in dataframes.items():
        required_cols = {'Claim Date*', 'Name of Portal*', 'Claim Status*', 'Qty*'}
        if required_cols.issubset(df.columns):
            st.subheader(f"Claim Summary – {file}")

            # Handle missing or incorrect data
            df['Claim Date*'] = pd.to_datetime(df['Claim Date*'], errors='coerce')
            df = df.dropna(subset=['Claim Date*', 'Qty*'])
            df['Qty*'] = pd.to_numeric(df['Qty*'], errors='coerce')

            # Group by required columns
            grouped_df = df.groupby([
                df['Claim Date*'].dt.date,
                'Name of Portal*',
                'Claim Status*'
            ])['Qty*'].sum().reset_index()

            grouped_df.columns = ['Claim Date', 'Name of Portal', 'Claim Status', 'Total Qty']
            st.dataframe(grouped_df)

            # Plot
            fig = px.bar(grouped_df, x='Claim Date', y='Total Qty', color='Claim Status',
                         barmode='group', title='Claim Qty by Date and Status')
            st.plotly_chart(fig)

            # Download button
            csv = grouped_df.to_csv(index=False).encode('utf-8')
            st.download_button(f"Download Claim Summary ({file})", csv, f"claim_summary_{file}.csv")





# --- QC & GRN Tab ---
with tabs[5]:
    st.header("QC & GRN")
    grn_df, qc_df = None, None

    for file, df in dataframes.items():
        if {'GRN No', 'GRN Date'}.issubset(df.columns):
            grn_df = df.copy()
        if {'GRN', 'QC pass quantity', 'QC fail quantity(Missing data)'}.issubset(df.columns):
            qc_df = df.copy()

    if grn_df is not None and qc_df is not None:
        st.subheader("Merged QC + GRN Summary")

        grn_df['GRN No'] = grn_df['GRN No'].astype(str).str.strip()
        qc_df['GRN'] = qc_df['GRN'].astype(str).str.strip()

        merged = pd.merge(grn_df, qc_df, how='left', left_on='GRN No', right_on='GRN')

        if 'SKU' not in merged.columns:
            if 'SKU_x' in merged.columns:
                merged.rename(columns={'SKU_x': 'SKU'}, inplace=True)
            elif 'SKU_y' in merged.columns:
                merged.rename(columns={'SKU_y': 'SKU'}, inplace=True)

        merged['GRN Date'] = pd.to_datetime(merged['GRN Date'], errors='coerce')
        for col in ['PO Ordered Quantity', 'GRN Received Quantity', 'QC pass quantity', 'QC fail quantity(Missing data)']:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0)

        merged = merged.dropna(subset=['GRN Date', 'SKU'])

        summary = merged.groupby('GRN Date').agg(
            SKUs=('SKU', 'nunique'),
            PO_QUANTITY=('PO Ordered Quantity', 'sum'),     # renamed here
            GRN_QTY=('GRN Received Quantity', 'sum'),        # renamed here
            QC_Passed=('QC pass quantity', 'sum'),
            QC_Failed=('QC fail quantity(Missing data)', 'sum')
        ).reset_index()

        summary['Fulfillment %'] = (summary['GRN_QTY'] / summary['PO_QUANTITY']) * 100
        summary['QC Pass %'] = (summary['QC_Passed'] / summary['GRN_QTY']) * 100
        summary['QC Fail %'] = (summary['QC_Failed'] / summary['GRN_QTY']) * 100

        st.dataframe(summary.round(2))

        fig = px.line(
            summary,
            x='GRN Date',
            y=['Fulfillment %', 'QC Pass %', 'QC Fail %'],
            title='GRN Fulfillment and QC % Over Time'
        )
        st.plotly_chart(fig)

        csv = summary.to_csv(index=False).encode('utf-8')
        st.download_button("Download QC+GRN Summary", csv, "qc_grn_summary.csv")
    else:
        st.info("GRN or QC data not found in uploaded files.")
