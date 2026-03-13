import streamlit as st
import pandas as pd
import matplotlib.colors as mcolors
import numpy as np

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#0F172A" # Sleek LeadNavigator Navy

N8N_COLUMN_MAPPER = {
    "GENDER": "gender",
    "MARRIED": "marital_status",
    "AGE_RANGE": "age",
    "INCOME_RANGE": "income",
    "PERSONAL_STATE": "state_raw",
    "PERSONAL_ZIP": "zip_code"
}

STATE_TO_REGION = {
    'CT':'Northeast','MA':'Northeast','ME':'Northeast','NH':'Northeast','NJ':'Northeast','NY':'Northeast','PA':'Northeast','RI':'Northeast','VT':'Northeast',
    'IA':'Midwest','IL':'Midwest','IN':'Midwest','KS':'Midwest','MI':'Midwest','MN':'Midwest','MO':'Midwest','ND':'Midwest','NE':'Midwest','OH':'Midwest','SD':'Midwest','WI':'Midwest',
    'AL':'South','AR':'South','DC':'South','DE':'South','FL':'South','GA':'South','KY':'South','LA':'South','MD':'South','MS':'South','NC':'South','OK':'South','SC':'South','TN':'South','TX':'South','VA':'South','WV':'South',
    'AK':'West','AZ':'West','CA':'West','CO':'West','HI':'West','ID':'West','MT':'West','NM':'West','NV':'West','OR':'West','UT':'West','WA':'West','WY':'West'
}

st.set_page_config(page_title=f"{PITCH_COMPANY_NAME} | Audience Engine", page_icon="🧭", layout="wide", initial_sidebar_state="collapsed")

def apply_custom_theme(primary_color):
    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
            html, body, [class*="css"] {{ font-family: 'Outfit', sans-serif; }}
            .stApp {{ background-color: #F8FAFC; }} /* Light slate background */
            h1, h2, h3 {{ color: #0F172A !important; font-weight: 600 !important; }}
            
            /* Hide the sidebar */
            [data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none !important; }}

            div[data-testid="stButton"] button {{ border-radius: 8px; font-weight: 500; padding: 0px 10px !important; }}
            div[data-testid="stButton"] button[kind="primary"] {{ background-color: {primary_color} !important; color: #FFFFFF !important; border: none; }}
            div[data-testid="stButton"] button[kind="secondary"] {{ background-color: #FFFFFF; color: #0F172A; border: 1px solid #CBD5E1; }}
            
            [data-testid="stMetric"] {{ background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
            
            /* Remove up/down arrows from metrics */
            [data-testid="stMetricDelta"] svg {{ display: none !important; }}
            [data-testid="stMetricDelta"] div {{ margin-left: 0 !important; }}

            .premium-table-container {{ border-radius: 12px; border: 1px solid #E2E8F0; background: #FFFFFF; overflow: hidden; margin-top: 1rem; margin-bottom: 2rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
            .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; }}
            .premium-table-container th {{ background-color: #F1F5F9 !important; color: #334155 !important; font-weight: 700 !important; text-align: center !important; padding: 12px !important; border-bottom: 2px solid #CBD5E1 !important; text-transform: uppercase !important; font-size: 0.75rem !important; }}
            .premium-table-container td {{ text-align: center !important; padding: 12px !important; border-bottom: 1px solid #F8FAFC !important; font-size: 0.85rem !important; }}
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; }}
        </style>
    """, unsafe_allow_html=True)

apply_custom_theme(PITCH_BRAND_COLOR)
# Keeping the requested "Heavenly Heat" style green gradient for the tables
custom_light_green = mcolors.LinearSegmentedColormap.from_list("custom_green", ["#F9F7F3", "#D1E5D1", "#6EAB6E"])

# ================ 2. DATA ENGINE =================
@st.cache_data(show_spinner=False)
def clean_n8n_data(df):
    """Standardizes the messy n8n API payload"""
    df = df.rename(columns=N8N_COLUMN_MAPPER)
    df.columns = [c.lower() for c in df.columns]
    
    if 'state_raw' in df.columns: df['region'] = df['state_raw'].str.strip().str.upper().map(STATE_TO_REGION).fillna('Unknown')
    if 'gender' in df.columns: df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female', 'Male': 'Male', 'Female': 'Female'}).fillna('Unknown')
    if 'marital_status' in df.columns: df['marital_status'] = df['marital_status'].fillna('Unknown')
    if 'zip_code' in df.columns: df['zip_code'] = df['zip_code'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)
    
    # 🚨 Explode the comma-separated emails so any burner email matches the Shopify order
    if 'personal_emails' in df.columns:
        df['email_match'] = df['personal_emails'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
        df = df.explode('email_match').reset_index(drop=True)
        df = df.drop_duplicates(subset=['email_match']).reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False)
def clean_orders_data(df):
    """Standardizes the Shopify Orders and scrubs $0 transactions"""
    # Auto-detect standard Shopify columns or fallback to our generated template
    email_col = next((c for c in df.columns if 'email' in c.lower()), 'Email')
    order_col = next((c for c in df.columns if 'name' in c.lower() or 'order' in c.lower()), 'Order ID')
    total_col = next((c for c in df.columns if 'total' in c.lower() or 'price' in c.lower()), 'Total')
    date_col = next((c for c in df.columns if 'created' in c.lower() or 'date' in c.lower()), 'Date')
    
    df = df.rename(columns={email_col: 'email_match', order_col: 'order_id', total_col: 'revenue_raw', date_col: 'order_date'})
    df['email_match'] = df['email_match'].astype(str).str.lower().str.strip()
    
    # Clean the currency string into a math-ready float
    df['revenue_raw'] = pd.to_numeric(df['revenue_raw'].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
    
    # 🚨 THE NEW FIX: Drop any order that is $0.00 (spare parts, free replacements)
    df = df[df['revenue_raw'] > 0]
    
    # Bulletproof date handling for real Shopify timezone formats
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', utc=True)
    df = df.dropna(subset=['order_date']) # Drop anything that failed to parse
    df['order_date'] = df['order_date'].dt.date # Safely extract just the YYYY-MM-DD
    
    return df.reset_index(drop=True)
    
    return df
def build_dashboard_views(orders_df, enriched_df, start_date, end_date):
    """Zero-Lag Engine: Filters dates, aggregates purchasers, and pre-renders HTML tables."""
    # 1. Filter orders by selected date range
    mask = (orders_df['order_date'] >= start_date) & (orders_df['order_date'] <= end_date)
    filtered_orders = orders_df.loc[mask]
    
    if filtered_orders.empty: return None

    # 2. Aggregation: Group by Email to create 1 "Purchaser" from multiple orders
    purchasers = filtered_orders.groupby('email_match').agg(
        revenue=('revenue_raw', 'sum'),
        order_count=('order_id', 'nunique')
    ).reset_index()

    # 3. Merge: Match the aggregated purchasers with their n8n demographic data
    df_joined = pd.merge(purchasers, enriched_df, on='email_match', how='inner').reset_index(drop=True)
    if df_joined.empty: return None

    total_rev = df_joined['revenue'].sum()
    total_buyers = df_joined['email_match'].nunique()
    
    summary_vars = [("Gender", "gender"), ("Age", "age"), ("Marital Status", "marital_status"), ("Region", "region"), ("State", "state_raw"), ("Zip Code", "zip_code"), ("Income", "income")]
    
    top_perf = {}
    all_html_views = {}

    # 4. Pre-calculate all top performers and HTML tables for instant UI swapping
    for label, col_key in summary_vars:
        if col_key in df_joined.columns:
            temp = df_joined[~df_joined[col_key].astype(str).str.lower().isin(['unknown', 'nan', 'u', 'none', '00nan', ''])]
            if not temp.empty:
                # Calculate the Top Performer metric
                rs = temp.groupby(col_key)['revenue'].sum()
                top_perf[label] = (rs.idxmax(), (rs.max() / total_rev * 100) if total_rev > 0 else 0)
                
                # Build the Data Table
                grp = temp.groupby(col_key).agg(Purchasers=('email_match', 'nunique'), Revenue=('revenue', 'sum')).reset_index()
                grp['% of Buyers'] = (grp['Purchasers'] / grp['Purchasers'].sum()) * 100
                grp['Rev / Purchaser'] = (grp['Revenue'] / grp['Purchasers'])
                
                final_v = grp.rename(columns={col_key: label.upper()}).sort_values('Revenue', ascending=False)
                if label == "Zip Code": final_v = final_v.head(100) # Cap rows for performance
                
                # Render to styled HTML string instantly
                styler = final_v.style.format({'Purchasers': '{:,.0f}', 'Revenue': '${:,.2f}', '% of Buyers': '{:.1f}%', 'Rev / Purchaser': '${:,.2f}'}).background_gradient(subset=['Revenue', '% of Buyers'], cmap=custom_light_green)
                all_html_views[label] = styler.hide(axis="index").to_html()

    return {
        "total_revenue": total_rev,
        "total_buyers": total_buyers,
        "top_performers": top_perf,
        "html_views": all_html_views
    }

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: st.session_state.app_state = "onboarding"

if st.session_state.app_state == "onboarding":
    st.markdown(f"<h1 style='text-align: center; font-size: 3rem; margin-top: 50px;'>🧭 {PITCH_COMPANY_NAME} Engine</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #64748B;'>Upload your Shopify Orders and Demographic Data to begin analysis.</p>", unsafe_allow_html=True)
    
    _, col1, col2, _ = st.columns([1, 2, 2, 1])
    with col1:
        orders_file = st.file_uploader("1. Upload Shopify Orders (CSV)", type=["csv"])
    with col2:
        n8n_file = st.file_uploader("2. Upload Enriched Data (CSV)", type=["csv"])
        
    if orders_file and n8n_file:
        with st.spinner("Resolving Profiles & Cleaning Data..."):
            raw_orders = pd.read_csv(orders_file, encoding='latin1', on_bad_lines='skip')
            raw_n8n = pd.read_csv(n8n_file, encoding='latin1', on_bad_lines='skip')
            
            st.session_state.cleaned_orders = clean_orders_data(raw_orders)
            st.session_state.cleaned_n8n = clean_n8n_data(raw_n8n)
            
            # Identify the full date range available in the Shopify file
            st.session_state.min_date = st.session_state.cleaned_orders['order_date'].min()
            st.session_state.max_date = st.session_state.cleaned_orders['order_date'].max()
            
            st.session_state.current_start = st.session_state.min_date
            st.session_state.current_end = st.session_state.max_date
            
            st.session_state.app_state = "dashboard"
            st.rerun()

elif st.session_state.app_state == "dashboard":
    
# --- HEADER & DATE CONTROLS ---
    c1, c2, c3 = st.columns([1, 3, 1])
    if c1.button("🔄 Start Over"): 
        st.session_state.app_state = "onboarding"
        st.rerun()
        
    with c2:
        # 🚨 THE SLIDER IS BACK: Instant visual date scrubbing
        selected_dates = st.slider(
            "Filter by Purchase Date",
            min_value=st.session_state.min_date,
            max_value=st.session_state.max_date,
            value=(st.session_state.current_start, st.session_state.current_end),
            format="MMM DD, YYYY"
        )
    
    # 🚨 Core Engine Trigger: Only rebuild tables if the date slider actually moved
    if (selected_dates[0] != st.session_state.current_start) or (selected_dates[1] != st.session_state.current_end) or ("dash_data" not in st.session_state):
        st.session_state.current_start = selected_dates[0]
        st.session_state.current_end = selected_dates[1]
        st.session_state.dash_data = build_dashboard_views(
            st.session_state.cleaned_orders, 
            st.session_state.cleaned_n8n, 
            selected_dates[0], 
            selected_dates[1]
        )
    
    dash_data = st.session_state.dash_data

    if not dash_data:
        st.warning("No matched profiles found within this specific date range. Try widening the dates.")
    else:
        # 1. MACRO METRICS
        m1, m2 = st.columns(2)
        m1.metric("Resolved Profiles", f"{dash_data['total_buyers']:,.0f}")
        m2.metric("Attributed Sales", f"${dash_data['total_revenue']:,.2f}")
        st.markdown("<br>", unsafe_allow_html=True)

        # 2. TOP PERFORMING DEMOGRAPHICS
        st.markdown("### 🏆 Top Performing Demographics")
        summary_cols = st.columns(len(dash_data['top_performers']))
        
        for i, (label, data) in enumerate(dash_data['top_performers'].items()):
            with summary_cols[i]:
                # We use a custom HTML div to handle long strings like Income better than st.metric
                st.markdown(f"""
                    <div style="
                        background-color: #FFFFFF; 
                        border: 1px solid #E2E8F0; 
                        border-radius: 12px; 
                        padding: 15px; 
                        text-align: center; 
                        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
                        min-height: 120px;
                        display: flex;
                        flex-direction: column;
                        justify-content: center;
                    ">
                        <p style="margin: 0; font-size: 0.8rem; color: #64748B; font-weight: 600; text-transform: uppercase;">{label}</p>
                        <p style="margin: 5px 0; font-size: 1.1rem; color: #0F172A; font-weight: 700; line-height: 1.2;">{data[0]}</p>
                        <p style="margin: 0; font-size: 0.85rem; color: #16A34A; background-color: #F0FDF4; border-radius: 20px; padding: 2px 8px; display: inline-block; align-self: center;">{data[1]:.1f}% of Revenue</p>
                    </div>
                """, unsafe_allow_html=True)

        # 3. SINGLE VARIABLE DEEP DIVE (Zero Lag UI)
        st.markdown("### 🔍 Audience Deep Dive")
        if "active_var" not in st.session_state: st.session_state.active_var = "Gender"
        if "active_loc_level" not in st.session_state: st.session_state.active_loc_level = "Region"
        
        v_labels = ["Gender", "Age", "Location", "Marital Status", "Income"]
        var_cols = st.columns(len(v_labels))
        for i, label in enumerate(v_labels):
            if var_cols[i].button(label, key=f"btn_{label}", type="primary" if st.session_state.active_var == label else "secondary", use_container_width=True):
                st.session_state.active_var = label; st.rerun()

        lookup_key = st.session_state.active_var
        if st.session_state.active_var == "Location":
            st.markdown("<br>", unsafe_allow_html=True)
            l1, l2, l3, _ = st.columns([1, 1, 1, 5])
            if l1.button("Region", type="primary" if st.session_state.active_loc_level == "Region" else "secondary"): st.session_state.active_loc_level = "Region"; st.rerun()
            if l2.button("State", type="primary" if st.session_state.active_loc_level == "State" else "secondary"): st.session_state.active_loc_level = "State"; st.rerun()
            if l3.button("Zip Code", type="primary" if st.session_state.active_loc_level == "Zip Code" else "secondary"): st.session_state.active_loc_level = "Zip Code"; st.rerun()
            lookup_key = st.session_state.active_loc_level

        # Render the exact pre-calculated HTML table
        if lookup_key in dash_data['html_views']:
            st.markdown(f'<div class="premium-table-container">{dash_data["html_views"][lookup_key]}</div>', unsafe_allow_html=True)
        else:
            st.info(f"Not enough data to calculate {lookup_key} for this date range.")
