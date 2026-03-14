import streamlit as st
import pandas as pd
import matplotlib.colors as mcolors
import numpy as np

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" # LeadNavigator Deep Purple

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
            .stApp {{ background-color: #FAFAFC; }} /* Extra subtle cool tint to match website */
            h1, h2, h3 {{ color: #0F172A !important; font-weight: 600 !important; }}
            [data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none !important; }}
            div[data-testid="stButton"] button {{ border-radius: 8px; font-weight: 500; padding: 0px 10px !important; }}
            div[data-testid="stButton"] button[kind="primary"] {{ background-color: {primary_color} !important; color: #FFFFFF !important; border: none; }}
            div[data-testid="stButton"] button[kind="secondary"] {{ background-color: #FFFFFF; color: {primary_color}; border: 1px solid #EBE4F4; }}
            .premium-table-container {{ border-radius: 12px; border: 1px solid #E2E8F0; background: #FFFFFF; overflow: hidden; margin-top: 1rem; margin-bottom: 2rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
            .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; }}
            .premium-table-container th {{ background-color: #F8F6FA !important; color: #4D148C !important; font-weight: 700 !important; text-align: center !important; padding: 15px 12px !important; border-bottom: 2px solid #EBE4F4 !important; text-transform: uppercase !important; font-size: 0.95rem !important; letter-spacing: 0.5px !important; }}
            .premium-table-container td {{ text-align: center !important; padding: 12px !important; border-bottom: 1px solid #F8FAFC !important; font-size: 0.85rem !important; }}
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; }}
        </style>
    """, unsafe_allow_html=True)

apply_custom_theme(PITCH_BRAND_COLOR)

# 🚨 THE NEW FIX: Ultra-subtle, premium SaaS gradient
brand_gradient = mcolors.LinearSegmentedColormap.from_list("brand_purple", ["#FFFFFF", "#FBF9FC", "#EBE4F4"])

# ================ 2. DATA ENGINE =================
@st.cache_data(show_spinner=False)
def clean_n8n_data(df):
    df = df.rename(columns=N8N_COLUMN_MAPPER)
    df.columns = [c.lower() for c in df.columns]
    if 'state_raw' in df.columns: df['region'] = df['state_raw'].str.strip().str.upper().map(STATE_TO_REGION).fillna('Unknown')
    if 'gender' in df.columns: df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female', 'Male': 'Male', 'Female': 'Female'}).fillna('Unknown')
    if 'marital_status' in df.columns:
        df['marital_status'] = df['marital_status'].map({'Y': 'Married', 'N': 'Single', 'Married': 'Married', 'Single': 'Single'}).fillna('Unknown')
    if 'zip_code' in df.columns: df['zip_code'] = df['zip_code'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)
    if 'personal_emails' in df.columns:
        df['email_match'] = df['personal_emails'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
        df = df.explode('email_match').reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False)
def clean_orders_data(df):
    email_col = next((c for c in df.columns if 'email' in c.lower()), 'Email')
    order_col = next((c for c in df.columns if 'name' in c.lower() or 'order' in c.lower()), 'Order ID')
    total_col = next((c for c in df.columns if 'total' in c.lower() or 'price' in c.lower()), 'Total')
    date_col = next((c for c in df.columns if 'created' in c.lower() or 'date' in c.lower()), 'Date')
    
    df = df.rename(columns={email_col: 'email_match', order_col: 'order_id', total_col: 'revenue_raw', date_col: 'order_date'})
    df['email_match'] = df['email_match'].astype(str).str.lower().str.strip()
    
    # 1. Clean currency and force blanks to 0
    df['revenue_raw'] = df['revenue_raw'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
    df['revenue_raw'] = pd.to_numeric(df['revenue_raw'], errors='coerce').fillna(0)
    
    # 2. Drop the zeros/blanks BEFORE doing anything else
    df = df[df['revenue_raw'] > 0]
    
    # 3. Handle dates
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', utc=True)
    df = df.dropna(subset=['order_date'])
    df['order_date'] = df['order_date'].dt.date
    
    return df.reset_index(drop=True)

def build_dashboard_views(orders_df, enriched_df, start_date, end_date):
    mask = (orders_df['order_date'] >= start_date) & (orders_df['order_date'] <= end_date)
    filtered_orders = orders_df.loc[mask]
    if filtered_orders.empty: return None
    
    # 🚨 Added logic: Calculate unique individual buyers in Shopify for the match rate
    unique_shopify_humans = filtered_orders['email_match'].nunique()
    
    purchasers = filtered_orders.groupby('email_match').agg(revenue=('revenue_raw', 'sum'), order_count=('order_id', 'nunique')).reset_index()
    df_joined = pd.merge(purchasers, enriched_df, on='email_match', how='inner').reset_index(drop=True)
    
    if df_joined.empty: return None
    
    total_rev = df_joined['revenue'].sum()
    
    # 🚨 Added logic: Calculate the match rate
    matched_count = df_joined['email_match'].nunique()
    match_rate = (matched_count / unique_shopify_humans * 100) if unique_shopify_humans > 0 else 0
    
    summary_vars = [("Gender", "gender"), ("Age", "age"), ("Marital Status", "marital_status"), ("Region", "region"), ("State", "state_raw"), ("Zip Code", "zip_code"), ("Income", "income")]
    top_perf = {}
    all_html_views = {}
    
    for label, col_key in summary_vars:
        if col_key in df_joined.columns:
            valid_rows = df_joined[~df_joined[col_key].astype(str).str.lower().isin(['unknown', 'nan', 'u', 'none', '00nan', '', 'null'])]
            if not valid_rows.empty:
                var_total = valid_rows['revenue'].sum()
                rs = valid_rows.groupby(col_key)['revenue'].sum()
                top_perf[label] = (rs.idxmax(), (rs.max() / var_total * 100))
                grp = valid_rows.groupby(col_key).agg(Purchasers=('email_match', 'nunique'), Revenue=('revenue', 'sum')).reset_index()
                grp['% of Buyers'] = (grp['Revenue'] / var_total) * 100
                grp['Rev / Purchaser'] = (grp['Revenue'] / grp['Purchasers'])
                final_v = grp.rename(columns={col_key: label.upper()}).sort_values('Revenue', ascending=False)
                if label == "Zip Code": final_v = final_v.head(100)
                styler = final_v.style.format({'Purchasers': '{:,.0f}', 'Revenue': '${:,.2f}', '% of Buyers': '{:.1f}%', 'Rev / Purchaser': '${:,.2f}'}).background_gradient(subset=['Revenue', '% of Buyers'], cmap=brand_gradient)
                all_html_views[label] = styler.hide(axis="index").to_html()
                
    # 🚨 Updated return statement to include the new match metrics
    return {
        "total_revenue": total_rev, 
        "total_buyers": matched_count, 
        "unique_shopify_customers": unique_shopify_humans,
        "match_rate": match_rate,
        "top_performers": top_perf, 
        "html_views": all_html_views
    }

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: 
    st.session_state.app_state = "onboarding"
    st.session_state.orders_vault = []
    st.session_state.n8n_vault = []

if st.session_state.app_state == "onboarding":
    st.markdown(f"<h1 style='text-align: center; font-size: 3rem; margin-top: 50px;'>🧭 {PITCH_COMPANY_NAME} Engine</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #64748B;'>Upload Shopify and Enriched Data files to begin.</p>", unsafe_allow_html=True)
    _, col1, col2, _ = st.columns([1, 2, 2, 1])
    with col1:
        st.subheader("🛒 Shopify Orders")
        st.session_state.orders_vault = st.file_uploader("Add Shopify CSVs", type=["csv"], accept_multiple_files=True, key="order_up")
    with col2:
        st.subheader("🧬 Enriched Data")
        st.session_state.n8n_vault = st.file_uploader("Add n8n CSVs", type=["csv"], accept_multiple_files=True, key="n8n_up")
    st.markdown("<br>", unsafe_allow_html=True)
    _, center_col, _ = st.columns([2, 1, 2])
    
    if center_col.button("🚀 Run Analysis", type="primary", use_container_width=True):
        if not st.session_state.orders_vault or not st.session_state.n8n_vault:
            st.error("Please upload at least one of each file type.")
        else:
            with st.spinner("Deduplicating & Processing Data..."):
                # 🚨 New Logic: Process Orders and count all the duplicates and blanks
                raw_df = pd.concat([pd.read_csv(f, encoding='latin1', on_bad_lines='skip') for f in st.session_state.orders_vault], ignore_index=True)
                raw_count = len(raw_df)
                
                # Clean: This converts blanks to 0 and removes them
                cleaned_step_1 = clean_orders_data(raw_df)
                st.session_state.zero_rev_count = raw_count - len(cleaned_step_1)
                
                # Deduplicate: Remove the multi-line line items
                unique_orders_df = cleaned_step_1.drop_duplicates(subset=['order_id'])
                st.session_state.line_item_dupes = len(cleaned_step_1) - len(unique_orders_df)
                
                # Identify Repeat Customers (Same email buying different Order IDs)
                unique_customers_count = unique_orders_df['email_match'].nunique()
                st.session_state.repeat_orders = len(unique_orders_df) - unique_customers_count
                
                # Final Vault storage
                st.session_state.cleaned_orders = unique_orders_df
                
                # Process Enriched Data
                all_n8n = pd.concat([pd.read_csv(f, encoding='latin1', on_bad_lines='skip') for f in st.session_state.n8n_vault], ignore_index=True)
                st.session_state.cleaned_n8n = clean_n8n_data(all_n8n).drop_duplicates(subset=['email_match'])
                
                st.session_state.min_date, st.session_state.max_date = st.session_state.cleaned_orders['order_date'].min(), st.session_state.cleaned_orders['order_date'].max()
                st.session_state.current_start, st.session_state.current_end = st.session_state.min_date, st.session_state.max_date
                st.session_state.app_state = "dashboard"
                st.rerun()

elif st.session_state.app_state == "dashboard":
    c1, c2, c3 = st.columns([1, 3, 1])
    if c1.button("🔄 Start Over"): 
        st.session_state.app_state = "onboarding"
        st.rerun()
    with c2:
        selected_dates = st.slider("Filter by Purchase Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, value=(st.session_state.current_start, st.session_state.current_end), format="MMM DD, YYYY")
    
    if (selected_dates[0] != st.session_state.current_start) or (selected_dates[1] != st.session_state.current_end) or ("dash_data" not in st.session_state):
        st.session_state.current_start, st.session_state.current_end = selected_dates[0], selected_dates[1]
        st.session_state.dash_data = build_dashboard_views(st.session_state.cleaned_orders, st.session_state.cleaned_n8n, selected_dates[0], selected_dates[1])
    
    dash_data = st.session_state.dash_data
    if dash_data:
        # 1. MACRO METRICS (DESCRIPTIVE)
        m1, m2 = st.columns(2)

        with m1:
            # 🚨 New Logic: The custom HTML card with the exact requested text
            st.markdown(f"""
                <div style="background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
                    <p style="margin: 0; font-size: 0.9rem; color: #64748B;">Resolved Customers</p>
                    <h2 style="margin: 10px 0; font-size: 3rem; color: #0F172A;">{dash_data['total_buyers']:,.0f}</h2>
                    <p style="margin: 0; font-size: 0.9rem; color: #1e293b; font-weight: 500;">
                        Identified <b>{dash_data['unique_shopify_customers']:,.0f}</b> individual customers and matched <b>{dash_data['total_buyers']:,.0f} ({dash_data['match_rate']:.1f}%)</b>.
                    </p>
                    <hr style="margin: 15px 0; border: 0; border-top: 1px solid #f1f5f9;">
                    <p style="margin: 0; font-size: 0.75rem; color: #94A3B8; line-height: 1.4; text-align: left;">
                        • Scrubbed <b>{st.session_state.zero_rev_count}</b> empty or $0 line items.<br>
                        • Collapsed <b>{st.session_state.line_item_dupes}</b> multi-line item rows.<br>
                        • Grouped <b>{st.session_state.repeat_orders}</b> repeat purchases from the same buyers.
                    </p>
                </div>
            """, unsafe_allow_html=True)
        with m2:
            st.markdown(f"""<div style="background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05); height: 100%; display: flex; flex-direction: column; justify-content: center;"><p style="margin: 0; font-size: 0.9rem; color: #64748B;">Attributed Sales</p><h2 style="margin: 10px 0; font-size: 3rem; color: #0F172A;">${dash_data['total_revenue']:,.2f}</h2></div>""", unsafe_allow_html=True)
        
        # 2. TOP PERFORMERS
       # 2. TOP PERFORMERS (Polished Subheading)
        st.markdown("""
            <h2 style="
                margin-top: 1.5rem; 
                margin-bottom: 0.5rem; 
                color: #0F172A; 
                font-weight: 700; 
                font-size: 1.8rem;
                display: flex;
                align-items: center;
                gap: 10px;
            ">
                <span style="font-size: 2rem;">🏆</span> Top Performing Demographics
            </h2>
        """, unsafe_allow_html=True)
        
        summary_cols = st.columns(len(dash_data['top_performers']))
        for i, (label, data) in enumerate(dash_data['top_performers'].items()):
            with summary_cols[i]:
                st.markdown(f'<div style="background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 15px; text-align: center; min-height: 120px; display: flex; flex-direction: column; justify-content: center;"><p style="margin: 0; font-size: 0.8rem; color: #64748B; font-weight: 600; text-transform: uppercase;">{label}</p><p style="margin: 5px 0; font-size: 1.1rem; color: #0F172A; font-weight: 700; line-height: 1.2;">{data[0]}</p><p style="margin: 0; font-size: 0.85rem; color: #16A34A; background-color: #F0FDF4; border-radius: 20px; padding: 2px 8px; display: inline-block; align-self: center;">{data[1]:.1f}% of Revenue</p></div>', unsafe_allow_html=True)
        
        # 3. DEEP DIVE (Polished Subheading)
        st.markdown("""
            <h2 style="
                margin-top: 2rem; 
                margin-bottom: 0.5rem; 
                color: #0F172A; 
                font-weight: 700; 
                font-size: 1.8rem;
                display: flex;
                align-items: center;
                gap: 10px;
            ">
                <span style="font-size: 2rem;">🔍</span> Audience Deep Dive
            </h2>
        """, unsafe_allow_html=True)
        
        if "active_var" not in st.session_state: st.session_state.active_var = "Gender"
        if "active_loc_level" not in st.session_state: st.session_state.active_loc_level = "Region"
        v_labels = ["Gender", "Age", "Location", "Marital Status", "Income"]
        var_cols = st.columns(len(v_labels))
        for i, label in enumerate(v_labels):
            if var_cols[i].button(label, key=f"btn_{label}", type="primary" if st.session_state.active_var == label else "secondary", use_container_width=True):
                st.session_state.active_var = label; st.rerun()
        
        lk = st.session_state.active_var
        if lk == "Location":
            l1, l2, l3, _ = st.columns([1, 1, 1, 5])
            if l1.button("Region", type="primary" if st.session_state.active_loc_level == "Region" else "secondary"): st.session_state.active_loc_level = "Region"; st.rerun()
            if l2.button("State", type="primary" if st.session_state.active_loc_level == "State" else "secondary"): st.session_state.active_loc_level = "State"; st.rerun()
            if l3.button("Zip Code", type="primary" if st.session_state.active_loc_level == "Zip Code" else "secondary"): st.session_state.active_loc_level = "Zip Code"; st.rerun()
            lk = st.session_state.active_loc_level

        if lk in dash_data['html_views']:
            st.markdown(f'<div class="premium-table-container">{dash_data["html_views"][lk]}</div>', unsafe_allow_html=True)
