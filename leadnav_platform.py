import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools
import re
import requests
from datetime import datetime, timedelta
 
# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator"
PITCH_BRAND_COLOR = "#4D148C"
N8N_WEBHOOK_URL = "https://your-n8n-instance.com/webhook/order-enrichment"
 
# BigQuery table references
BQ_B2C_VISITOR_TABLE = "leadnav-hhs.leadnav_platform.b2c_visitor_summary"
BQ_B2B_VISITOR_TABLE = "leadnav-hhs.leadnav_platform.b2b_visitor_summary"
BQ_ORDERS_TABLE = "leadnav-hhs.leadnav_platform.platform_order_data"
 
EXCLUDE_LIST = ['Unknown', 'UNKNOWN', 'U', '', 'None', 'NONE', 'nan', 'NaN', 'null', 'NULL', '<NA>', 'ALL']
 
st.set_page_config(page_title=f"{PITCH_COMPANY_NAME} | Conversion Engine", page_icon="🧭", layout="wide", initial_sidebar_state="collapsed")
 
def apply_custom_theme(primary_color):
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Playfair+Display:ital,wght@0,600;0,700;0,800;1,600&display=swap');
        html, body, [class*="css"] {{ font-family: 'Outfit', sans-serif; }}
        .stApp {{ background-color: #FAFAFC; }}
        [data-testid="stHeader"] {{ display: none !important; }}
        #MainMenu {{ visibility: hidden; }}
        footer {{ visibility: hidden; }}
        [data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none !important; }}
 
        .premium-table-container {{ width: 100% !important; border-radius: 12px; border: 1px solid {primary_color}; background: #FFFFFF; overflow: hidden; margin-top: 1rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
        .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; border: none !important; }}
        .premium-table-container th {{ font-family: 'Outfit', sans-serif !important; background-color: #F8F6FA !important; color: {primary_color} !important; font-weight: 700 !important; text-align: center !important; padding: 15px 12px !important; border-bottom: 2px solid {primary_color} !important; font-size: 0.95rem !important; }}
        .premium-table-container td {{ font-family: 'Outfit', sans-serif !important; text-align: center !important; padding: 12px !important; border-bottom: 1px solid #EBE4F4 !important; font-size: 0.9rem !important; color: #1e293b !important; }}
        .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; }}
 
        .serif-gradient-centerpiece {{ font-family: 'Playfair Display', serif !important; background: linear-gradient(90deg, #4D148C 0%, #20B2AA 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: inline-block; font-weight: 700 !important; letter-spacing: -0.5px; }}
        .modern-serif-title {{ font-family: 'Playfair Display', serif !important; color: #0F172A !important; font-weight: 700 !important; }}
 
        .auth-box {{ max-width: 400px; margin: 100px auto; padding: 30px; background: white; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); border: 1px solid #EBE4F4; }}
        .header-bar {{ display: flex; justify-content: space-between; align-items: center; padding: 1rem 0; margin-bottom: 2rem; border-bottom: 1px solid #EBE4F4; }}
        .header-logo {{ font-family: 'Playfair Display', serif !important; font-size: 1.5rem; font-weight: 700; color: #0F172A; }}
    </style>
""", unsafe_allow_html=True)
 
apply_custom_theme(PITCH_BRAND_COLOR)
brand_gradient = mcolors.LinearSegmentedColormap.from_list("brand_purple", ["#FFFFFF", "#FBF9FC", "#EBE4F4"])
 
def render_premium_table(styler_obj):
    try: styler_obj = styler_obj.hide(axis="index")
    except AttributeError: styler_obj = styler_obj.hide_index()
    st.markdown(f'<div class="premium-table-container">{styler_obj.to_html()}</div>', unsafe_allow_html=True)
 
# ================ 2. BIGQUERY CLIENT =================
@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])
 
# ================ 3. DATA LOADING FUNCTIONS =================
@st.cache_data(ttl=3600)
def load_visitor_base(pixel_id, tenant_type):
    """Load visitor summary data from BigQuery. No cleaning needed - BQ tables already have clean bucketed values."""
    try:
        client = get_bq_client()
 
        if tenant_type == "B2C":
            query = f"""
            SELECT pixel_id, visit_date, total_visitors, state, gender, age_range, income_bucket,
                   net_worth_bucket, homeowner, marital_status, children
            FROM `{BQ_B2C_VISITOR_TABLE}`
            WHERE pixel_id = @pixel_id
            """
        else:
            query = f"""
            SELECT pixel_id, visit_date, total_visitors, industry, employee_count_range, job_title,
                   seniority, company_revenue
            FROM `{BQ_B2B_VISITOR_TABLE}`
            WHERE pixel_id = @pixel_id
            """
 
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pixel_id", "STRING", pixel_id)]
        )
 
        df = client.query(query, job_config=job_config).to_dataframe()
 
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), "No data found"
 
        # Parse date and cast total_visitors to numeric
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        df['total_visitors'] = pd.to_numeric(df['total_visitors'], errors='coerce').fillna(0)
 
        # Select appropriate columns based on tenant type
        if tenant_type == "B2C":
            df_demo = df[['pixel_id', 'visit_date', 'total_visitors', 'gender', 'age_range',
                          'marital_status', 'children', 'homeowner', 'income_bucket', 'net_worth_bucket']]
            df_state = df[['visit_date', 'state', 'total_visitors']].groupby(['visit_date', 'state'])['total_visitors'].sum().reset_index()
        else:
            df_demo = df[['pixel_id', 'visit_date', 'total_visitors', 'industry', 'employee_count_range',
                          'job_title', 'seniority', 'company_revenue']]
            df_state = pd.DataFrame()
 
        return df_demo, df_state, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)
 
@st.cache_data(ttl=3600)
def load_order_base(pixel_id, tenant_type):
    """Load order data from BigQuery. No cleaning needed - BQ tables already have clean bucketed values."""
    try:
        client = get_bq_client()
 
        query = f"""
        SELECT pixel_id, order_id, order_date, customer_email, revenue, lineitem_name, state,
               gender, age_range, income_bucket, net_worth_bucket, homeowner, marital_status, children,
               company_name, company_industry, employee_count_range, job_title, seniority, company_revenue
        FROM `{BQ_ORDERS_TABLE}`
        WHERE pixel_id = @pixel_id
        """
 
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pixel_id", "STRING", pixel_id)]
        )
 
        df = client.query(query, job_config=job_config).to_dataframe()
 
        if df.empty:
            return pd.DataFrame(), None
 
        # Parse date and rename columns
        df['order_date'] = pd.to_datetime(df['order_date'])
        df = df.rename(columns={'order_id': 'Order_ID', 'revenue': 'Total'})
 
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)
 
# ================ 4. SESSION STATE INITIALIZATION =================
if 'app_state' not in st.session_state:
    st.session_state.app_state = 'login'
if 'pixel_id' not in st.session_state:
    st.session_state.pixel_id = None
if 'tenant_type' not in st.session_state:
    st.session_state.tenant_type = None
if 'username' not in st.session_state:
    st.session_state.username = None
 
# ================ 5. HEADER WITH LOGO & LOGOUT =================
def render_header():
    col1, col2 = st.columns([1, 20])
    with col1:
        st.markdown(f'<div class="header-logo">Lead<span style="color: {PITCH_BRAND_COLOR};">Navigator</span></div>', unsafe_allow_html=True)
    with col2:
        if st.button("Logout", key="logout_btn"):
            st.session_state.app_state = 'login'
            st.session_state.pixel_id = None
            st.session_state.tenant_type = None
            st.session_state.username = None
            st.rerun()
 
# ================ 6. LOGIN PAGE =================
def login_page():
    st.markdown('<div class="auth-box">', unsafe_allow_html=True)
    st.markdown(f'<h1 class="modern-serif-title" style="text-align: center;">Welcome to {PITCH_COMPANY_NAME}</h1>', unsafe_allow_html=True)
 
    username = st.text_input("Username", key="login_username")
    password = st.text_input("Password", type="password", key="login_password")
 
    if st.button("Login"):
        users = dict(st.secrets.get("users", {}))
        if username in users and users[username].get("password") == password:
            st.session_state.username = username
            st.session_state.pixel_id = users[username].get("pixel_id")
            st.session_state.tenant_type = users[username].get("tenant_type")
            st.session_state.app_state = 'onboarding'
            st.rerun()
        else:
            st.error("Invalid username or password")
 
    st.markdown('</div>', unsafe_allow_html=True)
 
# ================ 7. ONBOARDING PAGE =================
def onboarding_page():
    render_header()
    st.markdown(f'<h2 class="modern-serif-title">🔄 Sync Database</h2>', unsafe_allow_html=True)
    st.markdown("Click the button below to load your data from BigQuery and proceed to the dashboard.")
 
    if st.button("Load Data & Enter Dashboard"):
        with st.spinner("Loading data from BigQuery..."):
            df_demo, df_state, error = load_visitor_base(st.session_state.pixel_id, st.session_state.tenant_type)
            df_orders, order_error = load_order_base(st.session_state.pixel_id, st.session_state.tenant_type)
 
            if error:
                st.error(f"Error loading visitor data: {error}")
            elif order_error:
                st.error(f"Error loading order data: {order_error}")
            else:
                st.session_state.df_demo = df_demo
                st.session_state.df_state = df_state
                st.session_state.df_orders = df_orders
                st.session_state.app_state = 'dashboard'
                st.rerun()
 
# ================ 8. DASHBOARD PAGE =================
def dashboard_page():
    render_header()
 
    tenant_type = st.session_state.tenant_type
    pixel_id = st.session_state.pixel_id
 
    # Set configs and demo columns based on tenant type
    if tenant_type == "B2C":
        configs = [
            ("Gender", "gender"),
            ("Age", "age_range"),
            ("Income", "income_bucket"),
            ("State", "state"),
            ("Net Worth", "net_worth_bucket"),
            ("Children", "children"),
            ("Marital Status", "marital_status"),
            ("Homeowner", "homeowner"),
        ]
        DEMO_COLS = ['gender', 'age_range', 'marital_status', 'children', 'homeowner', 'income_bucket', 'net_worth_bucket']
    else:
        configs = [
            ("Industry", "industry"),
            ("Company Size", "employee_count_range"),
            ("Job Title", "job_title"),
            ("Seniority", "seniority"),
            ("Revenue", "company_revenue"),
        ]
        DEMO_COLS = ['industry', 'employee_count_range', 'job_title', 'seniority', 'company_revenue']
 
    # Main headline
    st.markdown(f'<h1 style="text-align: center;"><span class="serif-gradient-centerpiece">{tenant_type} Analytics Dashboard</span></h1>', unsafe_allow_html=True)
 
    # Date slider
    if 'date_range' not in st.session_state:
        st.session_state.date_range = (datetime.now() - timedelta(days=30), datetime.now())
 
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", st.session_state.date_range[0])
    with col2:
        end_date = st.date_input("End Date", st.session_state.date_range[1])
 
    st.session_state.date_range = (start_date, end_date)
 
    # Filter data by date range
    df_demo = st.session_state.df_demo
    df_state = st.session_state.df_state if tenant_type == "B2C" else pd.DataFrame()
    df_orders = st.session_state.df_orders
 
    if not df_demo.empty:
        df_demo_filtered = df_demo[(df_demo['visit_date'] >= pd.Timestamp(start_date)) & (df_demo['visit_date'] <= pd.Timestamp(end_date))].copy()
    else:
        df_demo_filtered = df_demo.copy()
 
    if not df_state.empty:
        df_state_filtered = df_state[(df_state['visit_date'] >= pd.Timestamp(start_date)) & (df_state['visit_date'] <= pd.Timestamp(end_date))].copy()
    else:
        df_state_filtered = df_state.copy()
 
    if not df_orders.empty:
        orders_in_range = df_orders[(df_orders['order_date'] >= pd.Timestamp(start_date)) & (df_orders['order_date'] <= pd.Timestamp(end_date))].copy()
    else:
        orders_in_range = df_orders.copy()
 
    # Build demo cube and state map for session state
    st.session_state.df_demo_cube = df_demo_filtered
    st.session_state.df_state_map = df_state_filtered
 
    # Ghost day integrity shield - filter orders to only dates with visitors
    active_days = set(df_demo_filtered['visit_date'].dt.date.unique()) if not df_demo_filtered.empty else set()
    if not orders_in_range.empty and active_days:
        orders_in_range['order_date_only'] = orders_in_range['order_date'].dt.date
        df_p_filtered = orders_in_range[orders_in_range['order_date_only'].isin(active_days)].copy()
    else:
        df_p_filtered = orders_in_range.copy()
 
    st.divider()
 
    # ===== CONTROLS SECTION =====
    ctrl1, ctrl2, ctrl3, ctrl4, ctrl5 = st.columns([1.2, 1.2, 1.1, 2.5, 1.2])
    with ctrl1:
        metric_choice = st.radio("Primary Metric", ["Rev/Visitor", "Conv %", "Revenue", "Purchases", "Visitors"])
    with ctrl2:
        sort_order = st.radio("Ranking Order", ["High to Low", "Low to High"])
        is_ascending = (sort_order == "Low to High")
    with ctrl3:
        min_purchasers = st.number_input("Min Purchases", value=1, min_value=0)
 
    with ctrl4:
        sku_toggle = st.toggle("Filter by Product", value=False)
        if sku_toggle and not orders_in_range.empty:
            if 'lineitem_name' in orders_in_range.columns:
                sku_opts = sorted([str(x) for x in orders_in_range['lineitem_name'].dropna().unique() if str(x) not in EXCLUDE_LIST])
            else:
                sku_opts = []
            selected_skus = st.multiselect("Select SKUs", options=sku_opts)
            if selected_skus:
                df_p_filtered = df_p_filtered[df_p_filtered['lineitem_name'].isin(selected_skus)]
            else:
                df_p_filtered = df_p_filtered.iloc[0:0]
 
    with ctrl5:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Force Refresh", use_container_width=True):
            load_order_base.clear()
            load_visitor_base.clear()
            st.session_state.app_state = "onboarding"
            st.rerun()
 
    metric_map = {"Conv %": "Conv %", "Purchases": "Purchases", "Revenue": "Revenue", "Visitors": "Visitors", "Rev/Visitor": "Rev/Visitor"}
 
    st.divider()
 
    # ===== SINGLE VARIABLE DEEP DIVE =====
    st.subheader("🎯 Single Variable Deep Dive")
 
    # Initialize active_single_var in session state
    if "active_single_var" not in st.session_state:
        st.session_state.active_single_var = configs[0][0]
 
    # Render buttons with session state persistence
    v_cols = st.columns(len(configs))
    for i, (label, col_name) in enumerate(configs):
        if v_cols[i].button(label, key=f"btn_{label}",
                           type="primary" if st.session_state.active_single_var == label else "secondary",
                           use_container_width=True):
            st.session_state.active_single_var = label
            st.rerun()
 
    # Get selected column from current active_single_var
    selected_col = dict(configs)[st.session_state.active_single_var]
 
    # For state (B2C only), use df_state_map; otherwise use df_demo_cube
    if selected_col == 'state' and tenant_type == 'B2C':
        df_v_grp = st.session_state.df_state_map[
            ~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)
        ].groupby('state', as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    else:
        df_v_grp = st.session_state.df_demo_cube[
            ~st.session_state.df_demo_cube[selected_col].isin(EXCLUDE_LIST)
        ].groupby(selected_col, as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
 
    # Merge with orders
    df_p_grp = pd.DataFrame()
    if not df_p_filtered.empty and selected_col in df_p_filtered.columns:
        df_p_grp = df_p_filtered[~df_p_filtered[selected_col].isin(EXCLUDE_LIST)].groupby(selected_col).agg(
            Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')
        ).reset_index()
 
    if not df_p_grp.empty:
        df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='outer').fillna(0)
    else:
        df_merged = df_v_grp.copy()
        df_merged['Purchases'] = 0
        df_merged['Revenue'] = 0.0
 
    if not df_merged.empty:
        df_merged['Visitors'] = df_merged.get('Visitors', 0) + df_merged['Purchases']
        df_merged['Conv %'] = (df_merged['Purchases'] / df_merged['Visitors'].replace(0, 1) * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue'] / df_merged['Visitors'].replace(0, 1)).round(2)
        df_merged = df_merged[df_merged['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
        if not df_merged.empty:
            df_merged.insert(0, 'Rank', range(1, len(df_merged) + 1))
            display_df = df_merged.rename(columns={selected_col: st.session_state.active_single_var})
            display_cols = ['Rank', st.session_state.active_single_var, 'Revenue', 'Visitors', 'Purchases', 'Conv %', 'Rev/Visitor']
            styler = display_df[display_cols].style\
                .set_properties(**{'font-weight': 'bold'}, subset=['Rank'])\
                .format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'})\
                .background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)
 
    st.divider()
 
    # ===== MULTI-VARIABLE COMBINATION MATRIX =====
    st.subheader("📊 Multi-Variable Combination Matrix")
    with st.expander("🎛️ Combination Filters", expanded=True):
        selected_filters, included_types = {}, []
        f_cols = st.columns(3)
        valid_matrix_configs = [c for c in configs if c[1] != 'state']
        for i, (label, col_name) in enumerate(valid_matrix_configs):
            with f_cols[i % 3]:
                c_title, c_inc = st.columns([3, 1])
                c_title.markdown(f'**{label}**')
                if c_inc.checkbox("Inc", key=f"inc_{col_name}"):
                    included_types.append(col_name)
                opts = sorted([str(x) for x in st.session_state.df_demo_cube[col_name].unique() if str(x) not in EXCLUDE_LIST])
                val = st.multiselect(f"Filter {label}", opts, key=f"f_{col_name}", label_visibility="collapsed")
                if val:
                    selected_filters[col_name] = val
 
    if included_types:
        combos = []
        filtered_cols = list(selected_filters.keys())
        unfiltered_cols = [c for c in included_types if c not in filtered_cols]
        base_v = st.session_state.df_demo_cube.copy()
        base_p = df_p_filtered.copy()
 
        for col, vals in selected_filters.items():
            base_v = base_v[base_v[col].isin(vals)]
            if not base_p.empty and col in base_p.columns:
                base_p = base_p[base_p[col].isin(vals)]
 
        for r in range(1 if not filtered_cols else 0, (max(3, len(filtered_cols)) - len(filtered_cols)) + 1):
            for subset in itertools.combinations(unfiltered_cols, r):
                sub_cols = filtered_cols + list(subset)
                if not sub_cols:
                    continue
 
                grp_v = base_v[~base_v[sub_cols].isin(EXCLUDE_LIST).any(axis=1)]\
                    .groupby(sub_cols, as_index=False)['total_visitors'].sum()
 
                if not base_p.empty and all(c in base_p.columns for c in sub_cols):
                    grp_p = base_p[~base_p[sub_cols].isin(EXCLUDE_LIST).any(axis=1)]\
                        .groupby(sub_cols).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
                else:
                    grp_p = pd.DataFrame(columns=sub_cols + ['Purchases', 'Revenue'])
 
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='outer').fillna(0)
                if 'total_visitors' in grp.columns:
                    grp = grp.rename(columns={'total_visitors': 'Visitors'})
                else:
                    grp['Visitors'] = 0
                grp['Visitors'] += grp['Purchases']
 
                for col in included_types:
                    if col not in sub_cols:
                        grp[col] = ""
                combos.append(grp)
 
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'].replace(0, 1) * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors'].replace(0, 1)).round(2)
            final_res = res[res['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
            if not final_res.empty:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                final_res.insert(0, 'Rank', range(1, len(final_res) + 1))
                rename_dict = {c[1]: c[0] for c in configs}
                display_cols = ['Rank'] + [rename_dict.get(c, c) for c in included_types] + ['Revenue', 'Visitors', 'Purchases', 'Conv %', 'Rev/Visitor']
                display_df = final_res.head(50).rename(columns=rename_dict)[display_cols]
                render_premium_table(display_df.style.format({
                    'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}',
                    'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'
                }).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient))
 
    st.divider()
 
    # ===== FILE UPLOAD SECTION =====
    st.subheader("📁 Upload Order Export for Enrichment")
    uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
    if uploaded_file is not None:
        if st.button("Trigger n8n Enrichment Pipeline", type="primary"):
            with st.spinner("Transmitting to enrichment pipeline..."):
                files = {'file': (uploaded_file.name, uploaded_file.getvalue(), 'text/csv')}
                data = {'pixel_id': pixel_id, 'tenant_type': tenant_type}
                try:
                    response = requests.post(N8N_WEBHOOK_URL, files=files, data=data)
                    if response.status_code == 200:
                        st.success("Success! Click 'Force Refresh' to see updated conversions.")
                    else:
                        st.error(f"Webhook failed: {response.status_code}")
                except Exception as e:
                    st.error(f"Error communicating with n8n: {e}")
 
# ================ 9. MAIN APP FLOW =================
def main():
    if st.session_state.app_state == 'login':
        login_page()
    elif st.session_state.app_state == 'onboarding':
        onboarding_page()
    elif st.session_state.app_state == 'dashboard':
        dashboard_page()
 
if __name__ == "__main__":
    main()
