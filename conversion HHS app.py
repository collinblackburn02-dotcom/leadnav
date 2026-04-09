import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools
import re

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" 

BQ_UNIQUE_ORDERS_VIEW = "leadnav-hhs.HHSpixeltest.heavenly_heat_unique_orders"
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
            
            /* Table Styling */
            .premium-table-container {{ width: 100% !important; border-radius: 12px; border: 1px solid {primary_color}; background: #FFFFFF; overflow: hidden; margin-top: 1rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
            .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; border: none !important; }}
            .premium-table-container th {{ font-family: 'Outfit', sans-serif !important; background-color: #F8F6FA !important; color: {primary_color} !important; font-weight: 700 !important; text-align: center !important; padding: 15px 12px !important; border-bottom: 2px solid {primary_color} !important; font-size: 0.95rem !important; }}
            .premium-table-container td {{ font-family: 'Outfit', sans-serif !important; text-align: center !important; padding: 12px !important; border-bottom: 1px solid #EBE4F4 !important; font-size: 0.9rem !important; color: #1e293b !important; }}
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; }}

            .serif-gradient-centerpiece {{ font-family: 'Playfair Display', serif !important; background: linear-gradient(90deg, #4D148C 0%, #20B2AA 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: inline-block; font-weight: 700 !important; letter-spacing: -0.5px; }}
            .modern-serif-title {{ font-family: 'Playfair Display', serif !important; color: #0F172A !important; font-weight: 700 !important; }}
            
            .custom-loader {{ border: 3px solid #f3f3f3; border-top: 3px solid {primary_color}; border-radius: 50%; width: 32px; height: 32px; animation: spin 1s linear infinite; margin: 0 auto 30px auto; }}
            @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
        </style>
    """, unsafe_allow_html=True)

apply_custom_theme(PITCH_BRAND_COLOR)
brand_gradient = mcolors.LinearSegmentedColormap.from_list("brand_purple", ["#FFFFFF", "#FBF9FC", "#EBE4F4"])

def render_premium_table(styler_obj):
    try: styler_obj = styler_obj.hide(axis="index")
    except AttributeError: styler_obj = styler_obj.hide_index() 
    html = styler_obj.to_html()
    st.markdown(f'<div class="premium-table-container">{html}</div>', unsafe_allow_html=True)

# ================ 2. THE VERIFIED DATA ENGINE =================
DEMO_COLS = ['gender', 'age_range', 'marital_status', 'children', 'homeowner_status', 'income_bracket', 'net_worth_bracket']
configs = [
    ("Gender", "gender"), ("Age", "age_range"), ("Income", "income_bracket"), ("State", "state"), 
    ("Net Worth", "net_worth_bracket"), ("Children", "children"), ("Marital Status", "marital_status"), ("Homeowner", "homeowner_status")
]
INCOME_MAP = {'Under $45k': 1, '$45k-$100k': 2, '$100k-$150k': 3, '$150k-$250k': 4, '$250k+': 5}
NET_WORTH_MAP = {'Under $100k': 1, '$100k-$249k': 2, '$250k-$499k': 3, '$500k+': 4}

@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])

def get_real_number(v):
    if pd.isna(v): return None
    v_str = str(v).lower().replace(',', '')
    match = re.search(r'(\d+\.?\d*)', v_str)
    if not match: return None
    val_num = float(match.group(1))
    if re.search(r'(m\b|million)', v_str): val_num *= 1000000
    elif re.search(r'(k\b|thousand)', v_str): val_num *= 1000
    return val_num

def bucket_income_bq(val):
    v = str(val).strip().lower()
    if v in ['unknown', 'all', 'none', 'nan', '<na>', 'null', '']: return 'Unknown'
    num = get_real_number(v)
    if num is None: return 'Unknown'
    if num < 45000: return 'Under $45k'
    elif num < 100000: return '$45k-$100k'
    elif num < 150000: return '$100k-$150k'
    elif num < 250000: return '$150k-$250k' 
    else: return '$250k+'

def bucket_net_worth_bq(val):
    v = str(val).strip().lower()
    if v in ['unknown', 'all', 'none', 'nan', '<na>', 'null', '']: return 'Unknown'
    num = get_real_number(v)
    if num is None: return 'Unknown'
    if num < 95000: return 'Under $100k'
    elif num < 245000: return '$100k-$249k'
    elif num < 450000: return '$250k-$499k' 
    else: return '$500k+'

@st.cache_data(show_spinner=False, ttl=1800)
def load_order_base():
    client = get_bq_client()
    try:
        df = client.query(f"SELECT * FROM `{BQ_UNIQUE_ORDERS_VIEW}`").to_dataframe()
        df = df.rename(columns={'order_id': 'Order_ID', 'revenue': 'Total', 'email': 'email_match', 'age': 'age_range', 'income': 'income_raw', 'net_worth': 'net_worth_raw', 'homeowner': 'homeowner_raw'})
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', utc=True).dt.date
        df['income_bracket'] = df['income_raw'].apply(bucket_income_bq)
        df['net_worth_bracket'] = df['net_worth_raw'].apply(bucket_net_worth_bq)
        df['state'] = df['state'].astype(str).str.strip().str.upper()
        return df, None
    except Exception as e: return pd.DataFrame(), str(e)

@st.cache_data(show_spinner=False, ttl=1800)
def load_visitor_base():
    client = get_bq_client()
    try:
        df = client.query("SELECT * FROM `leadnav-hhs.HHSpixeltest.daily_visitor_summary`").to_dataframe()
        df.columns = [c.lower().strip() for c in df.columns]
        df['visit_date'] = pd.to_datetime(df['visit_date']).dt.date
        df = df.rename(columns={'income_raw': 'income_bracket', 'net_worth_raw': 'net_worth_bracket', 'homeowner_raw': 'homeowner_status'})
        df['total_visitors'] = pd.to_numeric(df['total_visitors'], errors='coerce').fillna(0)
        
        # Ensure all demographics are bucketed consistently
        for col in DEMO_COLS:
            if 'income' in col: df[col] = df[col].apply(bucket_income_bq)
            elif 'net_worth' in col: df[col] = df[col].apply(bucket_net_worth_bq)
            df[col] = df[col].astype(str).str.strip().replace(['nan', 'NaN', 'None', 'null', ''], 'Unknown')
        
        df_demo = df.groupby(['visit_date'] + DEMO_COLS, as_index=False)['total_visitors'].sum()
        df_state = df.groupby(['visit_date', 'state'], as_index=False)['total_visitors'].sum()
        return df_demo, df_state, None
    except Exception as e: return pd.DataFrame(), pd.DataFrame(), str(e)

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: st.session_state.app_state = "onboarding"

if st.session_state.app_state == "onboarding":
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col: st.markdown(f'<div style="font-family: \'Outfit\', sans-serif; font-size: 1.6rem; font-weight: 800; color: #0F172A;">Lead<span style="color: {PITCH_BRAND_COLOR};">Navigator</span></div>', unsafe_allow_html=True)
    st.markdown("""<div style="text-align: center; margin-top: 40px; margin-bottom: 25px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.6rem;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 1.8rem; color: #0F172A !important;">Synchronize with BigQuery to view live conversions.</h2></div>""", unsafe_allow_html=True)
    
    _, center_col, _ = st.columns([2, 1.5, 2])
    if center_col.button("🔄 Sync Database", type="primary", use_container_width=True):
        with st.status("Connecting to Identity Graph...", expanded=True) as status:
            st.session_state.df_icp, order_err = load_order_base()
            st.session_state.df_demo_base, st.session_state.df_state_base, visitor_err = load_visitor_base()
            status.update(label="Sync Complete!", state="complete", expanded=False)
        
        if not order_err and not visitor_err:
            st.session_state.min_date = st.session_state.df_demo_base['visit_date'].min()
            st.session_state.max_date = st.session_state.df_demo_base['visit_date'].max()
            st.session_state.date_filter = (st.session_state.min_date, st.session_state.max_date)
            st.session_state.app_state = "dashboard"
            st.rerun()

elif st.session_state.app_state == "dashboard":
    included_types = [] 
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col: st.markdown(f'<div style="font-family: \'Outfit\', sans-serif; font-size: 1.6rem; font-weight: 800; color: #0F172A;">Lead<span style="color: {PITCH_BRAND_COLOR};">Navigator</span></div>', unsafe_allow_html=True)
    
    st.markdown(f"""<div style="text-align: center; margin-top: 10px; margin-bottom: 30px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.5rem; margin-bottom: 0px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 2.8rem; color: #0F172A !important; margin-top: -5px;">Optimize your traffic funnel.</h2></div>""", unsafe_allow_html=True)
    
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Traffic & Purchaser Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    
    # 🚨 DATA INTEGRITY FILTER
    current_dates = st.session_state.get("date_filter")
    active_days = set(st.session_state.df_demo_base[(st.session_state.df_demo_base['visit_date'] >= current_dates[0]) & (st.session_state.df_demo_base['visit_date'] <= current_dates[1])]['visit_date'].unique())
    
    orders_in_range = st.session_state.df_icp[(st.session_state.df_icp['order_date'] >= current_dates[0]) & (st.session_state.df_icp['order_date'] <= current_dates[1])]
    df_p_filtered = orders_in_range[orders_in_range['order_date'].isin(active_days)]
    
    st.session_state.df_demo_cube = st.session_state.df_demo_base[(st.session_state.df_demo_base['visit_date'] >= current_dates[0]) & (st.session_state.df_demo_base['visit_date'] <= current_dates[1])]
    st.session_state.df_state_map = st.session_state.df_state_base[(st.session_state.df_state_base['visit_date'] >= current_dates[0]) & (st.session_state.df_state_base['visit_date'] <= current_dates[1])]

    st.markdown("<hr>", unsafe_allow_html=True)
    ctrl1, ctrl2, ctrl3, ctrl4 = st.columns(4)
    with ctrl1: metric_choice = st.radio("Primary Metric Leaderboard", ["Rev/Visitor", "Conv %", "Revenue", "Purchases", "Visitors"])
    with ctrl2: sort_order = st.radio("Ranking Order", ["High to Low", "Low to High"]); is_ascending = (sort_order == "Low to High")
    with ctrl3: min_purchasers = st.number_input("Minimum Purchases", value=1, min_value=0)
    with ctrl4:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Force Data Refresh", use_container_width=True): 
            load_order_base.clear(); load_visitor_base.clear()
            st.session_state.app_state = "onboarding"; st.rerun()
    
    if len(orders_in_range) > len(df_p_filtered):
        st.info(f"🛡️ **Integrity Shield:** {len(orders_in_range) - len(df_p_filtered)} orders from 'Ghost Days' (0 visitors) were excluded.")

    st.markdown("<hr>", unsafe_allow_html=True)
    metric_map = {"Conv %": "Conv %", "Purchases": "Purchases", "Revenue": "Revenue", "Visitors": "Visitors", "Rev/Visitor": "Rev/Visitor"}

    # ========================================================
    # 🔍 SINGLE VARIABLE DEEP DIVE
    # ========================================================
    st.subheader("🔍 Single Variable Deep Dive")
    if "active_single_var" not in st.session_state: st.session_state.active_single_var = "Gender"
    
    v_cols = st.columns(8)
    for i, (label, col_name) in enumerate(configs):
        if v_cols[i].button(label, key=f"btn_{label}", type="primary" if st.session_state.active_single_var == label else "secondary", use_container_width=True):
            st.session_state.active_single_var = label; st.rerun()
                
    selected_col = dict(configs)[st.session_state.active_single_var]
    if selected_col == 'state':
        df_v_grp = st.session_state.df_state_map[~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)].groupby('state', as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    else:
        df_v_grp = st.session_state.df_demo_cube[~st.session_state.df_demo_cube[selected_col].isin(EXCLUDE_LIST)].groupby(selected_col, as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    
    df_p_grp = df_p_filtered[~df_p_filtered[selected_col].isin(EXCLUDE_LIST)].groupby(selected_col).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
    df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='outer').fillna(0)

    if not df_merged.empty:
        df_merged['Visitors'] += df_merged['Purchases']
        df_merged['Conv %'] = (df_merged['Purchases'] / df_merged['Visitors'] * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue'] / df_merged['Visitors']).round(2)
        df_merged = df_merged[df_merged['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
        
        if not df_merged.empty:
            df_merged.insert(0, 'Rank', range(1, len(df_merged) + 1))
            styler = df_merged.rename(columns={selected_col: st.session_state.active_single_var}).style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)

    # ========================================================
    # 📊 MULTI-VARIABLE COMBINATION MATRIX
    # ========================================================
    st.subheader("📊 Multi-Variable Combination Matrix")
    with st.expander("🎛️ Combination Filters", expanded=True):
        selected_filters, included_types = {}, []
        f_cols = st.columns(3)
        for i, (label, col_name) in enumerate([c for c in configs if c[1] != 'state']):
            with f_cols[i % 3]:
                c_title, c_inc = st.columns([3, 1])
                c_title.markdown(f'**{label}**')
                if c_inc.checkbox("Inc", key=f"inc_{col_name}"): included_types.append(col_name)
                opts = sorted([x for x in st.session_state.df_demo_cube[col_name].unique() if x not in EXCLUDE_LIST])
                val = st.multiselect(f"Filter {label}", opts, key=f"f_{col_name}", label_visibility="collapsed")
                if val: selected_filters[col_name] = val

    if included_types:
        combos = []
        filtered_cols = list(selected_filters.keys())
        unfiltered_cols = [c for c in included_types if c not in filtered_cols]
        base_v, base_p = st.session_state.df_demo_cube.copy(), df_p_filtered.copy()
        for col, vals in selected_filters.items():
            base_v = base_v[base_v[col].isin(vals)]; base_p = base_p[base_p[col].isin(vals)]

        for r in range(1 if not filtered_cols else 0, (max(3, len(filtered_cols)) - len(filtered_cols)) + 1):
            for subset in itertools.combinations(unfiltered_cols, r):
                sub_cols = filtered_cols + list(subset)
                if not sub_cols: continue
                grp_v = base_v[~base_v[sub_cols].isin(EXCLUDE_LIST).any(axis=1)].groupby(sub_cols, as_index=False)['total_visitors'].sum()
                grp_p = base_p[~base_p[sub_cols].isin(EXCLUDE_LIST).any(axis=1)].groupby(sub_cols).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='outer').fillna(0).rename(columns={'total_visitors': 'Visitors'})
                grp['Visitors'] += grp['Purchases']
                for col in included_types:
                    if col not in sub_cols: grp[col] = ""
                combos.append(grp)
        
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'] * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors']).round(2)
            final_res = res[res['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
            if not final_res.empty:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                final_res.insert(0, 'Rank', range(1, len(final_res) + 1))
                rename_dict = {c[1]: c[0] for c in configs}
                render_premium_table(final_res.head(50).rename(columns=rename_dict).style.format({'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient))
