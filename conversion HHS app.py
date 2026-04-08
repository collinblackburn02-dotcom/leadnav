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
# Updated to include uppercase versions
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
            .stMarkdown a svg {{ display: none !important; }}
            div[data-testid="stSlider"] label p {{ font-size: 1.2rem !important; font-weight: 700 !important; color: #0F172A !important; }}
            div[data-testid="stButton"] button {{ border-radius: 8px; font-weight: 600; margin-bottom: 5px; }}
            div[data-testid="stButton"] button[kind="primary"] {{ background-color: {primary_color} !important; color: #FFFFFF !important; border: none !important; }}
            
            .premium-table-container {{ width: 100% !important; border-radius: 12px; border: 1px solid {primary_color}; background: #FFFFFF; overflow: hidden; margin-top: 1rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
            .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; border: none !important; }}
            .premium-table-container th {{ font-family: 'Outfit', sans-serif !important; background-color: #F8F6FA !important; color: {primary_color} !important; font-weight: 700 !important; text-align: center !important; padding: 15px 12px !important; border-bottom: 2px solid {primary_color} !important; font-size: 0.95rem !important; text-transform: none !important; }}
            .premium-table-container td {{ font-family: 'Outfit', sans-serif !important; text-align: center !important; padding: 12px !important; border-bottom: 1px solid #EBE4F4 !important; font-size: 0.9rem !important; color: #1e293b !important; }}
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; text-align: center !important; }}
            
            .serif-gradient-centerpiece {{ font-family: 'Playfair Display', serif !important; background: linear-gradient(90deg, #4D148C 0%, #20B2AA 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: inline-block; font-weight: 700 !important; letter-spacing: -0.5px; }}
            .modern-serif-title {{ font-family: 'Playfair Display', serif !important; color: #0F172A !important; font-weight: 700 !important; }}
            
            .custom-loader {{ border: 3px solid #f3f3f3; border-top: 3px solid {primary_color}; border-radius: 50%; width: 32px; height: 32px; animation: spin 1s linear infinite; margin: 0 auto 30px auto; }}
            @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
            @keyframes fadeLoop {{ 0% {{ opacity: 0; transform: translateY(5px); }} 1%, 24% {{ opacity: 1; transform: translateY(0); }} 25%, 100% {{ opacity: 0; transform: translateY(-5px); }} }}
            .pitch-fact {{ font-family: 'Outfit', sans-serif; font-size: 1.15rem; color: #475569; font-weight: 400; font-style: italic; position: absolute; width: 100%; opacity: 0; animation: fadeLoop 80s infinite; line-height: 1.5; text-align: center; }}
            .fact-1 {{ animation-delay: 0s; }} .fact-2 {{ animation-delay: 20s; }} .fact-3 {{ animation-delay: 40s; }} .fact-4 {{ animation-delay: 60s; }}
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
    ("Gender", "gender"), 
    ("Age", "age_range"), 
    ("Income", "income_bracket"), 
    ("State", "state"), 
    ("Net Worth", "net_worth_bracket"), 
    ("Children", "children"), 
    ("Marital Status", "marital_status"), 
    ("Homeowner", "homeowner_status")
]
INCOME_MAP = {'Under $50k': 1, '$50k-$100k': 2, '$100k-$150k': 3, '$150k-$250k': 4, '$250k+': 5}
NET_WORTH_MAP = {'Under $100k': 1, '$100k-$249k': 2, '$250k-$499k': 3, '$500k+': 4}

@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])

def clean_gender(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    if v in ['m', 'male']: return 'Male'
    if v in ['f', 'female']: return 'Female'
    return 'Unknown'

def clean_yes_no(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    if v in ['y', 'yes', 'true', 't', '1', '1.0']: return 'Yes'
    if v in ['n', 'no', 'false', 'f', '0', '0.0']: return 'No'
    return 'Unknown'

def clean_marital(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    if v in ['y', 'yes', 'true', 't', 'married', 'm']: return 'Married'
    if v in ['n', 'no', 'false', 'f', 'single', 's']: return 'Single'
    return 'Unknown'

def clean_homeowner_bq(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    if v in ['y', 'yes', 'true', 't', 'homeowner']: return 'Homeowner'
    if v in ['n', 'no', 'false', 'f', 'renter']: return 'Renter'
    if 'homeowner' in v: return 'Homeowner'
    if 'renter' in v: return 'Renter'
    return 'Unknown'

def clean_age(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    if '65' in v: return '65+'
    if '18' in v and '24' in v: return '18-24'
    if '25' in v and '34' in v: return '25-34'
    if '35' in v and '44' in v: return '35-44'
    if '45' in v and '54' in v: return '45-54'
    if '55' in v and '64' in v: return '55-64'
    return 'Unknown'

def get_real_number(v):
    v_str = str(val).lower().replace(',', '') if 'val' in locals() else str(v).lower().replace(',', '')
    match = re.search(r'(\d+\.?\d*)', v_str)
    if not match: return None
    val_num = float(match.group(1))
    if re.search(r'(m\b|million)', v_str): val_num *= 1000000
    elif re.search(r'(k\b|thousand)', v_str): val_num *= 1000
    return val_num

def bucket_income_bq(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', 'unknown', '']: return 'ALL'
    num = get_real_number(v)
    if num is not None:
        if num < 50000: return 'Under $50k'
        elif num < 100000: return '$50k-$100k'
        elif num < 150000: return '$100k-$150k'
        elif num < 249999: return '$150k-$250k' 
        else: return '$250k+'
    return 'Unknown'

def bucket_net_worth_bq(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', 'unknown', '']: return 'ALL'
    num = get_real_number(v)
    if num is not None:
        if num < 100000: return 'Under $100k'
        elif num < 250000: return '$100k-$249k'
        elif num < 499999: return '$250k-$499k' 
        else: return '$500k+'
    return 'Unknown'

def normalize_demographics(df):
    if 'gender' in df.columns: df['gender'] = df['gender'].apply(clean_gender)
    if 'children' in df.columns: df['children'] = df['children'].apply(clean_yes_no)
    if 'marital_status' in df.columns: df['marital_status'] = df['marital_status'].apply(clean_marital)
    if 'age_range' in df.columns: df['age_range'] = df['age_range'].apply(clean_age)
    if 'homeowner_raw' in df.columns: df['homeowner_status'] = df['homeowner_raw'].apply(clean_homeowner_bq)
    if 'income_raw' in df.columns: df['income_bracket'] = df['income_raw'].apply(bucket_income_bq)
    if 'net_worth_raw' in df.columns: df['net_worth_bracket'] = df['net_worth_raw'].apply(bucket_net_worth_bq)
    if 'state' in df.columns: df['state'] = df['state'].astype(str).str.strip().str.upper()

    for col in df.columns:
        df[col] = df[col].replace(["", "nan", "NaN", "None", "null", "NULL", "<NA>", "unknown", "Unknown"], "Unknown")
    return df

@st.cache_data(show_spinner=False, ttl=1800)
def load_order_base():
    client = get_bq_client()
    try:
        df_orders = client.query(f"SELECT * FROM `{BQ_UNIQUE_ORDERS_VIEW}`").to_dataframe()
        if df_orders.empty: return df_orders, None

        df_orders = df_orders.rename(columns={'order_id': 'Order_ID', 'revenue': 'Total', 'email': 'email_match'})
        df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce', utc=True).dt.date
        df_orders = df_orders.rename(columns={'age': 'age_range', 'income': 'income_raw', 'net_worth': 'net_worth_raw', 'homeowner': 'homeowner_raw'})
        df_orders = normalize_demographics(df_orders)
        return df_orders, None
    except Exception as e:
        return pd.DataFrame(), str(e)

@st.cache_data(show_spinner=False, ttl=1800)
def load_visitor_base():
    client = get_bq_client()
    try:
        df_raw = client.query("SELECT * FROM `leadnav-hhs.HHSpixeltest.daily_visitor_summary`").to_dataframe()
        df_raw.columns = [c.lower().strip() for c in df_raw.columns]
        df_raw['visit_date'] = pd.to_datetime(df_raw['visit_date']).dt.date
        
        df_raw = df_raw.rename(columns={'income_raw': 'income_bracket', 'net_worth_raw': 'net_worth_bracket', 'homeowner_raw': 'homeowner_status'})

        if 'gender' in df_raw.columns: df_raw['gender'] = df_raw['gender'].apply(clean_gender)
        if 'children' in df_raw.columns: df_raw['children'] = df_raw['children'].apply(clean_yes_no)
        if 'marital_status' in df_raw.columns: df_raw['marital_status'] = df_raw['marital_status'].apply(clean_marital)
        if 'age_range' in df_raw.columns: df_raw['age_range'] = df_raw['age_range'].apply(clean_age)
        if 'homeowner_status' in df_raw.columns: df_raw['homeowner_status'] = df_raw['homeowner_status'].apply(clean_homeowner_bq)
        if 'income_bracket' in df_raw.columns: df_raw['income_bracket'] = df_raw['income_bracket'].apply(bucket_income_bq)
        if 'net_worth_bracket' in df_raw.columns: df_raw['net_worth_bracket'] = df_raw['net_worth_bracket'].apply(bucket_net_worth_bq)
        
        for col in df_raw.columns:
            if col not in ['total_visitors', 'visit_date']: 
                df_raw[col] = df_raw[col].astype(str).str.strip()
                if col == 'state':
                    df_raw[col] = df_raw[col].str.upper()

        df_raw = df_raw.replace(['nan', 'NaN', '<NA>', 'None', 'null', ''], 'ALL').fillna('ALL')

        df_demo = df_raw.groupby(['visit_date'] + DEMO_COLS, as_index=False)['total_visitors'].sum()
        df_state = df_raw.groupby(['visit_date', 'state'], as_index=False)['total_visitors'].sum()
        
        return df_demo, df_state, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: st.session_state.app_state = "onboarding"
if "df_icp" not in st.session_state: st.session_state.df_icp = None

custom_html_logo = f"""
    <div style="font-family: 'Outfit', sans-serif; font-size: 1.6rem; font-weight: 800; color: #0F172A; letter-spacing: -0.5px; margin-top: 10px; white-space: nowrap;">
        Lead<span style="color: {PITCH_BRAND_COLOR};">Navigator</span>
    </div>
"""

if st.session_state.app_state == "onboarding":
    
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col:
        st.markdown(custom_html_logo, unsafe_allow_html=True)
        
    st.markdown("""<div style="text-align: center; margin-top: 40px; margin-bottom: 25px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.6rem; margin-bottom: 2px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 1.8rem; color: #0F172A !important; margin-top: 5px;">Synchronize with BigQuery to view live conversions.</h2></div>""", unsafe_allow_html=True)
    
    _, center_col, _ = st.columns([2, 1.5, 2])
    st.markdown("<br>", unsafe_allow_html=True)
    
    if center_col.button("🔄 Sync Database", type="primary", use_container_width=True):
        status_placeholder = st.empty()
        with status_placeholder:
            st.markdown(f"""
                <div style="text-align: center; padding: 60px 40px; background: #F8F6FA; border-radius: 12px; border: 1px solid {PITCH_BRAND_COLOR}; min-height: 200px;">
                    <h3 class="modern-serif-title" style="color: {PITCH_BRAND_COLOR}; margin-bottom: 10px;">Connecting to Identity Graph...</h3>
                    <p style="color: #64748B; font-family: 'Outfit', sans-serif; margin-bottom: 40px;">Pulling live deduplicated order and visitor records.</p>
                    <div class="custom-loader"></div>
                </div>
            """, unsafe_allow_html=True)
            
        # 1. Load Orders
        st.session_state.df_icp, order_err = load_order_base()
        
        # 2. Load Visitors into our BASE state so we don't accidentally delete data when sliding
        st.session_state.df_demo_base, st.session_state.df_state_base, visitor_err = load_visitor_base()
        
        status_placeholder.empty()

        if order_err or visitor_err:
            st.error(f"🚨 BIGQUERY ERROR. Orders: {order_err} | Visitors: {visitor_err}")
        elif st.session_state.df_icp.empty:
            st.warning("BigQuery connected, but no orders were found in the unique view.")
        else:
            st.session_state.min_date = st.session_state.df_icp['order_date'].min()
            st.session_state.max_date = st.session_state.df_icp['order_date'].max()
            st.session_state.date_filter = (st.session_state.min_date, st.session_state.max_date)
            st.session_state.app_state = "dashboard"
            st.rerun()

elif st.session_state.app_state == "dashboard":
    
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col:
        st.markdown(custom_html_logo, unsafe_allow_html=True)
        
    st.markdown(f"""<div style="text-align: center; margin-top: 10px; margin-bottom: 30px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.5rem; margin-bottom: 0px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 2.8rem; color: #0F172A !important; margin-top: -5px;">Optimize your traffic funnel.</h2></div>""", unsafe_allow_html=True)
    
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Traffic & Purchaser Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    
    current_dates = st.session_state.get("date_filter")
    
    # 🚨 FILTER ORDERS
    df_p = st.session_state.df_icp.copy()
    df_p_filtered = df_p[(df_p['order_date'] >= current_dates[0]) & (df_p['order_date'] <= current_dates[1])]

    # 🚨 FILTER VISITORS (Using our safe base copies)
    st.session_state.df_demo_cube = st.session_state.df_demo_base[(st.session_state.df_demo_base['visit_date'] >= current_dates[0]) & (st.session_state.df_demo_base['visit_date'] <= current_dates[1])]
    st.session_state.df_state_map = st.session_state.df_state_base[(st.session_state.df_state_base['visit_date'] >= current_dates[0]) & (st.session_state.df_state_base['visit_date'] <= current_dates[1])]

    st.markdown("<hr style='margin-top: 10px; margin-bottom: 30px;'>", unsafe_allow_html=True)
    st.markdown('### 🎛️ Global Dashboard Controls')
    
    ctrl1, ctrl2, ctrl3, ctrl4 = st.columns(4)
    with ctrl1:
        metric_choice = st.radio("Primary Metric Leaderboard", ["Rev/Visitor", "Conv %", "Revenue", "Purchases", "Visitors"])
    with ctrl2:
        sort_order = st.radio("Ranking Order", ["High to Low", "Low to High"])
        is_ascending = (sort_order == "Low to High")
    with ctrl3:
        min_purchasers = st.number_input("Minimum Purchases", value=1, min_value=0)
    with ctrl4:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Force Data Refresh", use_container_width=True): 
            load_order_base.clear()
            load_visitor_base.clear()
            st.session_state.app_state = "onboarding"
            st.rerun()
            
    st.markdown("<hr style='margin-top: 10px; margin-bottom: 30px;'>", unsafe_allow_html=True)

    metric_map = {"Conv %": "Conv %", "Purchases": "Purchases", "Revenue": "Revenue", "Visitors": "Visitors", "Rev/Visitor": "Rev/Visitor"}

    for _, col_name in configs:
        if col_name not in df_p_filtered.columns:
            df_p_filtered[col_name] = 'Unknown'

    st.markdown('<p style="font-size: 2rem; font-weight: 700; margin-bottom: 0px;">Audience Insights Engine</p>', unsafe_allow_html=True)
    st.markdown('<p style="color: #64748B; margin-top: -5px; margin-bottom: 30px;">Traffic and Conversion Optimization</p>', unsafe_allow_html=True)
    
    # ========================================================
    # 🔍 SINGLE VARIABLE DEEP DIVE
    # ========================================================
    st.subheader("🔍 Single Variable Deep Dive")
    if "active_single_var" not in st.session_state: st.session_state.active_single_var = "Gender"
    
    for i in range(0, len(configs), 5):
        var_cols = st.columns(5)
        for j, (label, col_name) in enumerate(configs[i:i+5]):
            if var_cols[j].button(label, key=f"btn_{label}_t1", type="primary" if st.session_state.active_single_var == label else "secondary", use_container_width=True):
                st.session_state.active_single_var = label
                st.rerun()
                
    selected_col = dict(configs)[st.session_state.active_single_var]
    
    # 🚨 DYNAMIC GROUPING FIX
    if selected_col == 'state':
        df_v_sub = st.session_state.df_state_map[~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)]
        df_v_grp = df_v_sub.groupby('state', as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    else:
        df_v_sub = st.session_state.df_demo_cube[~st.session_state.df_demo_cube[selected_col].isin(EXCLUDE_LIST)]
        df_v_grp = df_v_sub.groupby(selected_col, as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    
    df_p = df_p_filtered[~df_p_filtered[selected_col].isin(EXCLUDE_LIST)]
    df_p_grp = df_p.groupby(selected_col).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
    
    # Changed to an OUTER merge so segments with purchasers but zero visitors are not dropped
    df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='outer').fillna(0)

    if not df_merged.empty:
        # Pad the visitor count with the purchaser count
        df_merged['Visitors'] = df_merged['Visitors'] + df_merged['Purchases']
        
        df_merged['Conv %'] = (df_merged['Purchases'] / df_merged['Visitors'] * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue'] / df_merged['Visitors']).round(2)
        
        # 🚨 FILTER BY PURCHASERS INSTEAD OF VISITORS
        df_merged = df_merged[df_merged['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
        
        if not df_merged.empty:
            display_df = df_merged.rename(columns={selected_col: st.session_state.active_single_var})
            display_df.insert(0, 'Rank', range(1, len(display_df) + 1))
            styler = display_df.style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)
        else:
            st.warning("No segments met the Minimum Purchases criteria. You can lower the floor in the Global Controls above.")
    else:
        st.warning("No segments met the Minimum Purchases criteria. You can lower the floor in the Global Controls above.")

    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ========================================================
    # 🏆 TOP CONVERSION DRIVERS
    # ========================================================
    st.subheader("🏆 Top Conversion Drivers")
    predictive_data = []
    for label, col_name in configs:
        # 🚨 DYNAMIC GROUPING FIX
        if col_name == 'state':
            df_v_sub = st.session_state.df_state_map[~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)]
            grp_v = df_v_sub.groupby('state', as_index=False)['total_visitors'].sum()
        else:
            df_v_sub = st.session_state.df_demo_cube[~st.session_state.df_demo_cube[col_name].isin(EXCLUDE_LIST)]
            grp_v = df_v_sub.groupby(col_name, as_index=False)['total_visitors'].sum()
        
        df_p_sub = df_p_filtered[~df_p_filtered[col_name].isin(EXCLUDE_LIST)]
        grp_p = df_p_sub.groupby(col_name).agg(Purchases=('Order_ID', 'nunique')).reset_index()
        
        # Changed to an OUTER merge
        grp = pd.merge(grp_v, grp_p, on=col_name, how='outer').fillna(0).rename(columns={'total_visitors': 'Visitors'})
        
        # Pad the visitor count with the purchaser count
        grp['Visitors'] = grp['Visitors'] + grp['Purchases']
        
        # 🚨 FILTER BY PURCHASERS INSTEAD OF VISITORS
        grp = grp[grp['Purchases'] >= min_purchasers]
        
        if len(grp) >= 2:
            grp['Conv %'] = (grp['Purchases'] / grp['Visitors']) * 100
            top_row, bot_row = grp.loc[grp['Conv %'].idxmax()], grp.loc[grp['Conv %'].idxmin()]
            predictive_data.append({"Demographic Trait": label, "Top Segment": top_row[col_name], "Conv % (Top)": top_row['Conv %'], "Worst Segment": bot_row[col_name], "Conv % (Worst)": bot_row['Conv %'], "Predictive Swing": top_row['Conv %'] - bot_row['Conv %']})

    if predictive_data:
        pred_df = pd.DataFrame(predictive_data).sort_values("Predictive Swing", ascending=is_ascending)
        pred_df.insert(0, 'Rank', range(1, len(pred_df) + 1))
        
        styler = pred_df.style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Rank': '{:.0f}', 'Conv % (Top)': '{:.2f}%', 'Conv % (Worst)': '{:.2f}%', 'Predictive Swing': '{:.2f}%'}).background_gradient(subset=['Predictive Swing', 'Conv % (Top)', 'Conv % (Worst)'], cmap=brand_gradient)
        render_premium_table(styler)

    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ========================================================
    # 📊 MULTI-VARIABLE COMBINATION MATRIX
    # ========================================================
    st.subheader("📊 Multi-Variable Combination Matrix")
    
    demo_only_configs = [c for c in configs if c[1] != 'state']

    with st.expander("🎛️ Combination Filters", expanded=True):
        selected_filters, included_types = {}, []
        filter_cols = st.columns(3)

        for i, (label, col_name) in enumerate(demo_only_configs):
            with filter_cols[i % 3]:
                c_title, c_inc = st.columns([3, 1])
                c_title.markdown(f'<p style="font-weight: 600; color: {PITCH_BRAND_COLOR}; margin-bottom: 0;">{label}</p>', unsafe_allow_html=True)
                is_inc = c_inc.checkbox("Inc", key=f"inc_{col_name}", help=f"Include {label} in combination matrix")
                
                opts = [x for x in st.session_state.df_demo_cube[col_name].unique() if x not in EXCLUDE_LIST]
                if col_name == 'income_bracket': opts = sorted(opts, key=lambda x: INCOME_MAP.get(x, 99))
                elif col_name == 'net_worth_bracket': opts = sorted(opts, key=lambda x: NET_WORTH_MAP.get(x, 99))
                else: opts = sorted(opts)

                val = st.multiselect(f"Filter {label}", opts, key=f"f_{col_name}", label_visibility="collapsed", placeholder="All")
                if is_inc: included_types.append(col_name)
                if val: selected_filters[col_name] = val

    st.markdown("<br>", unsafe_allow_html=True)
    
if included_types:
        combos = []
        
        # 1. Identify which columns have active filters vs which are unfiltered
        filtered_cols = list(selected_filters.keys())
        unfiltered_cols = [c for c in included_types if c not in filtered_cols]
        
        base_v = st.session_state.df_demo_cube.copy()
        base_p = df_p_filtered.copy()
        
        for col, vals in selected_filters.items():
            base_v = base_v[base_v[col].isin(vals)]
            base_p = base_p[base_p[col].isin(vals)]

        # 2. Max total variables shown should be 3, unless the user explicitly forces more via filters
        max_total_vars = max(3, len(filtered_cols))
        max_extra_vars = max_total_vars - len(filtered_cols)
        
        # If no filters are applied, start combos at length 1. If filters exist, start at length 0 (just the filters).
        min_r = 1 if len(filtered_cols) == 0 else 0
        
        for r in range(min_r, max_extra_vars + 1):
            for subset in itertools.combinations(unfiltered_cols, r):
                # 3. Every combination is built AROUND the explicit filters
                sub_cols = filtered_cols + list(subset)
                
                if not sub_cols:
                    continue
                
                # Process Visitors
                temp_v = base_v.copy()
                for col in sub_cols:
                    temp_v = temp_v[(temp_v[col] != 'ALL') & (~temp_v[col].isin(EXCLUDE_LIST))]
                if temp_v.empty: continue
                grp_v = temp_v.groupby(sub_cols, as_index=False)['total_visitors'].sum()
                
                # Process Purchasers
                temp_p = base_p.copy()
                for col in sub_cols:
                    temp_p = temp_p[~temp_p[col].isin(EXCLUDE_LIST)]
                grp_p = temp_p.groupby(sub_cols).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
                
                # Merge Visitors and Purchasers (OUTER merge)
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='outer').fillna(0).rename(columns={'total_visitors': 'Visitors'})
                
                # Pad the visitor count with the purchaser count
                grp['Visitors'] = grp['Visitors'] + grp['Purchases']
                
                # Blank out unused variables
                for col in included_types:
                    if col not in sub_cols:
                        grp[col] = ""
                        
                combos.append(grp)
                
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'] * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors']).round(2)
            
            # 🚨 FILTER BY PURCHASERS INSTEAD OF VISITORS
            final_res = res[res['Purchases'] >= min_purchasers].sort_values(metric_map[metric_choice], ascending=is_ascending)
            ordered_cols = included_types + ["Visitors", "Purchases", "Revenue", "Conv %", "Rev/Visitor"]
            rename_dict = {c[1]: c[0] for c in configs}
            
            if final_res.empty: st.warning("No combinations met the Minimum Purchases criteria.")
            else:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                
                final_res.insert(0, 'Rank', range(1, len(final_res) + 1))
                final_ordered_cols = ['Rank'] + ordered_cols
                
                styler = final_res.head(50)[final_ordered_cols].rename(columns=rename_dict).style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
                render_premium_table(styler)
