import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" 

# We no longer need Aidan's Webhook URL here because BQ handles it!
BQ_UNIQUE_ORDERS_VIEW = "leadnav-hhs.HHSpixeltest.heavenly_heat_unique_orders"

EXCLUDE_LIST = ['Unknown', 'U', '', 'None', 'nan', 'NaN', 'null', 'NULL', '<NA>', 'ALL']

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
    ("Homeowner", "homeowner_status"),
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

def bucket_income_bq(val):
    # 🚨 THE FIX: Instantly swap " to " for a hyphen so the text matches perfectly
    v = str(val).strip().lower().replace(" to ", " - ")
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    
    under_50k = ['under $10,000', '$10,000 - $14,999', 'less than $20,000', '$20,000 - $24,999', '$25,000 - $29,999', '$30,000 - $34,999', '$35,000 - $39,999', '$40,000 - $44,999', '$45,000 - $49,999', '$45,000 - $59,999']
    from_50k_to_100k = ['$50,000 - $54,999', '$55,000 - $59,999', '$60,000 - $64,999', '$65,000 - $74,999', '$75,000 - $99,999']
    from_100k_to_150k = ['$100,000 - $149,999']
    from_150k_to_250k = ['$150,000 - $174,999', '$150,000 - $199,999', '$175,000 - $199,999', '$200,000 - $249,999']
    over_250k = ['$250,000 +']
    
    if v in under_50k: return 'Under $50k'
    if v in from_50k_to_100k: return '$50k-$100k'
    if v in from_100k_to_150k: return '$100k-$150k'
    if v in from_150k_to_250k: return '$150k-$250k'
    if v in over_250k: return '$250k+'
    return 'Unknown'

def bucket_net_worth_bq(val):
    # 🚨 THE FIX: Instantly swap " to " for a hyphen here too
    v = str(val).strip().lower().replace(" to ", " - ")
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    
    under_100k = ['less than $1', '$1 - $4,999', '$5,000 - $9,999', '$10,000 - $24,999', '$25,000 - $49,999', '$50,000 - $99,999', '-$2,499 - $2,499', '$2,500 - $24,999']
    from_100k_to_249k = ['$100,000 - $249,999', '$150,000 - $249,999']
    from_250k_to_499k = ['$250,000 - $499,999', '$375,000 - $499,999']
    over_500k = ['$499,999 or more', '$750,000 - $999,999']
    
    if v in under_100k: return 'Under $100k'
    if v in from_100k_to_249k: return '$100k-$249k'
    if v in from_250k_to_499k: return '$250k-$499k'
    if v in over_500k: return '$500k+'
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

@st.cache_data(show_spinner=False, ttl=1800) # Caches for 30 mins
def load_order_base():
    """Pulls directly from our smart deduplicating view in BigQuery."""
    client = get_bq_client()
    try:
        df_orders = client.query(f"SELECT * FROM `{BQ_UNIQUE_ORDERS_VIEW}`").to_dataframe()
        if df_orders.empty: return df_orders, None

        # Rename core columns to match what the dashboard engine expects
        df_orders = df_orders.rename(columns={
            'order_id': 'Order_ID',
            'revenue': 'Total',
            'email': 'email_match'
        })
        
        # Ensure date format
        df_orders['order_date'] = pd.to_datetime(df_orders['order_date'], errors='coerce', utc=True).dt.date
        
        # Map BQ demographic names to our normalization engine's inputs
        df_orders = df_orders.rename(columns={
            'age': 'age_range',
            'income': 'income_raw',
            'net_worth': 'net_worth_raw',
            'homeowner': 'homeowner_raw'
        })

        # Run it through the exact same cleaner the Visitor data uses
        df_orders = normalize_demographics(df_orders)
        
        return df_orders, None
    except Exception as e:
        return pd.DataFrame(), str(e)

@st.cache_data(show_spinner=False, ttl=1800)
def load_visitor_base():
    client = get_bq_client()
    try:
        df_demo = client.query("SELECT * FROM `leadnav-hhs.HHSpixeltest.weekly_demographic_summary`").to_dataframe()
        df_state = client.query("SELECT * FROM `leadnav-hhs.HHSpixeltest.weekly_state_summary`").to_dataframe()
        
        df_demo.columns = [c.lower().strip() for c in df_demo.columns]
        df_state.columns = [c.lower().strip() for c in df_state.columns]
        
        df_demo = df_demo.rename(columns={
            'married': 'marital_status',
            'age': 'age_range',
            'income': 'income_bracket',
            'net_worth': 'net_worth_bracket',
            'homeowner': 'homeowner_status'
        })

        if 'gender' in df_demo.columns: df_demo['gender'] = df_demo['gender'].apply(clean_gender)
        if 'children' in df_demo.columns: df_demo['children'] = df_demo['children'].apply(clean_yes_no)
        if 'marital_status' in df_demo.columns: df_demo['marital_status'] = df_demo['marital_status'].apply(clean_marital)
        if 'age_range' in df_demo.columns: df_demo['age_range'] = df_demo['age_range'].apply(clean_age)
        if 'homeowner_status' in df_demo.columns: df_demo['homeowner_status'] = df_demo['homeowner_status'].apply(clean_homeowner_bq)
        if 'income_bracket' in df_demo.columns: df_demo['income_bracket'] = df_demo['income_bracket'].apply(bucket_income_bq)
        if 'net_worth_bracket' in df_demo.columns: df_demo['net_worth_bracket'] = df_demo['net_worth_bracket'].apply(bucket_net_worth_bq)
        
        for col in df_demo.columns:
            if col != 'total_visitors': df_demo[col] = df_demo[col].astype(str).str.strip()
        
        for col in df_state.columns:
            if col != 'total_visitors': 
                df_state[col] = df_state[col].astype(str).str.strip().str.upper()

        df_demo = df_demo.replace(['nan', 'NaN', '<NA>', 'None', 'null', ''], 'ALL').fillna('ALL')
        df_state = df_state.replace(['nan', 'NaN', '<NA>', 'None', 'null', ''], 'ALL').fillna('ALL')

        df_demo = df_demo.groupby(DEMO_COLS, as_index=False)['total_visitors'].sum()
        df_state = df_state.groupby('state', as_index=False)['total_visitors'].sum()
        
        return df_demo, df_state, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: st.session_state.app_state = "onboarding"
if "df_icp" not in st.session_state: st.session_state.df_icp = None

# Custom HTML Logo block
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
            
        # 1. Load Orders directly from BQ View
        st.session_state.df_icp, order_err = load_order_base()
        
        # 2. Load Visitors directly from BQ Tables
        st.session_state.df_demo_cube, st.session_state.df_state_map, visitor_err = load_visitor_base()
        
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
    
    st.info("💡 **Note:** Visitor baselines reflect your historical BigQuery snapshot. The Date Slider filters your live BigQuery Order data.")
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Purchaser Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    
    current_dates = st.session_state.get("date_filter")
    
    st.markdown("<hr style='margin-top: 10px; margin-bottom: 30px;'>", unsafe_allow_html=True)
    st.markdown('### 🎛️ Global Dashboard Controls')
    
    ctrl1, ctrl2, ctrl3, ctrl4 = st.columns(4)
    with ctrl1:
        metric_choice = st.radio("Primary Metric Leaderboard", ["Rev/Visitor", "Conv %", "Revenue", "Purchases", "Visitors"])
    with ctrl2:
        sort_order = st.radio("Ranking Order", ["High to Low", "Low to High"])
        is_ascending = (sort_order == "Low to High")
    with ctrl3:
        min_visitors = st.number_input("Minimum Traffic Floor", value=10)
    with ctrl4:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Force Data Refresh", use_container_width=True): 
            # Clears the cache so it forces a fresh pull from BQ
            load_order_base.clear()
            load_visitor_base.clear()
            st.session_state.app_state = "onboarding"
            st.rerun()
            
    st.markdown("<hr style='margin-top: 10px; margin-bottom: 30px;'>", unsafe_allow_html=True)

    metric_map = {"Conv %": "Conv %", "Purchases": "Purchases", "Revenue": "Revenue", "Visitors": "Visitors", "Rev/Visitor": "Rev/Visitor"}

    df_p_filtered = st.session_state.df_icp[
        (st.session_state.df_icp['order_date'] >= current_dates[0]) & 
        (st.session_state.df_icp['order_date'] <= current_dates[1])
    ].copy()

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
    
    if selected_col == 'state':
        df_v_grp = st.session_state.df_state_map[~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)].copy().rename(columns={'total_visitors': 'Visitors'})
    else:
        mask = (st.session_state.df_demo_cube[selected_col] != 'ALL') & (~st.session_state.df_demo_cube[selected_col].isin(EXCLUDE_LIST))
        for c in DEMO_COLS:
            if c != selected_col: mask &= (st.session_state.df_demo_cube[c] == 'ALL')
        df_v_grp = st.session_state.df_demo_cube[mask][[selected_col, 'total_visitors']].rename(columns={'total_visitors': 'Visitors'})
    
    df_p = df_p_filtered[~df_p_filtered[selected_col].isin(EXCLUDE_LIST)]
    df_p_grp = df_p.groupby(selected_col).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
    
    df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='left').fillna(0)

    if not df_merged.empty:
        df_merged['Conv %'] = (df_merged['Purchases'] / df_merged['Visitors'] * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue'] / df_merged['Visitors']).round(2)
        df_merged = df_merged[df_merged['Visitors'] >= min_visitors].sort_values(metric_map[metric_choice], ascending=is_ascending)
        
        if not df_merged.empty:
            display_df = df_merged.rename(columns={selected_col: st.session_state.active_single_var})
            display_df.insert(0, 'Rank', range(1, len(display_df) + 1))
            styler = display_df.style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)
        else:
            st.warning("No segments met the Minimum Traffic Floor criteria. You can lower the floor in the Global Controls above.")
    else:
        st.warning("No segments met the Minimum Traffic Floor criteria. You can lower the floor in the Global Controls above.")

    st.markdown("<hr>", unsafe_allow_html=True)
    
    # ========================================================
    # 🏆 TOP CONVERSION DRIVERS
    # ========================================================
    st.subheader("🏆 Top Conversion Drivers")
    predictive_data = []
    for label, col_name in configs:
        if col_name == 'state':
            grp_v = st.session_state.df_state_map[~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)].copy()
        else:
            mask = (st.session_state.df_demo_cube[col_name] != 'ALL') & (~st.session_state.df_demo_cube[col_name].isin(EXCLUDE_LIST))
            for c in DEMO_COLS:
                if c != col_name: mask &= (st.session_state.df_demo_cube[c] == 'ALL')
            grp_v = st.session_state.df_demo_cube[mask][[col_name, 'total_visitors']]
        
        df_p_sub = df_p_filtered[~df_p_filtered[col_name].isin(EXCLUDE_LIST)]
        grp_p = df_p_sub.groupby(col_name).agg(Purchases=('Order_ID', 'nunique')).reset_index()
        
        grp = pd.merge(grp_v, grp_p, on=col_name, how='left').fillna(0).rename(columns={'total_visitors': 'Visitors'})
        grp = grp[grp['Visitors'] >= min_visitors]
        
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
        max_combo_size = min(3, len(included_types))
        
        # 🚨 THE FIX: Pre-filter the ENTIRE database before running any combinations
        base_v = st.session_state.df_demo_cube.copy()
        base_p = df_p_filtered.copy()
        
        for col, vals in selected_filters.items():
            base_v = base_v[base_v[col].isin(vals)]
            base_p = base_p[base_p[col].isin(vals)]

        for r in range(1, max_combo_size + 1):
            for subset in itertools.combinations(included_types, r):
                sub_cols = list(subset)
                
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
                
                # Merge Visitors and Purchasers
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='left').fillna(0).rename(columns={'total_visitors': 'Visitors'})
                
                # Keep explicit filters visible, blank out unused variables
                for col in included_types:
                    if col not in sub_cols:
                        if col in selected_filters:
                            grp[col] = ", ".join(selected_filters[col])
                        else:
                            grp[col] = ""
                combos.append(grp)
                
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'] * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors']).round(2)
            
            final_res = res[res['Visitors'] >= min_visitors].sort_values(metric_map[metric_choice], ascending=is_ascending)
            ordered_cols = included_types + ["Visitors", "Purchases", "Revenue", "Conv %", "Rev/Visitor"]
            rename_dict = {c[1]: c[0] for c in configs}
            
            if final_res.empty: st.warning("No combinations met the Traffic Floor minimum.")
            else:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                
                final_res.insert(0, 'Rank', range(1, len(final_res) + 1))
                final_ordered_cols = ['Rank'] + ordered_cols
                
                styler = final_res.head(50)[final_ordered_cols].rename(columns=rename_dict).style.set_properties(**{'font-weight': 'bold'}, subset=['Rank']).format({'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
                render_premium_table(styler)
