import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools
import requests
import io
import re

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" 
AIDAN_WEBHOOK_URL = "https://n8n.srv1144572.hstgr.cloud/webhook/669d6ef0-1393-479e-81c5-5b0bea4262b7"

N8N_COLUMN_MAPPER = {
    "GENDER": "gender", "MARRIED": "marital_status", "AGE_RANGE": "age_range",
    "INCOME_RANGE": "income_raw", "PERSONAL_STATE": "state", 
    "HOMEOWNER": "homeowner_raw", "CHILDREN": "children", "NET_WORTH": "net_worth_raw"
}

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
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; text-align: left !important; padding-left: 20px !important; }}
            
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

# ================ 2. HYPER-RESILIENT DATA ENGINE =================
DEMO_COLS = ['gender', 'age_range', 'marital_status', 'children', 'homeowner_status', 'income_bracket', 'net_worth_bracket']
configs = [("Gender", "gender"), ("Age", "age_range"), ("Income", "income_bracket"), ("State", "state"), ("Net Worth", "net_worth_bracket"), ("Children", "children"), ("Marital Status", "marital_status"), ("Homeowner", "homeowner_status")]

INCOME_MAP = {'Under $50k': 1, '$50k-$100k': 2, '$100k-$150k': 3, '$150k+': 4}
NET_WORTH_MAP = {'Under $100k': 1, '$100k-$249k': 2, '$250k-$499k': 3, '$500k+': 4}

@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])

# 🚨 THE FIX: All functions now respect "ALL" so BigQuery CUBEs aren't accidentally destroyed
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
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    nums = [int(n) for n in re.findall(r'\d+', v.replace(',', ''))]
    if not nums: return 'Unknown'
    lower = nums[0]
    if lower < 50000: return 'Under $50k'
    if 50000 <= lower < 100000: return '$50k-$100k'
    if 100000 <= lower < 150000: return '$100k-$150k'
    if lower >= 150000: return '$150k+'
    return 'Unknown'

def bucket_net_worth_bq(val):
    v = str(val).strip().lower()
    if v in ['all', 'none', 'nan', '<na>', 'null', '']: return 'ALL'
    nums = [int(n) for n in re.findall(r'\d+', v.replace(',', ''))]
    if not nums: return 'Unknown'
    lower = nums[0]
    if lower < 100000: return 'Under $100k'
    if 100000 <= lower < 250000: return '$100k-$249k'
    if 250000 <= lower < 500000: return '$250k-$499k'
    if lower >= 500000: return '$500k+'
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

@st.cache_data(show_spinner=False)
def clean_orders_data(df):
    cols_lower = [str(c).strip().lower() for c in df.columns]
    e_col = df.columns[cols_lower.index('email')] if 'email' in cols_lower else next((c for c in df.columns if 'email' in str(c).lower()), 'Email')
    o_col = df.columns[cols_lower.index('name')] if 'name' in cols_lower else next((c for c in df.columns if 'order' in str(c).lower()), 'Order ID')
    t_col = df.columns[cols_lower.index('total')] if 'total' in cols_lower else next((c for c in df.columns if 'total' in str(c).lower()), 'Total')
    d_col = df.columns[cols_lower.index('created at')] if 'created at' in cols_lower else next((c for c in df.columns if 'date' in str(c).lower()), 'Date')

    df = df.rename(columns={e_col: 'email_match', o_col: 'order_id', t_col: 'revenue_raw', d_col: 'order_date'})
    df['email_match'] = df['email_match'].astype(str).str.lower().str.strip()
    df = df[df['email_match'].str.contains('@', na=False)] 
    df['revenue_raw'] = pd.to_numeric(df['revenue_raw'].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
    df = df[df['revenue_raw'] > 0]
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', utc=True).dt.date
    df['order_id'] = df['order_id'].astype(str).str.strip()
    return df.dropna(subset=['order_date']).drop_duplicates(subset=['order_id']).reset_index(drop=True)

@st.cache_data(show_spinner=False)
def clean_api_purchasers(df):
    df.columns = [str(c).strip().upper() for c in df.columns]
    standard_emails = ['PERSONAL_EMAILS', 'BUSINESS_EMAIL', 'EMAIL_MATCH', 'DEEP_VERIFIED_EMAILS']
    found_email_col = next((col for col in standard_emails if col in df.columns), None)
    if not found_email_col: found_email_col = next((col for col in df.columns if 'EMAIL' in col), None)
    if not found_email_col: return pd.DataFrame(columns=['email_match'])
    
    df = df.rename(columns={found_email_col: 'email_match'})
    df = df.rename(columns=N8N_COLUMN_MAPPER)
    df.columns = [c.lower() for c in df.columns]
        
    df = normalize_demographics(df)
        
    df['email_match'] = df['email_match'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
    df = df.explode('email_match').reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False, ttl=3600)
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
            if col != 'total_visitors': df_state[col] = df_state[col].astype(str).str.strip()

        df_demo = df_demo.groupby(DEMO_COLS, as_index=False)['total_visitors'].sum()
        df_state = df_state.groupby('state', as_index=False)['total_visitors'].sum()
        
        return df_demo, df_state, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: st.session_state.app_state = "onboarding"
if "df_icp" not in st.session_state: st.session_state.df_icp = None

if st.session_state.app_state == "onboarding":
    st.image("logo.png", width=180)
    st.markdown("""<div style="text-align: center; margin-top: 0px; margin-bottom: 25px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.6rem; margin-bottom: 2px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 1.8rem; color: #0F172A !important; margin-top: 5px;">Upload order data to build your conversion matrix.</h2></div>""", unsafe_allow_html=True)
    
    _, col1, _ = st.columns([1, 2, 1])
    with col1:
        st.subheader("👥 Order Data")
        st.session_state.orders_vault = st.file_uploader("Upload Shopify Order Export (CSV) or Order Data that includes the headers: Name, Order ID, Email, Total.", type=["csv"], accept_multiple_files=True, key="order_up")
    st.markdown("<br>", unsafe_allow_html=True)
    _, center_col, _ = st.columns([2, 1.5, 2])
    
    if center_col.button("🚀 Run Analysis", type="primary", use_container_width=True):
        if not st.session_state.orders_vault: st.error("Please upload your order file.")
        else:
            status_placeholder = st.empty()
            with status_placeholder:
                st.markdown(f"""
                    <div style="text-align: center; padding: 60px 40px; background: #F8F6FA; border-radius: 12px; border: 1px solid {PITCH_BRAND_COLOR}; min-height: 380px;">
                        <h3 class="modern-serif-title" style="color: {PITCH_BRAND_COLOR}; margin-bottom: 10px;">LeadNavigator Intelligence is active...</h3>
                        <p style="color: #64748B; font-family: 'Outfit', sans-serif; margin-bottom: 40px;">Processing multi-touch attribution metrics (Est. 2-3 mins)</p>
                        <div class="custom-loader"></div>
                    </div>
                """, unsafe_allow_html=True)
                
                raw_df = pd.concat([pd.read_csv(f, encoding='latin1', on_bad_lines='skip') for f in st.session_state.orders_vault], ignore_index=True)
                cleaned_orders = clean_orders_data(raw_df)
                unique_emails = cleaned_orders['email_match'].unique().tolist()
                
                try:
                    response = requests.post(AIDAN_WEBHOOK_URL, json={"emails": unique_emails}, timeout=180)
                    if response.status_code == 200:
                        raw_enriched_df = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', engine='python')
                        df_n8n_clean = clean_api_purchasers(raw_enriched_df).drop_duplicates(subset=['email_match'])
                        
                        purchasers_totals = cleaned_orders.groupby('email_match').agg(Total=('revenue_raw', 'sum'), Order_ID=('order_id', 'first'), order_date=('order_date', 'min')).reset_index()
                        st.session_state.df_icp = pd.merge(purchasers_totals, df_n8n_clean, on='email_match', how='inner').reset_index(drop=True)
                        
                        st.session_state.min_date = cleaned_orders['order_date'].min()
                        st.session_state.max_date = cleaned_orders['order_date'].max()
                        st.session_state.date_filter = (st.session_state.min_date, st.session_state.max_date)
                        
                        st.session_state.df_demo_cube, st.session_state.df_state_map, bq_error = load_visitor_base()
                        
                        if bq_error:
                            st.error(f"🚨 BIGQUERY CONNECTION ERROR: {bq_error}")
                            st.stop()
                        if 'gender' not in st.session_state.df_demo_cube.columns:
                            st.error(f"🚨 SQL ERROR: Columns mismatch. Found: {st.session_state.df_demo_cube.columns.tolist()}")
                            st.stop()
                        
                        st.session_state.app_state = "dashboard"
                        st.rerun()
                    else: st.error(f"Error {response.status_code}")
                except Exception as e: st.error(f"Error: {str(e)}")

elif st.session_state.app_state == "dashboard":
    st.image("logo.png", width=180)
    st.markdown(f"""<div style="text-align: center; margin-top: -10px; margin-bottom: 30px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.5rem; margin-bottom: 0px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 2.8rem; color: #0F172A !important; margin-top: -5px;">Optimize your traffic funnel.</h2></div>""", unsafe_allow_html=True)
    
    st.info("💡 **Note:** Visitor baselines reflect your historical BigQuery snapshot. The Date Slider filters your uploaded Purchaser order data.")
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Purchaser Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    
    current_dates = st.session_state.get("date_filter")
    
    with st.sidebar:
        st.markdown(f"<h2 style='color: {PITCH_BRAND_COLOR}; text-align: center; margin-bottom: 0;'>🎯 LeadNavigator</h2>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #888; font-size: 0.8rem;'>Conversion Engine</p>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.header("Global Controls")
        sort_order = st.radio("Ranking Order", ["High to Low", "Low to High"], horizontal=True)
        is_ascending = (sort_order == "Low to High")
        metric_choice = st.radio("Primary Metric Leaderboard", ["Rev/Visitor", "Conv %", "Revenue", "Purchases", "Visitors"])
        min_visitors = st.number_input("Minimum Traffic Floor", value=250)
        st.markdown("<br><br>", unsafe_allow_html=True)
        if st.button("🔄 Upload New Orders", use_container_width=True): 
            st.session_state.app_state = "onboarding"
            st.rerun()

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
            styler = display_df.style.format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)
        else:
            st.warning("No segments met the Minimum Traffic Floor criteria. You can lower the floor on the left sidebar.")
    else:
        st.warning("No segments met the Minimum Traffic Floor criteria. You can lower the floor on the left sidebar.")

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
        styler = pred_df.style.format({'Conv % (Top)': '{:.2f}%', 'Conv % (Worst)': '{:.2f}%', 'Predictive Swing': '{:.2f}%'}).background_gradient(subset=['Predictive Swing', 'Conv % (Top)'], cmap=brand_gradient).background_gradient(subset=['Conv % (Worst)'], cmap=mcolors.LinearSegmentedColormap.from_list("custom_purple_r", ["#4D148C", "#FBF9FC", "#FFFFFF"]))
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
                is_inc = c_inc.checkbox("Inc", key=f"inc_{col_name}", help=f"Include {label}")
                
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
        
        for r in range(1, max_combo_size + 1):
            for subset in itertools.combinations(included_types, r):
                sub_cols = list(subset)
                
                mask = pd.Series(True, index=st.session_state.df_demo_cube.index)
                for col in DEMO_COLS:
                    if col in sub_cols: mask &= (st.session_state.df_demo_cube[col] != 'ALL') & (~st.session_state.df_demo_cube[col].isin(EXCLUDE_LIST))
                    else: mask &= (st.session_state.df_demo_cube[col] == 'ALL')
                
                for col, vals in selected_filters.items(): 
                    if col in sub_cols: mask &= st.session_state.df_demo_cube[col].isin(vals)
                        
                temp_v = st.session_state.df_demo_cube[mask].copy()
                if temp_v.empty: continue
                
                temp_p = df_p_filtered.copy()
                for col in sub_cols:
                    temp_p = temp_p[~temp_p[col].isin(EXCLUDE_LIST)]
                    if col in selected_filters: temp_p = temp_p[temp_p[col].isin(selected_filters[col])]
                    
                grp_v = temp_v[sub_cols + ['total_visitors']]
                grp_p = temp_p.groupby(sub_cols).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='left').fillna(0).rename(columns={'total_visitors': 'Visitors'})
                
                for col in included_types:
                    if col not in sub_cols:
                        grp[col] = ", ".join(selected_filters[col]) if col in selected_filters and selected_filters[col] else ""
                combos.append(grp)
                
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'] * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors']).round(2)
            
            final_res = res[res['Visitors'] >= min_visitors].sort_values(metric_map[metric_choice], ascending=is_ascending)
            ordered_cols = included_types + ["Visitors", "Purchases", "Revenue", "Conv %", "Rev/Visitor"]
            rename_dict = {c[1]: c[0] for c in configs}
            
            if final_res.empty:
                st.warning(f"No combinations met the Traffic Floor minimum.")
            else:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                styler = final_res.head(50)[ordered_cols].rename(columns=rename_dict).style.format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
                render_premium_table(styler)
                
                temp_p = df_p_filtered.copy()
                for col in sub_cols:
                    temp_p = temp_p[~temp_p[col].isin(EXCLUDE_LIST)]
                    if col in selected_filters: temp_p = temp_p[temp_p[col].isin(selected_filters[col])]
                    
                grp_v = temp_v[sub_cols + ['total_visitors']]
                grp_p = temp_p.groupby(sub_cols).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
                grp = pd.merge(grp_v, grp_p, on=sub_cols, how='left').fillna(0).rename(columns={'total_visitors': 'Visitors'})
                
                for col in included_types:
                    if col not in sub_cols:
                        grp[col] = ", ".join(selected_filters[col]) if col in selected_filters and selected_filters[col] else ""
                combos.append(grp)
                
        if combos:
            res = pd.concat(combos, ignore_index=True).drop_duplicates(subset=included_types)
            res['Conv %'] = (res['Purchases'] / res['Visitors'] * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue'] / res['Visitors']).round(2)
            
            final_res = res[res['Visitors'] >= min_visitors].sort_values(metric_map[metric_choice], ascending=is_ascending)
            ordered_cols = included_types + ["Visitors", "Purchases", "Revenue", "Conv %", "Rev/Visitor"]
            rename_dict = {c[1]: c[0] for c in configs}
            
            if final_res.empty:
                st.warning(f"No combinations met the Traffic Floor minimum.")
            else:
                st.metric("Total Segments Found", f"{len(final_res):,}")
                styler = final_res.head(50)[ordered_cols].rename(columns=rename_dict).style.format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
                render_premium_table(styler)
