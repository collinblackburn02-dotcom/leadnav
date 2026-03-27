import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools
import requests
import io

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" 
AIDAN_WEBHOOK_URL = "https://n8n.srv1144572.hstgr.cloud/webhook/669d6ef0-1393-479e-81c5-5b0bea4262b7"

N8N_COLUMN_MAPPER = {
    "GENDER": "gender", "MARRIED": "marital_status", "AGE_RANGE": "age",
    "INCOME_RANGE": "income", "PERSONAL_STATE": "state_raw", "SKIPTRACE_ZIP": "zip_code",
    "HOMEOWNER": "homeowner", "CHILDREN": "children", "NET_WORTH": "net_worth",
    "SENIORITY_LEVEL": "seniority", "COMPANY_REVENUE": "co_revenue",
    "COMPANY_EMPLOYEE_COUNT": "co_size", "COMPANY_INDUSTRY": "industry",
    "DEPARTMENT": "department", "JOB_TITLE": "job_title", 
    "SKIPTRACE_CREDIT_RATING": "credit_rating",
    "COMPANY_STATE": "co_state", "COMPANY_NAICS": "naics",
    "COMPANY_ZIP": "co_zip_code"
}

STATE_TO_REGION = {
    'CT':'Northeast','MA':'Northeast','ME':'Northeast','NH':'Northeast','NJ':'Northeast','NY':'Northeast','PA':'Northeast','RI':'Northeast','VT':'Northeast',
    'IA':'Midwest','IL':'Midwest','IN':'Midwest','KS':'Midwest','MI':'Midwest','MN':'Midwest','MO':'Midwest','ND':'Midwest','NE':'Midwest','OH':'Midwest','SD':'Midwest','WI':'Midwest',
    'AL':'South','AR':'South','DC':'South','DE':'South','FL':'South','GA':'South','KY':'South','LA':'South','MD':'South','MS':'South','NC':'South','OK':'South','SC':'South','TN':'South','TX':'South','VA':'South','WV':'South',
    'AK':'West','AZ':'West','CA':'West','CO':'West','HI':'West','ID':'West','MT':'West','NM':'West','NV':'West','OR':'West','UT':'West','WA':'West','WY':'West'
}

EXCLUDE_LIST = ['Unknown', 'U', '', 'None', 'nan', 'NaN', 'null', 'NULL', '<NA>']

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

# ================ 2. DATA ENGINE =================
@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(creds_dict), project=creds_dict["project_id"])

def bucket_income(val):
    val_str = str(val).lower()
    if val in EXCLUDE_LIST or val_str in ['unknown', 'u', 'none', 'nan']: return 'Unknown'
    if any(x in val_str for x in ['less than', 'under', '$20,000 to $44', '$45,000 to $59']): return '$0-$59,999'
    if any(x in val_str for x in ['$60,000 to $74', '$75,000 to $99']): return '$60,000-$99,999'
    if any(x in val_str for x in ['$100,000 to $149', '$150,000 to $199']): return '$100,000-$199,999'
    if any(x in val_str for x in ['$200,000', '$250,000']): return '$200,000+'
    return val

def bucket_net_worth(val):
    val_str = str(val).lower()
    if val in EXCLUDE_LIST or val_str in ['unknown', 'u', 'none', 'nan']: return 'Unknown'
    if any(x in val_str for x in ['-$', 'less than', '$2,500 to $24', '$25,000 to $49', 'under']): return '$49,999 and below'
    if any(x in val_str for x in ['$50,000 to $74', '$75,000 to $99']): return '$50,000-$99,999'
    if any(x in val_str for x in ['$100,000 to $149', '$150,000 to $249']): return '$100,000-$249,999'
    if any(x in val_str for x in ['$250,000 to $374', '$375,000 to $499']): return '$250,000-$499,999'
    if any(x in val_str for x in ['$500,000 to $74', '$750,000 to $999']): return '$500,000-$999,999'
    if '1,000,000' in val_str or 'million' in val_str: return '$1,000,000+'
    return val

def normalize_demographics(df):
    """Ensures BigQuery (Visitors) and n8n (Purchasers) use the EXACT same categories"""
    for col in ['gender', 'homeowner', 'children']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            df[col] = df[col].replace({'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female', 'True': 'Yes', 'False': 'No'})
            
    if 'marital_status' in df.columns:
        df['marital_status'] = df['marital_status'].astype(str).str.strip().str.title()
        df['marital_status'] = df['marital_status'].replace({'Y': 'Married', 'N': 'Single', 'Yes': 'Married', 'No': 'Single', 'True': 'Married', 'False': 'Single'})

    if 'credit_rating' in df.columns:
        df['credit_rating'] = df['credit_rating'].replace({
            'A': 'High (A, B, C)', 'B': 'High (A, B, C)', 'C': 'High (A, B, C)',
            'D': 'Medium (D, E)', 'E': 'Medium (D, E)',
            'F': 'Low (F, G)', 'G': 'Low (F, G)'
        })

    if 'income' in df.columns: df['income'] = df['income'].apply(bucket_income)
    if 'net_worth' in df.columns: df['net_worth'] = df['net_worth'].apply(bucket_net_worth)

    for col in df.columns:
        df[col] = df[col].replace(["", "nan", "NaN", "None", "null", "NULL", "<NA>"], "Unknown")
        
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
    """Takes the n8n webhook response for purchasers and formats it"""
    df.columns = [str(c).strip().upper() for c in df.columns]
    standard_emails = ['PERSONAL_EMAILS', 'BUSINESS_EMAIL', 'EMAIL_MATCH', 'DEEP_VERIFIED_EMAILS']
    found_email_col = next((col for col in standard_emails if col in df.columns), None)
    if not found_email_col: found_email_col = next((col for col in df.columns if 'EMAIL' in col), None)
    if not found_email_col: return pd.DataFrame(columns=['email_match'])
    
    df = df.rename(columns={found_email_col: 'email_match'})
    df = df.rename(columns=N8N_COLUMN_MAPPER)
    df.columns = [c.lower() for c in df.columns]
    
    if 'state_raw' in df.columns: 
        df['state_raw'] = df['state_raw'].astype(str).str.strip().str.upper()
        df['region'] = df['state_raw'].map(STATE_TO_REGION).fillna('Unknown')
        
    df = normalize_demographics(df)
        
    df['email_match'] = df['email_match'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
    df = df.explode('email_match').reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False)
def load_visitor_base(start_date, end_date):
    """Queries BigQuery for Visitors dynamically based on the Date Slider"""
    client = get_bq_client()
    # 🚨 FIX: We are now using event_timestamp to filter and pixel_id to count unique visits
    query = f"""
        SELECT 
            GENDER as gender, AGE_RANGE as age, INCOME_RANGE as income, 
            CASE 
                WHEN PERSONAL_STATE IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA') THEN 'Northeast'
                WHEN PERSONAL_STATE IN ('IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI') THEN 'Midwest'
                WHEN PERSONAL_STATE IN ('AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX', 'VA', 'WV', 'DC') THEN 'South'
                WHEN PERSONAL_STATE IN ('AK', 'AZ', 'CA', 'CO', 'HI', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WA', 'WY') THEN 'West'
                ELSE 'Unknown'
            END as region,
            PERSONAL_STATE as state_raw,
            NET_WORTH as net_worth, CHILDREN as children, MARRIED as marital_status, 
            HOMEOWNER as homeowner, SKIPTRACE_CREDIT_RATING as credit_rating,
            COUNT(DISTINCT pixel_id) as total_visitors
        FROM `xenon-mantis-430216-n4.visitors_raw.all_visitors_combined`
        WHERE DATE(event_timestamp) >= '{start_date}' AND DATE(event_timestamp) <= '{end_date}'
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
    try:
        df = client.query(query).to_dataframe()
        df = normalize_demographics(df)
        df['total_visitors'] = pd.to_numeric(df['total_visitors'], errors='coerce').fillna(0)
        return df
    except Exception as e:
        st.error(f"Failed to fetch BigQuery Visitors: {e}")
        return pd.DataFrame()

configs = [("Gender", "gender"), ("Age", "age"), ("Income", "income"), ("Region", "region"), ("State", "state_raw"), ("Net Worth", "net_worth"), ("Children", "children"), ("Marital Status", "marital_status"), ("Homeowner", "homeowner"), ("Credit Rating", "credit_rating")]
INCOME_MAP = {'$0-$59,999': 1, '$60,000-$99,999': 2, '$100,000-$199,999': 3, '$200,000+': 4}
NET_WORTH_MAP = {'$49,999 and below': 1, '$50,000-$99,999': 2, '$100,000-$249,999': 3, '$250,000-$499,999': 4, '$500,000-$999,999': 5, '$1,000,000+': 6}
CREDIT_MAP = {'High (A, B, C)': 1, 'Medium (D, E)': 2, 'Low (F, G)': 3}

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
                        <div style="position: relative; height: 120px;">
                            <div class="pitch-fact fact-1">Organizations that leverage customer behavioral insights outperform peers by 85% in sales growth and more than 25% in gross margin.</div>
                            <div class="pitch-fact fact-2">Data-driven organizations are 23 times more likely to acquire customers and 6 times more likely to retain them.</div>
                            <div class="pitch-fact fact-3">Companies that use data-driven marketing are 6x more likely to be profitable year-over-year.</div>
                            <div class="pitch-fact fact-4">Understanding traffic flow eliminates wasted ad spend and instantly lowers your Customer Acquisition Cost.</div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                
                raw_df = pd.concat([pd.read_csv(f, encoding='latin1', on_bad_lines='skip') for f in st.session_state.orders_vault], ignore_index=True)
                cleaned_orders = clean_orders_data(raw_df)
                unique_emails = cleaned_orders['email_match'].unique().tolist()
                
                try:
                    # 🚨 TRACK 1: GET PURCHASER ENRICHMENT FROM N8N WEBHOOK
                    response = requests.post(AIDAN_WEBHOOK_URL, json={"emails": unique_emails}, timeout=180)
                    if response.status_code == 200:
                        raw_enriched_df = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', engine='python')
                        df_n8n_clean = clean_api_purchasers(raw_enriched_df).drop_duplicates(subset=['email_match'])
                        
                        # Merge Purchasers with their Shopify Order Totals & Dates
                        purchasers_totals = cleaned_orders.groupby('email_match').agg(Total=('revenue_raw', 'sum'), Order_ID=('order_id', 'first'), order_date=('order_date', 'min')).reset_index()
                        st.session_state.df_icp = pd.merge(purchasers_totals, df_n8n_clean, on='email_match', how='inner').reset_index(drop=True)
                        
                        st.session_state.min_date = cleaned_orders['order_date'].min()
                        st.session_state.max_date = cleaned_orders['order_date'].max()
                        st.session_state.date_filter = (st.session_state.min_date, st.session_state.max_date)
                        
                        st.session_state.app_state = "dashboard"
                        st.rerun()
                    else: st.error(f"Error {response.status_code}")
                except Exception as e: st.error(f"Error: {str(e)}")

elif st.session_state.app_state == "dashboard":
    st.image("logo.png", width=180)
    st.markdown(f"""<div style="text-align: center; margin-top: -10px; margin-bottom: 30px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.5rem; margin-bottom: 0px;">Conversion Analytics Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 2.8rem; color: #0F172A !important; margin-top: -5px;">Optimize your traffic funnel.</h2></div>""", unsafe_allow_html=True)
    
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    
    current_dates = st.session_state.get("date_filter")
    
    # 🚨 TRACK 2: GET VISITORS FROM BIGQUERY (Re-runs when date slider changes)
    if "last_computed_dates" not in st.session_state or st.session_state.last_computed_dates != current_dates:
        with st.spinner("Fetching Visitor Traffic from BigQuery..."):
            st.session_state.df_visitors = load_visitor_base(current_dates[0], current_dates[1])
            st.session_state.last_computed_dates = current_dates
    
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

    # Filter Purchasers locally by the date slider
    df_p_filtered = st.session_state.df_icp[
        (st.session_state.df_icp['order_date'] >= current_dates[0]) & 
        (st.session_state.df_icp['order_date'] <= current_dates[1])
    ]

    st.markdown('<p style="font-size: 2rem; font-weight: 700; margin-bottom: 0px;">Audience Insights Engine</p>', unsafe_allow_html=True)
    st.markdown('<p style="color: #64748B; margin-top: -5px; margin-bottom: 30px;">Traffic and Conversion Optimization</p>', unsafe_allow_html=True)
    
    st.subheader("🔍 Single Variable Deep Dive")
    if "active_single_var" not in st.session_state: st.session_state.active_single_var = "Gender"
    
    # 🚨 DYNAMIC BUTTONS FOR THE LEADNAV UI
    for i in range(0, len(configs), 5):
        var_cols = st.columns(5)
        for j, (label, col_name) in enumerate(configs[i:i+5]):
            if var_cols[j].button(label, key=f"btn_{label}_t1", type="primary" if st.session_state.active_single_var == label else "secondary", use_container_width=True):
                st.session_state.active_single_var = label
                st.rerun()
                
    selected_col = dict(configs)[st.session_state.active_single_var]
    
    # 🚨 COMBINATION MATH (BigQuery Visitors vs n8n Purchasers)
    df_v = st.session_state.df_visitors[~st.session_state.df_visitors[selected_col].isin(EXCLUDE_LIST)]
    df_v_grp = df_v.groupby(selected_col)['total_visitors'].sum().reset_index().rename(columns={'total_visitors': 'Visitors'})
    
    df_p = df_p_filtered[~df_p_filtered[selected_col].isin(EXCLUDE_LIST)]
    df_p_grp = df_p.groupby(selected_col).agg(Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')).reset_index()
    
    df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='left').fillna(0)

    if not df_merged.empty:
        df_merged['Conv %'] = (df_merged['Purchases'] / df_merged['Visitors'] * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue'] / df_merged['Visitors']).round(2)
        df_merged = df_merged[df_merged['Visitors'] >= min_visitors].sort_values(metric_map[metric_choice], ascending=is_ascending)
        display_df = df_merged.rename(columns={selected_col: st.session_state.active_single_var})
        
        styler = display_df.style.format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
        render_premium_table(styler)

    st.markdown("<hr>", unsafe_allow_html=True)
    
    st.subheader("🏆 Top Conversion Drivers")
    predictive_data = []
    for label, col_name in configs:
        df_v_sub = st.session_state.df_visitors[~st.session_state.df_visitors[col_name].isin(EXCLUDE_LIST)]
        grp_v = df_v_sub.groupby(col_name)['total_visitors'].sum().reset_index()
        
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
    st.subheader("📊 Multi-Variable Combination Matrix")

    with st.expander("🎛️ Combination Filters", expanded=True):
        selected_filters, included_types = {}, []
        filter_cols = st.columns(3)

        for i, (label, col_name) in enumerate(configs):
            with filter_cols[i % 3]:
                c_title, c_inc = st.columns([3, 1])
                c_title.markdown(f'<p style="font-weight: 600; color: {PITCH_BRAND_COLOR}; margin-bottom: 0;">{label}</p>', unsafe_allow_html=True)
                is_inc = c_inc.checkbox("Inc", key=f"inc_{col_name}", help=f"Include {label}")
                
                opts = [x for x in st.session_state.df_visitors[col_name].unique() if x not in EXCLUDE_LIST]
                if col_name == 'income': opts = sorted(opts, key=lambda x: INCOME_MAP.get(x, 99))
                elif col_name == 'net_worth': opts = sorted(opts, key=lambda x: NET_WORTH_MAP.get(x, 99))
                elif col_name == 'credit_rating': opts = sorted(opts, key=lambda x: CREDIT_MAP.get(x, 99))
                else: opts = sorted(opts)

                val = st.multiselect(f"Filter {label}", opts, key=f"f_{col_name}", label_visibility="collapsed", placeholder="All")
                if is_inc: included_types.append(col_name)
                if val: selected_filters[col_name] = val

    dff_v = st.session_state.df_visitors.copy()
    dff_p = df_p_filtered.copy()
    
    for col, vals in selected_filters.items(): 
        dff_v = dff_v[dff_v[col].isin(vals)]
        dff_p = dff_p[dff_p[col].isin(vals)]

    for col in included_types:
        dff_v = dff_v[~dff_v[col].isin(EXCLUDE_LIST)]
        dff_p = dff_p[~dff_p[col].isin(EXCLUDE_LIST)]

    st.markdown("<br>", unsafe_allow_html=True)
    if not dff_v.empty and (selected_filters or included_types):
        total_vis = dff_v['total_visitors'].sum()
        total_purch = dff_p['Order_ID'].nunique()
        total_rev = dff_p['Total'].sum()
        avg_conv = (total_purch / total_vis * 100) if total_vis > 0 else 0
        avg_rev_vis = (total_rev / total_vis) if total_vis > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Filtered Segment Visitors", f"{total_vis:,.0f}")
        m2.metric("Segment Purchases", f"{total_purch:,.0f}")
        m3.metric("Segment Conv Rate", f"{avg_conv:.2f}%")
        m4.metric("Segment Rev / Visitor", f"${avg_rev_vis:,.2f}")
        st.markdown("<br>", unsafe_allow_html=True)

    if included_types and not dff_v.empty:
        combos = []
        max_combo_size = min(3, len(included_types))
        
        for r in range(1, max_combo_size + 1):
            for subset in itertools.combinations(included_types, r):
                sub_cols = list(subset)
                temp_v, temp_p = dff_v.copy(), dff_p.copy()
                
                for col in sub_cols:
                    temp_v = temp_v[~temp_v[col].isin(EXCLUDE_LIST)]
                    temp_p = temp_p[~temp_p[col].isin(EXCLUDE_LIST)]
                    
                if temp_v.empty: continue
                    
                grp_v = temp_v.groupby(sub_cols)['total_visitors'].sum().reset_index()
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
                styler = final_res.head(50)[ordered_cols].rename(columns=rename_dict).style.format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'}).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
                render_premium_table(styler)
