import streamlit as st
import pandas as pd
import matplotlib.colors as mcolors
import numpy as np
import requests
import json
import time
import io

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator" 
PITCH_BRAND_COLOR = "#4D148C" 
AIDAN_WEBHOOK_URL = "https://n8n.srv1144572.hstgr.cloud/webhook/669d6ef0-1393-479e-81c5-5b0bea4262b7"

N8N_COLUMN_MAPPER = {
    "GENDER": "gender", "MARRIED": "marital_status", "AGE_RANGE": "age",
    "INCOME_RANGE": "income", "PERSONAL_STATE": "state_raw", "PERSONAL_ZIP": "zip_code",
    "HOMEOWNER": "homeowner", "CHILDREN": "children", "NET_WORTH": "net_worth",
    "SENIORITY_LEVEL": "seniority", "COMPANY_REVENUE": "co_revenue",
    "COMPANY_EMPLOYEE_COUNT": "co_size", "COMPANY_INDUSTRY": "industry",
    "DEPARTMENT": "department", "JOB_TITLE": "job_title"
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
            @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Playfair+Display:ital,wght@0,600;0,700;0,800;1,600&display=swap');
            html, body, [class*="css"] {{ font-family: 'Outfit', sans-serif; }}
            .stApp {{ background-color: #FAFAFC; }} 
            [data-testid="stHeader"] {{ display: none !important; }}
            #MainMenu {{ visibility: hidden; }}
            footer {{ visibility: hidden; }}
            [data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none !important; }}
            .stMarkdown a svg {{ display: none !important; }}
            div[data-testid="stSlider"] label p {{ font-size: 1.2rem !important; font-weight: 700 !important; color: #0F172A !important; }}
            div[data-testid="stButton"] button {{ border-radius: 8px; font-weight: 600; }}
            div[data-testid="stButton"] button[kind="primary"] {{ background-color: {primary_color} !important; color: #FFFFFF !important; border: none !important; }}
            
            /* 🚨 TABLE CSS RESTORED */
            .premium-table-container {{ width: 100% !important; border-radius: 12px; border: 1px solid {primary_color}; background: #FFFFFF; overflow: hidden; margin-top: 1rem; box-shadow: 0 4px 6px rgba(0,0,0,0.02); }}
            .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; border: none !important; }}
            .premium-table-container th {{ font-family: 'Outfit', sans-serif !important; background-color: #F8F6FA !important; color: {primary_color} !important; font-weight: 700 !important; text-align: center !important; padding: 15px 12px !important; border-bottom: 2px solid {primary_color} !important; font-size: 0.95rem !important; text-transform: none !important; }}
            .premium-table-container td {{ font-family: 'Outfit', sans-serif !important; text-align: center !important; padding: 12px !important; border-bottom: 1px solid #EBE4F4 !important; font-size: 0.9rem !important; color: #1e293b !important; }}
            .premium-table-container th:first-child, .premium-table-container td:first-child {{ text-align: left !important; padding-left: 20px !important; }}
            .premium-table-container td:first-child {{ font-weight: 700 !important; color: #0F172A !important; }}
            
            .serif-gradient-centerpiece {{ font-family: 'Playfair Display', serif !important; background: linear-gradient(90deg, #4D148C 0%, #20B2AA 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: inline-block; font-weight: 700 !important; letter-spacing: -0.5px; }}
            .modern-serif-title {{ font-family: 'Playfair Display', serif !important; color: #0F172A !important; font-weight: 700 !important; }}
            
            /* 🌀 CENTERED SPINNER */
            .custom-loader {{ border: 3px solid #f3f3f3; border-top: 3px solid {primary_color}; border-radius: 50%; width: 32px; height: 32px; animation: spin 1s linear infinite; margin: 0 auto 30px auto; }}
            @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}

            /* 🚀 THE PITCH ROTATOR (80 seconds total, fast start, long hold) */
            @keyframes fadeLoop {{ 
                0% {{ opacity: 0; transform: translateY(5px); }} 
                1%, 24% {{ opacity: 1; transform: translateY(0); }} 
                25%, 100% {{ opacity: 0; transform: translateY(-5px); }} 
            }}
            .pitch-fact {{ font-family: 'Outfit', sans-serif; font-size: 1.15rem; color: #475569; font-weight: 400; font-style: italic; position: absolute; width: 100%; opacity: 0; animation: fadeLoop 80s infinite; line-height: 1.5; text-align: center; }}
            .fact-1 {{ animation-delay: 0s; }}
            .fact-2 {{ animation-delay: 20s; }}
            .fact-3 {{ animation-delay: 40s; }}
            .fact-4 {{ animation-delay: 60s; }}
        </style>
    """, unsafe_allow_html=True)

apply_custom_theme(PITCH_BRAND_COLOR)
brand_gradient = mcolors.LinearSegmentedColormap.from_list("brand_purple", ["#FFFFFF", "#FBF9FC", "#EBE4F4"])

# ================ 2. DATA ENGINE =================
@st.cache_data(show_spinner=False)
def clean_api_response(df):
    df.columns = [str(c).strip().upper() for c in df.columns]
    standard_emails = ['PERSONAL_EMAILS', 'BUSINESS_EMAIL', 'EMAIL_MATCH', 'DEEP_VERIFIED_EMAILS']
    found_email_col = next((col for col in standard_emails if col in df.columns), None)
    if not found_email_col: found_email_col = next((col for col in df.columns if 'EMAIL' in col), None)
    if not found_email_col: return pd.DataFrame(columns=['email_match'])
    df = df.rename(columns={found_email_col: 'email_match'})
    df = df.rename(columns=N8N_COLUMN_MAPPER)
    df.columns = [c.lower() for c in df.columns]
    
    # 🚨 DATA MAPPING FIX (Y/N -> Yes/No, M/F -> Male/Female)
    clean_map = {'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female', 'YES': 'Yes', 'NO': 'No', 'MALE': 'Male', 'FEMALE': 'Female'}
    for col in ['gender', 'homeowner', 'children', 'marital_status']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper().map(clean_map).fillna(df[col])
            
    if 'state_raw' in df.columns: 
        df['region'] = df['state_raw'].str.strip().str.upper().map(STATE_TO_REGION).fillna('Unknown')
    df['email_match'] = df['email_match'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
    df = df.explode('email_match').reset_index(drop=True)
    return df

@st.cache_data(show_spinner=False)
def clean_orders_data(df):
    e_col = next((c for c in df.columns if 'email' in c.lower()), 'Email')
    o_col = next((c for c in df.columns if 'name' in c.lower() or 'order' in c.lower()), 'Order ID')
    t_col = next((c for c in df.columns if 'total' in c.lower() or 'price' in c.lower()), 'Total')
    d_col = next((c for c in df.columns if 'created' in c.lower() or 'date' in c.lower()), 'Date')
    df = df.rename(columns={e_col: 'email_match', o_col: 'order_id', t_col: 'revenue_raw', d_col: 'order_date'})
    df['email_match'] = df['email_match'].astype(str).str.lower().str.strip()
    df['revenue_raw'] = pd.to_numeric(df['revenue_raw'].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)
    df = df[df['revenue_raw'] > 0]
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce', utc=True).dt.date
    return df.dropna(subset=['order_date']).reset_index(drop=True)

def build_dashboard_views(orders_df, enriched_df, start_date, end_date, biz_type):
    mask = (orders_df['order_date'] >= start_date) & (orders_df['order_date'] <= end_date)
    f_orders = orders_df.loc[mask]
    if f_orders.empty: return None
    purchasers = f_orders.groupby('email_match').agg(revenue=('revenue_raw', 'sum')).reset_index()
    df_joined = pd.merge(purchasers, enriched_df, on='email_match', how='inner').reset_index(drop=True)
    if df_joined.empty: return None
    total_rev, matched_count = df_joined['revenue'].sum(), df_joined['email_match'].nunique()
    unique_shopify = f_orders['email_match'].nunique()
    match_rate = (matched_count / unique_shopify * 100) if unique_shopify > 0 else 0
    
    # 🚨 ADDED "CHILDREN" BACK TO DTC VARIABLES
    vars = ([("Industry", "industry"), ("Seniority", "seniority"), ("Company Revenue", "co_revenue"), ("Company Size", "co_size"), ("Department", "department"), ("Job Title", "job_title"), ("Region", "region"), ("State", "state_raw")] if biz_type == "B2B / Enterprise Sales" else [("Gender", "gender"), ("Age", "age"), ("Marital Status", "marital_status"), ("Region", "region"), ("State", "state_raw"), ("Income", "income"), ("Homeowner", "homeowner"), ("Children", "children"), ("Net Worth", "net_worth")])
    top_perf, all_html = {}, {}
    for label, col_key in vars:
        if col_key in df_joined.columns:
            df_joined[col_key] = df_joined[col_key].astype(str).str.strip()
            valid = df_joined[~df_joined[col_key].str.lower().isin(['unknown', 'nan', 'none', 'null', ''])]
            if not valid.empty:
                v_total, rs = valid['revenue'].sum(), valid.groupby(col_key)['revenue'].sum()
                top_perf[label] = (rs.idxmax(), (rs.max() / v_total * 100))
                grp = valid.groupby(col_key).agg(Purchasers=('email_match', 'nunique'), Revenue=('revenue', 'sum')).reset_index()
                grp['% of Buyers'], grp['Rev / Purchaser'] = (grp['Revenue'] / v_total) * 100, (grp['Revenue'] / grp['Purchasers'])
                
                f_v = grp.rename(columns={col_key: label}).sort_values('Revenue', ascending=False).head(50)
                all_html[label] = f_v.style.format({'Purchasers': '{:,.0f}', 'Revenue': '${:,.2f}', '% of Buyers': '{:.1f}%', 'Rev / Purchaser': '${:,.2f}'}).background_gradient(subset=['Revenue', '% of Buyers'], cmap=brand_gradient).hide(axis="index").to_html()
    return {"total_revenue": total_rev, "total_buyers": matched_count, "unique_shopify": unique_shopify, "match_rate": match_rate, "top_performers": top_perf, "html_views": all_html}

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: 
    st.session_state.app_state = "onboarding"
    st.session_state.biz_type = "DTC Ecommerce"

if st.session_state.app_state == "onboarding":
