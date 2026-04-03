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

st.set_page_config(page_title=f"{PITCH_COMPANY_NAME} | Audience Engine", page_icon="🧭", layout="wide", initial_sidebar_state="collapsed")

def apply_custom_theme(primary_color):
    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Playfair+Display:ital,wght@0,600;0,700;0,800;1,600&display=swap');
            html, body, [class*="css"] {{ font-family: 'Outfit', sans-serif; }}
            .stApp {{ background-color: #FAFAFC; }} 
            
            /* Hide Streamlit Watermarks & Badges */
            [data-testid="stHeader"] {{ display: none !important; }}
            #MainMenu {{ visibility: hidden; }}
            footer {{ visibility: hidden; }}
            [data-testid="stSidebar"], [data-testid="collapsedControl"] {{ display: none !important; }}
            .stDeployButton {{ display: none !important; }}
            div[data-testid="viewerBadge"] {{ display: none !important; }}
            /* Nuke the bottom right floating container */
            .stApp > header { display: none !important; }
            .stApp a { display: none !important; }
            div[data-testid="stDecoration"] { display: none !important; }
            div[data-testid="stToolbar"] { display: none !important; }
            #st-actionbox { display: none !important; }
            
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

# Custom HTML Logo block
custom_html_logo = f"""
    <div style="font-family: 'Outfit', sans-serif; font-size: 1.6rem; font-weight: 800; color: #0F172A; letter-spacing: -0.5px; margin-top: 10px; white-space: nowrap;">
        Lead<span style="color: {PITCH_BRAND_COLOR};">Navigator</span>
    </div>
"""

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
    
    if 'credit_rating' not in df.columns:
        c_col = next((c for c in df.columns if 'credit' in c), None)
        if c_col: df = df.rename(columns={c_col: 'credit_rating'})
    
    for col in ['gender', 'homeowner', 'children']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
            df[col] = df[col].replace({'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'})
            
    if 'marital_status' in df.columns:
        df['marital_status'] = df['marital_status'].astype(str).str.strip().str.title()
        df['marital_status'] = df['marital_status'].replace({'Y': 'Married', 'N': 'Single', 'Yes': 'Married', 'No': 'Single'})
            
    if 'state_raw' in df.columns: 
        df['state_raw'] = df['state_raw'].astype(str).str.strip().str.upper()
        df['region'] = df['state_raw'].map(STATE_TO_REGION).fillna('Unknown')
        
    if 'co_state' in df.columns:
        df['co_state'] = df['co_state'].astype(str).str.strip().str.upper()
        df['co_region'] = df['co_state'].map(STATE_TO_REGION).fillna('Unknown')
        
    if 'naics' in df.columns:
        def map_naics(code):
            c = ''.join(filter(str.isdigit, str(code))) 
            if not c or len(c) < 2: return 'Unknown'
            rollup_code = c[:3].ljust(3, '0') 
            prefix = c[:2] 
            
            naics_3_map = {
                '111': 'Crop Production', '112': 'Animal Production', '113': 'Forestry', '114': 'Fishing & Hunting', '115': 'Agriculture Support',
                '211': 'Oil & Gas Extraction', '212': 'Mining', '213': 'Mining Support', '221': 'Utilities',
                '236': 'Building Construction', '237': 'Heavy & Civil Engineering', '238': 'Specialty Trade Contractors',
                '311': 'Food Mfg', '312': 'Beverage & Tobacco Mfg', '313': 'Textile Mills', '314': 'Textile Product Mills', '315': 'Apparel Mfg', '316': 'Leather Mfg',
                '321': 'Wood Product Mfg', '322': 'Paper Mfg', '323': 'Printing', '324': 'Petroleum & Coal Mfg', '325': 'Chemical Mfg', '326': 'Plastics & Rubber Mfg', '327': 'Nonmetallic Mineral Mfg',
                '331': 'Primary Metal Mfg', '332': 'Fabricated Metal Mfg', '333': 'Machinery Mfg', '334': 'Computer & Electronic Mfg', '335': 'Electrical Equipment Mfg', '336': 'Transportation Equip Mfg', '337': 'Furniture Mfg', '339': 'Miscellaneous Mfg',
                '423': 'Merchant Wholesalers (Durable)', '424': 'Merchant Wholesalers (Nondurable)', '425': 'Wholesale Electronic Markets',
                '441': 'Motor Vehicle Dealers', '442': 'Furniture Stores', '443': 'Electronics Stores', '444': 'Building Material Dealers', '445': 'Food & Beverage Stores', '446': 'Health & Personal Care Stores', '447': 'Gasoline Stations', '448': 'Clothing Stores',
                '451': 'Sporting Goods & Book Stores', '452': 'General Merchandise Stores', '453': 'Misc Store Retailers', '454': 'Nonstore Retailers',
                '481': 'Air Transportation', '482': 'Rail Transportation', '483': 'Water Transportation', '484': 'Truck Transportation', '485': 'Transit & Ground Passenger Transport', '486': 'Pipeline Transportation', '488': 'Transportation Support',
                '491': 'Postal Service', '492': 'Couriers & Messengers', '493': 'Warehousing & Storage',
                '511': 'Publishing Industries', '512': 'Motion Picture & Sound Recording', '515': 'Broadcasting', '517': 'Telecommunications', '518': 'Data Processing & Hosting', '519': 'Other Information Services',
                '521': 'Monetary Authorities', '522': 'Credit Intermediation', '523': 'Securities & Commodity Contracts', '524': 'Insurance Carriers', '525': 'Funds & Trusts',
                '531': 'Real Estate', '532': 'Rental & Leasing Services', '533': 'Lessors of Intangible Assets',
                '541': 'Professional, Scientific, & Tech Services', '551': 'Management of Companies',
                '561': 'Administrative & Support Services', '562': 'Waste Management',
                '611': 'Educational Services',
                '621': 'Ambulatory Health Care', '622': 'Hospitals', '623': 'Nursing & Residential Care', '624': 'Social Assistance',
                '711': 'Performing Arts & Sports', '712': 'Museums & Historical Sites', '713': 'Amusement & Recreation',
                '721': 'Accommodation', '722': 'Food Services & Drinking Places',
                '811': 'Repair & Maintenance', '812': 'Personal & Laundry Services', '813': 'Religious & Civic Orgs', '814': 'Private Households',
                '921': 'Exec & Legislative Govt', '922': 'Justice & Public Order', '923': 'Human Resource Programs', '924': 'Environmental Quality', '925': 'Housing & Urban Development', '926': 'Economic Programs', '928': 'National Security'
            }
            naics_2_map = {
                '11': 'Agriculture', '21': 'Mining', '22': 'Utilities', '23': 'Construction', '31': 'Manufacturing', '32': 'Manufacturing', '33': 'Manufacturing',
                '42': 'Wholesale', '44': 'Retail', '45': 'Retail', '48': 'Transportation', '49': 'Transportation', '51': 'Information',
                '52': 'Finance & Insurance', '53': 'Real Estate', '54': 'Professional Services', '55': 'Management', '56': 'Administrative', '61': 'Education',
                '62': 'Health Care', '71': 'Arts & Entertainment', '72': 'Accommodation & Food', '81': 'Other Services', '92': 'Public Admin'
            }
            desc = naics_3_map.get(rollup_code)
            if not desc: desc = naics_2_map.get(prefix, 'Other/Unknown')
            if desc == 'Other/Unknown': return 'Unknown'
            return f"{rollup_code} - {desc}" 
        df['naics'] = df['naics'].apply(map_naics)
        
    df['email_match'] = df['email_match'].astype(str).str.lower().str.replace(r'[^a-z0-9@._,-]', '', regex=True).str.split(',')
    df = df.explode('email_match').reset_index(drop=True)
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

def generate_html_report(dash_data, biz_type):
    html_content = f"""
    <html>
    <head>
        <title>{PITCH_COMPANY_NAME} Executive Report</title>
        <style>
            body {{ font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #0F172A; padding: 40px; background: #FFFFFF; position: relative; }}
            h1 {{ color: {PITCH_BRAND_COLOR}; font-size: 28px; border-bottom: 2px solid {PITCH_BRAND_COLOR}; padding-bottom: 10px; width: 70%; }}
            h2 {{ color: #1e293b; font-size: 22px; margin-top: 40px; }}
            .summary-box {{ background-color: #F8F5FA; border: 1px solid {PITCH_BRAND_COLOR}; border-radius: 8px; padding: 20px; text-align: center; margin-bottom: 30px; }}
            .summary-box h3 {{ margin: 0; font-size: 18px; color: #64748B; text-transform: uppercase; }}
            .summary-box h2 {{ margin: 10px 0 0 0; font-size: 32px; color: {PITCH_BRAND_COLOR}; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 14px; }}
            th {{ background-color: {PITCH_BRAND_COLOR}; color: #FFFFFF; padding: 12px; text-align: center; border: 1px solid #ddd; }}
            td {{ padding: 10px; border: 1px solid #ddd; text-align: center; }}
            td:first-child {{ text-align: left; font-weight: bold; background-color: #F8F5FA; }}
            .report-logo {{ position: absolute; top: 40px; right: 40px; font-size: 24px; font-weight: 800; color: #0F172A; }}
            .report-logo span {{ color: {PITCH_BRAND_COLOR}; }}
        </style>
    </head>
    <body>
        <div class="report-logo">Lead<span>Navigator</span></div>
        <h1>{PITCH_COMPANY_NAME} | Audience Insights Report</h1>
        <div style="display: flex; gap: 20px;">
            <div class="summary-box" style="flex: 1;">
                <h3>Resolved Customers</h3>
                <h2>{dash_data['total_buyers']:,.0f}</h2>
                <p>Match Rate: {dash_data['match_rate']:.1f}%</p>
            </div>
            <div class="summary-box" style="flex: 1;">
                <h3>Attributed Sales</h3>
                <h2>${dash_data['total_revenue']:,.2f}</h2>
            </div>
        </div>
    """
    for label, table_html in dash_data['html_views'].items():
        clean_table = table_html.replace('class="dataframe"', '').replace('id="T_', 'class="')
        html_content += f"<h2>{label} Breakdown</h2>"
        html_content += clean_table
        
    html_content += "</body></html>"
    return html_content

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
    
    if biz_type == "B2B / Enterprise Sales":
        vars = [("Industry", "industry"), ("Seniority", "seniority"), ("Company Revenue", "co_revenue"), ("Company Size", "co_size"), ("Department", "department"), ("Job Title", "job_title"), ("NAICS Code", "naics"), ("Company Region", "co_region"), ("Company State", "co_state")]
    else:
        vars = [("Gender", "gender"), ("Age", "age"), ("Marital Status", "marital_status"), ("Region", "region"), ("State", "state_raw"), ("Income", "income"), ("Homeowner", "homeowner"), ("Children", "children"), ("Net Worth", "net_worth"), ("Credit Rating", "credit_rating")]
        
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
                html = f_v.style.format({'Purchasers': '{:,.0f}', 'Revenue': '${:,.2f}', '% of Buyers': '{:.1f}%', 'Rev / Purchaser': '${:,.2f}'}).background_gradient(subset=['Revenue', '% of Buyers'], cmap=brand_gradient).hide(axis="index").to_html()
                all_html[label] = html
                
    return {"total_revenue": total_rev, "total_buyers": matched_count, "unique_shopify": unique_shopify, "match_rate": match_rate, "top_performers": top_perf, "html_views": all_html}

# ================ 3. APP FLOW =================
if "app_state" not in st.session_state: 
    st.session_state.app_state = "onboarding"
    st.session_state.biz_type = "DTC Ecommerce"

if st.session_state.app_state == "onboarding":
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col:
        st.markdown(custom_html_logo, unsafe_allow_html=True)
        
    st.markdown("""<div style="text-align: center; margin-top: 0px; margin-bottom: 25px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.6rem; margin-bottom: 2px;">Customer Insights Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 1.8rem; color: #0F172A !important; margin-top: 5px;">Upload order data to reveal your customer profile.</h2></div>""", unsafe_allow_html=True)
    _, type_col, _ = st.columns([2, 1, 2])
    st.session_state.biz_type = type_col.selectbox("Business Type Profile", ["DTC Ecommerce", "B2B / Enterprise Sales"])
    st.markdown("<br>", unsafe_allow_html=True)
    _, col1, _ = st.columns([1, 2, 1])
    with col1:
        st.subheader("👥 Customer Data")
        st.session_state.orders_vault = st.file_uploader("Upload Shopify Order Export (CSV) or Order Data that includes the headers: Name, Order ID, Email, Total (Other columns are fine and will be ignored).", type=["csv"], accept_multiple_files=True, key="order_up")
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
                        <p style="color: #64748B; font-family: 'Outfit', sans-serif; margin-bottom: 40px;">Enriching profiles via Identity Graph (Est. 2-3 mins)</p>
                        <div class="custom-loader"></div>
                        <div style="position: relative; height: 120px;">
                            <div class="pitch-fact fact-1">Organizations that leverage customer behavioral insights outperform peers by 85% in sales growth and more than 25% in gross margin.<br><span style="font-style: normal; font-weight: 700; color: #1e293b;">— McKinsey & Co</span></div>
                            <div class="pitch-fact fact-2">Data-driven organizations are 23 times more likely to acquire customers and 6 times more likely to retain them.<br><span style="font-style: normal; font-weight: 700; color: #1e293b;">— MIT Sloan</span></div>
                            <div class="pitch-fact fact-3">Companies that use data-driven marketing are 6x more likely to be profitable year-over-year.<br><span style="font-style: normal; font-weight: 700; color: #1e293b;">— Forbes</span></div>
                            <div class="pitch-fact fact-4">Acquiring a new customer is up to 25x more expensive than retaining one. Knowledge of your current buyer is your greatest asset.<br><span style="font-style: normal; font-weight: 700; color: #1e293b;">— Harvard Business Review</span></div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
                
                raw_df = pd.concat([pd.read_csv(f, encoding='latin1', on_bad_lines='skip') for f in st.session_state.orders_vault], ignore_index=True)
                cleaned_orders = clean_orders_data(raw_df)
                unique_emails = cleaned_orders['email_match'].unique().tolist()
                try:
                    response = requests.post(AIDAN_WEBHOOK_URL, json={"emails": unique_emails}, timeout=180)
                    if response.status_code == 200:
                        raw_enriched_df = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', engine='python')
                        st.session_state.integrity_stats = {"processed": len(raw_enriched_df), "total": len(response.text.strip().split('\n')) - 1}
                        st.session_state.cleaned_n8n = clean_api_response(raw_enriched_df).drop_duplicates(subset=['email_match'])
                        st.session_state.cleaned_orders = cleaned_orders
                        st.session_state.min_date, st.session_state.max_date = cleaned_orders['order_date'].min(), cleaned_orders['order_date'].max()
                        st.session_state.date_filter = (st.session_state.min_date, st.session_state.max_date)
                        
                        if st.session_state.biz_type == "B2B / Enterprise Sales":
                            st.session_state.active_var = "Industry"
                            st.session_state.active_loc_level = "Company Region"
                        else:
                            st.session_state.active_var = "Location"
                            st.session_state.active_loc_level = "Region"
                            
                        st.session_state.app_state = "dashboard"
                        st.rerun()
                    else: st.error(f"Error {response.status_code}")
                except Exception as e: st.error(f"Error: {str(e)}")

elif st.session_state.app_state == "dashboard":
    logo_col, _ = st.columns([1.5, 8.5])
    with logo_col:
        st.markdown(custom_html_logo, unsafe_allow_html=True)
        
    st.markdown(f"""<div style="text-align: center; margin-top: -10px; margin-bottom: 30px;"><h1 class="serif-gradient-centerpiece" style="font-size: 3.5rem; margin-bottom: 0px;">Customer Insights Dashboard.</h1><h2 class="serif-subheadline" style="font-size: 2.8rem; color: #0F172A !important; margin-top: -5px;">Get to know your customers.</h2></div>""", unsafe_allow_html=True)
    
    _, c2, _ = st.columns([1, 4, 1])
    with c2: st.slider("Filter Date", min_value=st.session_state.min_date, max_value=st.session_state.max_date, key="date_filter", format="MMM DD, YYYY")
    current_dates = st.session_state.get("date_filter")
    if current_dates and st.session_state.get("last_computed_dates") != current_dates:
        st.session_state.last_computed_dates = current_dates
        st.session_state.dash_data = build_dashboard_views(st.session_state.cleaned_orders, st.session_state.cleaned_n8n, current_dates[0], current_dates[1], st.session_state.biz_type)
    dash_data = st.session_state.get("dash_data")
    
    if dash_data:
        m1, m2 = st.columns(2)
        with m1: st.markdown(f"""<div style="background-color: #F8F5FA; border: 1px solid {PITCH_BRAND_COLOR}; border-radius: 12px; padding: 25px 20px; text-align: center;"><h3 style="margin: 0; font-size: 1.5rem; color: #0F172A; font-weight: 700;">Resolved Customers</h3><h4 style="margin: 5px 0 15px 0; font-size: 2.2rem; color: {PITCH_BRAND_COLOR}; font-weight: 700;">{dash_data['total_buyers']:,.0f}</h4><p style="margin: 0; font-size: 0.9rem; color: #1e293b;">Matched <b>{dash_data['match_rate']:.1f}%</b> of buyers.</p></div>""", unsafe_allow_html=True)
        with m2: st.markdown(f"""<div style="background-color: #F8F5FA; border: 1px solid {PITCH_BRAND_COLOR}; border-radius: 12px; padding: 25px 20px; text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center;"><h3 style="margin: 0; font-size: 1.5rem; color: #0F172A; font-weight: 700;">Attributed Sales</h3><h4 style="margin: 5px 0 0 0; font-size: 2.2rem; color: {PITCH_BRAND_COLOR}; font-weight: 700;">${dash_data['total_revenue']:,.2f}</h4></div>""", unsafe_allow_html=True)
        
        st.markdown("<div style='margin-top: 4rem;'></div>", unsafe_allow_html=True)
        st.markdown("""<h2 class="modern-serif-title" style="margin-bottom: 2rem; display: flex; align-items: center; gap: 10px;"><span style="font-size: 2rem;">🏆</span> Top Performing Segments</h2>""", unsafe_allow_html=True)
        items = list(dash_data['top_performers'].items())
        for i in range(0, len(items), 5):
            chunk = items[i:i+5]; cols = st.columns(5)
            for j, (label, data) in enumerate(chunk):
                with cols[j]: st.markdown(f'''<div style="background-color: #F8F5FA; border: 1px solid {PITCH_BRAND_COLOR}; border-radius: 12px; padding: 15px; text-align: center; min-height: 120px; display: flex; flex-direction: column; justify-content: center; align-items: center; margin-bottom: 2rem;"><p style="margin: 0; font-size: 1.1rem; color: #0F172A; font-weight: 800; text-transform: uppercase;">{label}</p><h3 style="margin: 8px 0 10px 0; font-size: 0.95rem; color: {PITCH_BRAND_COLOR}; font-weight: 600;">{data[0]}</h3><p style="margin: 0; font-size: 0.85rem; color: {PITCH_BRAND_COLOR}; background-color: #EBE4F4; border-radius: 20px; padding: 4px 10px; display: inline-block; font-weight: 600;">{data[1]:.1f}% of Rev</p></div>''', unsafe_allow_html=True)
        
        st.markdown("<div style='margin-top: 3rem;'></div>", unsafe_allow_html=True)
        st.markdown("""<h2 class="modern-serif-title" style="margin-bottom: 1.5rem; display: flex; align-items: center; gap: 10px;"><span style="font-size: 2rem;">🔍</span> Audience Deep Dive</h2>""", unsafe_allow_html=True)
        
        if st.session_state.biz_type == "B2B / Enterprise Sales":
            v_labels = ["Industry", "Seniority", "Company Revenue", "Company Size", "Department", "Job Title", "Company Location", "NAICS Code"]
        else:
            v_labels = ["Gender", "Age", "Location", "Marital Status", "Income", "Homeowner", "Children", "Net Worth", "Credit Rating"]
            
        for i in range(0, len(v_labels), 5):
            var_cols = st.columns(5)
            for j, label in enumerate(v_labels[i:i+5]):
                if var_cols[j].button(label, key=f"btn_{label}", type="primary" if st.session_state.active_var == label else "secondary", use_container_width=True):
                    st.session_state.active_var = label; st.rerun()
                    
        lk = st.session_state.active_var
        if lk == "Location" or lk == "Company Location":
            st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)
            l1, l2, _ = st.columns([1, 1, 6])
            
            if st.session_state.biz_type == "B2B / Enterprise Sales":
                if l1.button("Company Region", key="co_reg_btn", type="primary" if st.session_state.active_loc_level == "Company Region" else "secondary"): st.session_state.active_loc_level = "Company Region"; st.rerun()
                if l2.button("Company State", key="co_state_btn", type="primary" if st.session_state.active_loc_level == "Company State" else "secondary"): st.session_state.active_loc_level = "Company State"; st.rerun()
                lk = st.session_state.active_loc_level
            else:
                if l1.button("Region", key="reg_btn", type="primary" if st.session_state.active_loc_level == "Region" else "secondary"): st.session_state.active_loc_level = "Region"; st.rerun()
                if l2.button("State", key="state_btn", type="primary" if st.session_state.active_loc_level == "State" else "secondary"): st.session_state.active_loc_level = "State"; st.rerun()
                lk = st.session_state.active_loc_level
        
        if lk in dash_data['html_views']: 
            st.markdown(f'<div class="premium-table-container">{dash_data["html_views"][lk]}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="text-align: center; padding: 40px; background: #F8F6FA; border-radius: 12px; border: 1px dashed {PITCH_BRAND_COLOR}; margin-top: 1rem;">
                    <h4 style="color: #0F172A; font-family: 'Outfit', sans-serif; font-weight: 600; font-size: 1.2rem; margin: 0 0 10px 0;">No Valid Data Available</h4>
                    <p style="color: #64748B; font-size: 0.95rem; margin: 0;">The Identity Graph could not confidently match enough records to generate insights for <b>{lk}</b>.</p>
                </div>
            """, unsafe_allow_html=True)
    
    # 🚨 EXPORT AND GO BACK BUTTONS AT THE BOTTOM
    st.markdown("<br><hr style='border-top: 1px solid #E2E8F0; margin: 2rem 0;'><br>", unsafe_allow_html=True)
    _, action_col, _ = st.columns([2, 1.5, 2])
    
    if "dash_data" in st.session_state and st.session_state.dash_data:
        export_html = generate_html_report(st.session_state.dash_data, st.session_state.biz_type)
        action_col.download_button(
            label="📥 Export Executive Report",
            data=export_html,
            file_name="LeadNavigator_Audience_Report.html",
            mime="text/html",
            use_container_width=True
        )
        
    if action_col.button("← Go Back", use_container_width=True, type="secondary"): 
        st.session_state.app_state = "onboarding"; st.rerun()
