import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.colors as mcolors
import itertools
import re
import requests
import hashlib
from datetime import datetime, timedelta
import io

# ================ 1. CONFIGURATION & THEME =================
PITCH_COMPANY_NAME = "LeadNavigator"
PITCH_BRAND_COLOR = "#4D148C"
N8N_WEBHOOK_URL = "https://n8n.srv1144572.hstgr.cloud/webhook/669d6ef0-1393-479e-81c5-5b0bea4262b7"

BQ_B2C_VISITOR_TABLE = "leadnav-hhs.leadnav_platform.b2c_visitor_summary"
BQ_B2B_VISITOR_TABLE = "leadnav-hhs.leadnav_platform.b2b_visitor_summary"
BQ_ORDERS_TABLE      = "leadnav-hhs.leadnav_platform.platform_order_data"

EXCLUDE_LIST = ['Unknown', 'UNKNOWN', 'U', '', 'None', 'NONE', 'nan', 'NaN', 'null', 'NULL', '<NA>', 'ALL']

SIDEBAR_BG     = "#160A2E"
SIDEBAR_ACCENT = "#7C3AED"
SIDEBAR_TEXT   = "#C4B5FD"
SIDEBAR_MUTED  = "#6D5A8E"

st.set_page_config(
    page_title=f"{PITCH_COMPANY_NAME} | Conversion Engine",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="expanded"
)

def apply_custom_theme(primary_color):
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Playfair+Display:ital,wght@0,600;0,700;0,800;1,600&display=swap');
        html, body, [class*="css"] {{ font-family: 'Outfit', sans-serif; }}
        .stApp {{ background-color: #FAFAFC; }}
        [data-testid="stHeader"] {{ display: none !important; }}
        #MainMenu {{ visibility: hidden; }}
        footer {{ visibility: hidden; }}

        /* ── SIDEBAR ── */
        [data-testid="stSidebar"] {{
            background-color: {SIDEBAR_BG} !important;
            min-width: 252px !important;
            max-width: 252px !important;
            padding-top: 0 !important;
        }}
        [data-testid="stSidebar"] > div:first-child {{
            padding-top: 0 !important;
        }}
        [data-testid="collapsedControl"] {{
            color: {SIDEBAR_TEXT} !important;
        }}

        /* Sidebar date pickers — target baseweb container */
        [data-testid="stSidebar"] .stDateInput [data-baseweb="input"] {{
            background: rgba(255,255,255,0.05) !important;
            border: 1.5px solid rgba(196,181,253,0.45) !important;
            border-radius: 999px !important;
        }}
        [data-testid="stSidebar"] .stDateInput [data-baseweb="input"] input {{
            background: transparent !important;
            color: {SIDEBAR_TEXT} !important;
            font-size: 0.88rem !important;
            padding: 5px 14px !important;
        }}
        [data-testid="stSidebar"] .stDateInput label {{
            color: {SIDEBAR_MUTED} !important;
            font-size: 0.63rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
        }}

        /* Sidebar number input — match secondary button scheme, compact */
        [data-testid="stSidebar"] .stNumberInput {{
            margin-bottom: 0 !important;
        }}
        [data-testid="stSidebar"] .stNumberInput > div {{
            min-height: 0 !important;
        }}
        /* Number input — single pill, one border on the outer row container */
        [data-testid="stSidebar"] .stNumberInput > div:last-child {{
            background: rgba(255,255,255,0.05) !important;
            border: 1.5px solid rgba(196,181,253,0.45) !important;
            border-radius: 999px !important;
            overflow: hidden !important;
            display: flex !important;
            align-items: center !important;
            height: 34px !important;
        }}
        /* Suppress focus ghost box — hide any 3rd+ child divs Streamlit injects */
        [data-testid="stSidebar"] .stNumberInput input:focus {{
            outline: none !important;
            box-shadow: none !important;
        }}
        [data-testid="stSidebar"] .stNumberInput > div:nth-child(n+3) {{
            display: none !important;
        }}
        /* Strip inner borders from all children */
        [data-testid="stSidebar"] .stNumberInput [data-baseweb="input"],
        [data-testid="stSidebar"] .stNumberInput [data-baseweb="base-input"],
        [data-testid="stSidebar"] .stNumberInput > div > div {{
            background: transparent !important;
            border: none !important;
            outline: none !important;
            box-shadow: none !important;
        }}
        [data-testid="stSidebar"] .stNumberInput input {{
            background: transparent !important;
            border: none !important;
            outline: none !important;
            box-shadow: none !important;
            color: {SIDEBAR_TEXT} !important;
            font-size: 0.88rem !important;
            text-align: center !important;
            padding: 0 6px !important;
            height: 100% !important;
        }}
        /* +/- buttons: no individual border, just a subtle left separator */
        [data-testid="stSidebar"] .stNumberInput button {{
            background: rgba(255,255,255,0.08) !important;
            border: none !important;
            border-left: 1px solid rgba(196,181,253,0.25) !important;
            border-radius: 0 !important;
            outline: none !important;
            box-shadow: none !important;
            color: {SIDEBAR_TEXT} !important;
            height: 100% !important;
            padding: 0 10px !important;
            font-size: 0.95rem !important;
        }}

        /* Sidebar file uploader — match secondary button scheme */
        [data-testid="stSidebar"] [data-testid="stFileUploader"] {{
            background: rgba(255,255,255,0.05) !important;
            border: 1.5px solid rgba(196,181,253,0.45) !important;
            border-radius: 12px !important;
        }}
        [data-testid="stSidebar"] [data-testid="stFileUploader"] * {{
            color: {SIDEBAR_TEXT} !important;
            font-size: 0.72rem !important;
        }}
        /* Drop zone */
        [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {{
            background: rgba(255,255,255,0.05) !important;
            border: 1.5px solid rgba(196,181,253,0.45) !important;
            border-radius: 12px !important;
        }}
        /* Uploaded file card */
        [data-testid="stSidebar"] [data-testid="stFileUploader"] section {{
            background: rgba(255,255,255,0.05) !important;
            border: 1.5px solid rgba(196,181,253,0.45) !important;
            border-radius: 12px !important;
        }}
        [data-testid="stSidebar"] [data-testid="stFileUploader"] section > div,
        [data-testid="stSidebar"] [data-testid="stFileUploader"] section li {{
            background: transparent !important;
            border: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stFileUploader"] button {{
            background: rgba(255,255,255,0.05) !important;
            border: 1px solid rgba(196,181,253,0.22) !important;
            border-radius: 6px !important;
            color: {SIDEBAR_TEXT} !important;
            font-size: 0.68rem !important;
            padding: 3px 8px !important;
        }}

        /* Sidebar toggle */
        [data-testid="stSidebar"] .stToggle label {{
            color: {SIDEBAR_TEXT} !important;
            font-size: 0.73rem !important;
        }}

        /* Sidebar multiselect */
        [data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] {{
            background: rgba(255,255,255,0.07) !important;
            border: 1px solid rgba(196,181,253,0.2) !important;
            border-radius: 8px !important;
        }}
        [data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] {{
            background-color: {SIDEBAR_ACCENT} !important;
        }}
        [data-testid="stSidebar"] .stMultiSelect input {{
            color: #E2D9F3 !important;
            font-size: 0.73rem !important;
        }}

        /* Sidebar pill buttons — compact, natural width */
        [data-testid="stSidebar"] .stButton > button {{
            border-radius: 999px !important;
            font-size: 0.88rem !important;
            font-weight: 600 !important;
            padding: 3px 14px !important;
            white-space: nowrap !important;
            margin-bottom: 1px !important;
            line-height: 1.3 !important;
            transition: all 0.15s ease !important;
            border-width: 1px !important;
            min-height: 0 !important;
            height: auto !important;
        }}
        /* Target the inner <p> Streamlit wraps button text in */
        [data-testid="stSidebar"] .stButton > button p {{
            font-size: 0.88rem !important;
            font-weight: 600 !important;
            margin: 0 !important;
            line-height: 1.3 !important;
        }}
        [data-testid="stSidebar"] .stButton > button[kind="primary"] {{
            background: {SIDEBAR_ACCENT} !important;
            border-color: {SIDEBAR_ACCENT} !important;
            color: #FFFFFF !important;
        }}
        [data-testid="stSidebar"] .stButton > button[kind="secondary"] {{
            background: rgba(255,255,255,0.05) !important;
            border-color: rgba(196,181,253,0.22) !important;
            color: {SIDEBAR_TEXT} !important;
        }}
        [data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover {{
            background: rgba(124,58,237,0.2) !important;
            border-color: {SIDEBAR_ACCENT} !important;
            color: #FFFFFF !important;
        }}

        /* Sidebar divider */
        [data-testid="stSidebar"] hr {{
            border-color: rgba(196,181,253,0.15) !important;
            margin: 0.5rem 0 !important;
        }}

        /* Sidebar expanders - dark themed */
        [data-testid="stSidebar"] [data-testid="stExpander"] {{
            background: transparent !important;
            border: none !important;
            border-bottom: 1px solid rgba(196,181,253,0.15) !important;
            border-radius: 0 !important;
            margin: 0 !important;
        }}
        [data-testid="stSidebar"] [data-testid="stExpander"] details {{
            background: transparent !important;
        }}
        [data-testid="stSidebar"] [data-testid="stExpander"] details summary {{
            background: transparent !important;
            padding: 7px 0 !important;
            color: {SIDEBAR_MUTED} !important;
            font-family: 'Outfit', sans-serif !important;
            font-size: 0.61rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
            list-style: none !important;
        }}
        [data-testid="stSidebar"] [data-testid="stExpander"] details summary svg {{
            stroke: {SIDEBAR_MUTED} !important;
        }}
        [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {{
            background: transparent !important;
            border: none !important;
            padding: 6px 0 8px 0 !important;
        }}

        /* Sidebar section labels */
        .sidebar-section-label {{
            font-family: 'Outfit', sans-serif;
            font-size: 0.61rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: {SIDEBAR_MUTED};
            margin: 0 0 4px 0;
            padding: 0;
        }}

        /* Sidebar logo */
        .sidebar-logo {{
            font-family: 'Playfair Display', serif;
            font-size: 1.25rem;
            font-weight: 700;
            color: #FFFFFF;
            padding: 1.1rem 0 0.7rem 0;
            white-space: nowrap;
        }}

        /* Sidebar spacer to push logout to bottom */
        .sidebar-spacer {{
            flex: 1;
            min-height: 40px;
        }}

        /* ── KPI CARDS ── */
        .kpi-card {{
            background: #FFFFFF;
            border-radius: 12px;
            padding: 14px 16px;
            border: 1px solid #EBE4F4;
            box-shadow: 0 2px 8px rgba(77,20,140,0.05);
            height: 100%;
        }}
        .kpi-label {{
            font-family: 'Outfit', sans-serif;
            font-size: 0.66rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.09em;
            color: #94A3B8;
            margin-bottom: 4px;
        }}
        .kpi-value {{
            font-family: 'Outfit', sans-serif;
            font-size: 1.5rem;
            font-weight: 800;
            color: #0F172A;
            line-height: 1.1;
        }}
        .kpi-sub {{
            font-size: 0.7rem;
            color: #64748B;
            margin-top: 2px;
        }}

        /* ── ACTIVE FILTER PILLS ── */
        .filter-pills-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            padding-top: 1rem;
            padding-bottom: 0.2rem;
            margin-bottom: 0.2rem;
        }}
        .filter-pill {{
            background: #F0EBFA;
            border: 1px solid #D8C8F5;
            border-radius: 999px;
            padding: 3px 11px;
            font-size: 0.72rem;
            font-weight: 600;
            color: {primary_color};
            white-space: nowrap;
        }}

        /* ── SECTION TITLES ── */
        .section-title {{
            font-family: 'Outfit', sans-serif !important;
            font-weight: 700 !important;
            font-size: 1.35rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
            color: #0F172A !important;
            margin: 0 0 12px 0 !important;
            padding: 0 !important;
            line-height: 1.2 !important;
        }}

        /* ── PREMIUM TABLE ── */
        .premium-table-container {{ width: 100% !important; border-radius: 12px; border: 1px solid #EBE4F4; background: #FFFFFF; overflow: hidden; margin-top: 0.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.03); }}
        .premium-table-container table {{ width: 100% !important; border-collapse: collapse !important; border: none !important; }}
        .premium-table-container th {{
            font-family: 'Outfit', sans-serif !important;
            background-color: #F8F6FA !important;
            color: {primary_color} !important;
            font-weight: 700 !important;
            text-align: center !important;
            padding: 13px 12px !important;
            border-bottom: 2px solid {primary_color} !important;
            font-size: 0.72rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
        }}
        .premium-table-container td {{
            font-family: 'Outfit', sans-serif !important;
            text-align: center !important;
            padding: 11px 12px !important;
            border-bottom: 1px solid #F1F5F9 !important;
            font-size: 0.82rem !important;
            font-weight: 500;
            color: #0F172A !important;
        }}
        .premium-table-container td:first-child {{ color: {primary_color} !important; font-size: 0.9rem !important; font-weight: 700 !important; }}
        .premium-table-container td:nth-child(2) {{ color: {primary_color} !important; font-weight: 700 !important; }}
        .premium-table-container tr:last-child td {{ border-bottom: none !important; }}

        /* ── MISC ── */
        .serif-gradient-centerpiece {{ font-family: 'Playfair Display', serif !important; background: linear-gradient(90deg, #4D148C 0%, #20B2AA 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: inline-block; font-weight: 700 !important; letter-spacing: -0.5px; }}
        .modern-serif-title {{ font-family: 'Playfair Display', serif !important; color: #0F172A !important; font-weight: 700 !important; }}
        .header-logo {{ font-family: 'Playfair Display', serif !important; font-size: 1.8rem; font-weight: 700; color: #0F172A; line-height: 1.1; white-space: nowrap; }}

        /* Login form */
        .stTextInput label {{
            font-family: 'Outfit', sans-serif !important;
            font-weight: 700 !important;
            font-size: 0.85rem !important;
            color: {primary_color} !important;
            letter-spacing: 0.02em !important;
        }}
        .stTextInput input {{
            border: 1.5px solid #EBE4F4 !important;
            border-radius: 10px !important;
            padding: 10px 16px !important;
            font-family: 'Outfit', sans-serif !important;
            font-size: 0.95rem !important;
            background: #FFFFFF !important;
            box-shadow: 0 2px 8px rgba(77,20,140,0.04) !important;
        }}
        .stTextInput input:focus {{
            border-color: {primary_color} !important;
            box-shadow: 0 0 0 3px rgba(77,20,140,0.1) !important;
            outline: none !important;
        }}

        /* Main area buttons only — all-caps, spaced */
        [data-testid="stMain"] .stButton > button {{
            white-space: nowrap !important;
            font-size: 0.7rem !important;
            padding: 6px 14px !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            font-family: 'Outfit', sans-serif !important;
        }}
        /* Sidebar buttons — explicitly no uppercase so nothing bleeds through */
        [data-testid="stSidebar"] .stButton > button {{
            text-transform: none !important;
            letter-spacing: normal !important;
        }}

        /* ── Filter option pills inside bordered containers — soft pill style ── */
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pills"] {{
            border-radius: 999px !important;
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            padding: 4px 13px !important;
            border: 1px solid #D8C8F5 !important;
            background: #F0EBFA !important;
            color: {primary_color} !important;
            text-transform: none !important;
            letter-spacing: 0 !important;
        }}
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pills"] p,
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pills"] span {{
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            text-transform: none !important;
            color: {primary_color} !important;
        }}
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pillsActive"] {{
            border-radius: 999px !important;
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            padding: 4px 13px !important;
            border: 1px solid {primary_color} !important;
            background: {primary_color} !important;
            color: #FFFFFF !important;
            text-transform: none !important;
            letter-spacing: 0 !important;
        }}
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pillsActive"] p,
        [data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBaseButton-pillsActive"] span {{
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            color: #FFFFFF !important;
            text-transform: none !important;
        }}

        /* ── ST.PILLS — unselected state ── */
        [data-testid="stBaseButton-pills"] {{
            border-radius: 8px !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            font-family: 'Outfit', sans-serif !important;
            padding: 5px 13px !important;
            border: 1.5px solid #D8C8F5 !important;
            background: #FFFFFF !important;
            color: {primary_color} !important;
        }}
        [data-testid="stBaseButton-pills"] p,
        [data-testid="stBaseButton-pills"] span {{
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            color: {primary_color} !important;
        }}
        /* ── ST.PILLS — selected state (Streamlit uses a different testid) ── */
        [data-testid="stBaseButton-pillsActive"] {{
            border-radius: 8px !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            font-family: 'Outfit', sans-serif !important;
            padding: 5px 13px !important;
            border: 1.5px solid {primary_color} !important;
            background: {primary_color} !important;
            color: #FFFFFF !important;
        }}
        [data-testid="stBaseButton-pillsActive"] p,
        [data-testid="stBaseButton-pillsActive"] span {{
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            color: #FFFFFF !important;
        }}

        /* ── VARIABLE SELECTOR RADIO-AS-PILLS ── */
        [data-testid="stMain"] .stRadio > label {{
            display: none !important;
        }}
        [data-testid="stMain"] .stRadio > div[role="radiogroup"],
        [data-testid="stMain"] .stRadio > div {{
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: wrap !important;
            gap: 6px !important;
            align-items: center !important;
        }}
        [data-testid="stMain"] .stRadio [data-baseweb="radio"] {{
            background: #FFFFFF !important;
            border: 1.5px solid #D8C8F5 !important;
            border-radius: 8px !important;
            padding: 5px 13px !important;
            margin: 0 !important;
            cursor: pointer !important;
            transition: background 0.15s ease !important;
        }}
        /* Hide the radio circle dot */
        [data-testid="stMain"] .stRadio [data-baseweb="radio"] > div:first-child {{
            display: none !important;
        }}
        /* Label text */
        [data-testid="stMain"] .stRadio [data-baseweb="radio"] > div:last-child,
        [data-testid="stMain"] .stRadio [data-baseweb="radio"] > div:last-child p {{
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            color: {primary_color} !important;
            padding: 0 !important;
            margin: 0 !important;
            line-height: 1.3 !important;
        }}
        /* Selected pill */
        [data-testid="stMain"] .stRadio [data-baseweb="radio"]:has(input:checked) {{
            background: {primary_color} !important;
            border-color: {primary_color} !important;
        }}
        [data-testid="stMain"] .stRadio [data-baseweb="radio"]:has(input:checked) > div:last-child,
        [data-testid="stMain"] .stRadio [data-baseweb="radio"]:has(input:checked) > div:last-child p {{
            color: #FFFFFF !important;
        }}

        /* Multiselect tags (main area) */
        .stMultiSelect [data-baseweb="tag"] {{
            background-color: {primary_color} !important;
            border-radius: 6px !important;
        }}
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
    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    return bigquery.Client(
        credentials=service_account.Credentials.from_service_account_info(creds_dict),
        project=creds_dict["project_id"]
    )

# ================ 3. DATA LOADING =================
@st.cache_data(ttl=3600)
def load_visitor_base(pixel_id, tenant_type):
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
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        df['total_visitors'] = pd.to_numeric(df['total_visitors'], errors='coerce').fillna(0)
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

# ================ 3B. BUCKETING & CLEANING =================
def get_real_number(v):
    if pd.isna(v): return None
    v_str = str(v).lower().replace(',', '')
    match = re.search(r'(\d+\.?\d*)', v_str)
    if not match: return None
    val_num = float(match.group(1))
    if re.search(r'(m\b|million)', v_str): val_num *= 1000000
    elif re.search(r'(k\b|thousand)', v_str): val_num *= 1000
    return val_num

def bucket_income(val):
    num = get_real_number(val)
    if num is None: return 'Unknown'
    if num < 50000: return 'Under $50k'
    elif num < 100000: return '$50k-$100k'
    elif num < 200000: return '$100k-$200k'
    else: return '$200k+'

def bucket_net_worth(val):
    num = get_real_number(val)
    if num is None: return 'Unknown'
    if num < 100000: return 'Under $100k'
    elif num < 500000: return '$100k-$500k'
    elif num < 1000000: return '$500k-$1M'
    else: return '$1M+'

def clean_gender(val):
    if pd.isna(val): return 'Unknown'
    val_str = str(val).strip().upper()
    if val_str == 'M': return 'Male'
    elif val_str == 'F': return 'Female'
    return str(val)

def clean_boolean(val):
    if pd.isna(val): return 'Unknown'
    val_str = str(val).strip().upper()
    if val_str in ['Y', 'YES']: return 'Yes'
    elif val_str in ['N', 'NO']: return 'No'
    return str(val)

def clean_marital(val):
    if pd.isna(val): return 'Unknown'
    val_str = str(val).strip().upper()
    if val_str in ['Y', 'YES']: return 'Married'
    elif val_str in ['N', 'NO']: return 'Single'
    return str(val)

def clean_homeowner(val):
    if pd.isna(val): return 'Unknown'
    val_str = str(val).strip().upper()
    if val_str in ['Y', 'YES', 'HOMEOWNER', 'OWNER']: return 'Yes'
    elif val_str in ['N', 'NO', 'RENTER']: return 'No'
    return str(val)

def clean_state(val):
    if pd.isna(val): return 'Unknown'
    return str(val).strip().upper()

@st.cache_data(ttl=3600)
def load_order_base(pixel_id, tenant_type):
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
        parsed_dates = pd.to_datetime(df['order_date'], errors='coerce')
        if parsed_dates.dt.tz is not None:
            parsed_dates = parsed_dates.dt.tz_convert(None)
        df['order_date'] = parsed_dates
        df = df.rename(columns={'order_id': 'Order_ID', 'revenue': 'Total'})

        # Clean demographic fields that may have raw Y/N values from BQ
        if 'homeowner'      in df.columns: df['homeowner']      = df['homeowner'].apply(clean_homeowner)
        if 'children'       in df.columns: df['children']       = df['children'].apply(clean_boolean)
        if 'marital_status' in df.columns: df['marital_status'] = df['marital_status'].apply(clean_marital)
        if 'gender'         in df.columns: df['gender']         = df['gender'].apply(clean_gender)

        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

# ================ 4. SESSION STATE =================
if 'app_state' not in st.session_state:
    st.session_state.app_state = 'login'
if 'pixel_id' not in st.session_state:
    st.session_state.pixel_id = None
if 'tenant_type' not in st.session_state:
    st.session_state.tenant_type = None
if 'username' not in st.session_state:
    st.session_state.username = None
if 'client_name' not in st.session_state:
    st.session_state.client_name = None
if 'has_unsaved_enrichment' not in st.session_state:
    st.session_state.has_unsaved_enrichment = False
if 'pending_save_orders' not in st.session_state:
    st.session_state.pending_save_orders = pd.DataFrame()

# ================ 5. LOGIN PAGE =================
def login_page():
    # Full-screen login — no scroll, no padding
    st.markdown(
        '<style>'
        'html,body{overflow:hidden!important;height:100vh!important;}'
        '.stApp{height:100vh!important;overflow:hidden!important;}'
        '[data-testid="stSidebar"]{display:none!important;}'
        '[data-testid="collapsedControl"]{display:none!important;}'
        '[data-testid="stMain"]{padding:0!important;height:100vh!important;overflow:hidden!important;}'
        '[data-testid="stMainBlockContainer"]{padding:0!important;max-width:100%!important;height:100vh!important;overflow:hidden!important;}'
        '[data-testid="stVerticalBlock"]{gap:0!important;}'
        '[data-testid="stHorizontalBlock"]{gap:0!important;height:100vh!important;align-items:stretch!important;}'
        '[data-testid="stHorizontalBlock"]>[data-testid="stVerticalBlockBorderWrapper"]{height:100vh!important;}'
        '</style>',
        unsafe_allow_html=True
    )

    left, right = st.columns([4, 7])

    with left:
        st.markdown(f"""
        <div style="background:#160A2E;padding:56px 44px;
                    min-height:100vh;display:flex;flex-direction:column;justify-content:space-between;
                    box-sizing:border-box;">
          <div>
            <div style="font-family:'Playfair Display',serif;font-size:2.4rem;font-weight:700;color:#fff;line-height:1.1;">
              Lead<span style="color:#7C3AED;">Navigator</span>
            </div>
            <div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:0.13em;
                        color:#6D5A8E;margin-top:12px;">Conversion Intelligence</div>
            <div style="width:48px;height:1.5px;background:rgba(196,181,253,0.2);margin:32px 0;"></div>
            <div style="margin-bottom:32px;">
              <div style="font-size:3rem;font-weight:800;color:#C4B5FD;line-height:1;">308M+</div>
              <div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                          color:#6D5A8E;margin-top:6px;">Consumer profiles</div>
            </div>
            <div style="margin-bottom:32px;">
              <div style="font-size:3rem;font-weight:800;color:#C4B5FD;line-height:1;">41%</div>
              <div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                          color:#6D5A8E;margin-top:6px;">Avg enrichment rate</div>
            </div>
            <div style="margin-bottom:32px;">
              <div style="font-size:3rem;font-weight:800;color:#C4B5FD;line-height:1;">8.3x</div>
              <div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
                          color:#6D5A8E;margin-top:6px;">Avg client ROI</div>
            </div>
            <div style="width:48px;height:1.5px;background:rgba(196,181,253,0.2);margin:32px 0;"></div>
            <div style="display:flex;flex-direction:column;gap:10px;">
              <div style="display:inline-flex;align-items:center;gap:9px;background:rgba(124,58,237,0.15);
                          border:1px solid rgba(196,181,253,0.2);border-radius:999px;padding:8px 18px;
                          font-size:0.88rem;font-weight:600;color:#C4B5FD;width:fit-content;">
                <div style="width:8px;height:8px;border-radius:50%;background:#7C3AED;flex-shrink:0;"></div>
                SuperPixel&#8482; On-Site Intelligence
              </div>
              <div style="display:inline-flex;align-items:center;gap:9px;background:rgba(124,58,237,0.15);
                          border:1px solid rgba(196,181,253,0.2);border-radius:999px;padding:8px 18px;
                          font-size:0.88rem;font-weight:600;color:#C4B5FD;width:fit-content;">
                <div style="width:8px;height:8px;border-radius:50%;background:#7C3AED;flex-shrink:0;"></div>
                In-Market Audiences
              </div>
              <div style="display:inline-flex;align-items:center;gap:9px;background:rgba(124,58,237,0.15);
                          border:1px solid rgba(196,181,253,0.2);border-radius:999px;padding:8px 18px;
                          font-size:0.88rem;font-weight:600;color:#C4B5FD;width:fit-content;">
                <div style="width:8px;height:8px;border-radius:50%;background:#7C3AED;flex-shrink:0;"></div>
                Multi-Channel Activation
              </div>
            </div>
          </div>
          <div style="font-size:0.78rem;color:#3D2A5E;font-weight:500;">Powered by LeadNavigator.AI</div>
        </div>
        """, unsafe_allow_html=True)

    with right:
        st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)
        st.markdown(
            f'<div style="text-align:center;margin-bottom:28px;line-height:1.15;">'
            f'<span class="serif-gradient-centerpiece" style="font-size:2.4rem;">Welcome to<br>{PITCH_COMPANY_NAME}</span></div>',
            unsafe_allow_html=True
        )
        _, fc, _ = st.columns([1, 1.4, 1])
        with fc:
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Sign In", use_container_width=True, type="primary"):
                users = dict(st.secrets.get("users", {}))
                if username in users and users[username].get("password") == password:
                    st.session_state.username = username
                    st.session_state.pixel_id = users[username].get("pixel_id")
                    st.session_state.tenant_type = users[username].get("tenant_type")
                    st.session_state.client_name = users[username].get("client_name", username.replace("_", " ").title())
                    with st.spinner("Syncing your data..."):
                        df_demo, df_state, error = load_visitor_base(
                            st.session_state.pixel_id, st.session_state.tenant_type
                        )
                        df_orders, order_error = load_order_base(
                            st.session_state.pixel_id, st.session_state.tenant_type
                        )
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
                else:
                    st.error("Invalid username or password")
            st.markdown(
                '<p style="text-align:center;font-size:0.72rem;color:#94A3B8;margin-top:14px;">'
                'Secure &middot; Private &middot; Encrypted</p>',
                unsafe_allow_html=True
            )

# ================ 6. ONBOARDING PAGE =================
def onboarding_page():
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

# ================ 7. ENRICHMENT HELPER =================
def run_enrichment(uploaded_file, pixel_id, tenant_type):
    """Run the full enrichment pipeline. Returns (success, message)."""
    try:
        raw_df = pd.read_csv(io.BytesIO(uploaded_file.getvalue()), encoding='latin1', on_bad_lines='skip')
        raw_df.columns = [str(c).strip().lower() for c in raw_df.columns]

        email_col = next((c for c in raw_df.columns if 'email' in c), None)
        if not email_col:
            return False, "No email column found in your CSV."

        revenue_col  = next((c for c in raw_df.columns if any(x in c for x in ['total', 'revenue', 'amount'])), None)
        lineitem_col = next((c for c in raw_df.columns if any(x in c for x in ['lineitem_name', 'line_item', 'product_title', 'product', 'sku', 'item_name', 'variant'])), None)
        unique_emails = raw_df[email_col].dropna().astype(str).str.lower().str.strip()
        unique_emails = unique_emails[unique_emails.str.contains('@', na=False)].unique().tolist()

        response = requests.post(N8N_WEBHOOK_URL, json={"emails": unique_emails}, timeout=180)

        if response.status_code != 200:
            return False, f"Webhook failed with status {response.status_code}: {response.text}"

        try:
            enriched_df = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', engine='python')
        except Exception:
            return False, "Could not parse enriched response as CSV."

        enriched_df.columns = [str(c).strip().lower() for c in enriched_df.columns]

        STANDARD_EMAIL_COLS = ['personal_emails', 'business_email', 'email_match', 'deep_verified_emails']
        enriched_email_col = next((c for c in STANDARD_EMAIL_COLS if c in enriched_df.columns), None)
        if enriched_email_col is None:
            return False, f"Could not find email column in enriched response. Got: {list(enriched_df.columns[:15])}"

        enriched_df = enriched_df.rename(columns={enriched_email_col: 'email_match'})
        enriched_df['email_match'] = enriched_df['email_match'].astype(str).str.split(',')
        enriched_df = enriched_df.explode('email_match')
        enriched_df['email_match'] = enriched_df['email_match'].str.strip().str.lower()
        enriched_df = enriched_df[enriched_df['email_match'].str.contains('@', na=False)].drop_duplicates('email_match')

        N8N_COLUMN_MAPPER = {
            "gender": "gender", "married": "marital_status", "age_range": "age_range",
            "income_range": "income_bucket", "personal_state": "state",
            "homeowner": "homeowner", "children": "children", "net_worth": "net_worth_bucket",
        }
        for src_col, dst_col in N8N_COLUMN_MAPPER.items():
            if src_col in enriched_df.columns:
                enriched_df[dst_col] = enriched_df[src_col]

        if 'income_bucket'    in enriched_df.columns: enriched_df['income_bucket']    = enriched_df['income_bucket'].apply(bucket_income)
        if 'net_worth_bucket' in enriched_df.columns: enriched_df['net_worth_bucket'] = enriched_df['net_worth_bucket'].apply(bucket_net_worth)
        if 'gender'           in enriched_df.columns: enriched_df['gender']           = enriched_df['gender'].apply(clean_gender)
        if 'children'         in enriched_df.columns: enriched_df['children']         = enriched_df['children'].apply(clean_boolean)
        if 'marital_status'   in enriched_df.columns: enriched_df['marital_status']   = enriched_df['marital_status'].apply(clean_marital)
        if 'homeowner'        in enriched_df.columns: enriched_df['homeowner']        = enriched_df['homeowner'].apply(clean_homeowner)
        if 'state'            in enriched_df.columns: enriched_df['state']            = enriched_df['state'].apply(clean_state)

        orders_join = raw_df.copy()
        orders_join['email_match'] = orders_join[email_col].astype(str).str.lower().str.strip()
        date_col = next((c for c in orders_join.columns if any(x in c for x in ['created', 'date', 'ordered'])), None)

        join_cols = ['email_match']
        if revenue_col:  join_cols.append(revenue_col)
        if date_col:     join_cols.append(date_col)
        if lineitem_col: join_cols.append(lineitem_col)

        joined_df = pd.merge(orders_join[join_cols], enriched_df, on='email_match', how='left')
        if revenue_col:
            joined_df = joined_df.rename(columns={revenue_col: 'Total'})
        else:
            joined_df['Total'] = 0.0
        joined_df['Total'] = pd.to_numeric(joined_df['Total'], errors='coerce').fillna(0.0)

        # Detect order ID column from the original CSV
        order_id_col = next((c for c in raw_df.columns if any(x == c for x in
                             ['name', 'order_id', 'order_number', 'id', 'number'])), None)

        temp_orders = pd.DataFrame()
        # Generate deterministic IDs so re-uploading the same orders won't create duplicates
        if order_id_col and order_id_col in orders_join.columns:
            temp_orders['Order_ID'] = ('EN_' + orders_join[order_id_col].astype(str).str.strip()
                                       .apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16]))
        else:
            temp_orders['Order_ID'] = [
                'EN_' + hashlib.md5(f"{e}_{d}_{a}".encode()).hexdigest()[:16]
                for e, d, a in zip(
                    joined_df['email_match'],
                    joined_df.get(date_col, pd.Series([''] * len(joined_df))),
                    joined_df['Total']
                )
            ]
        temp_orders['Total'] = joined_df['Total']

        if date_col and date_col in joined_df.columns:
            temp_orders['order_date'] = pd.to_datetime(joined_df[date_col], errors='coerce').fillna(datetime.now())
        else:
            temp_orders['order_date'] = datetime.now()

        for col in ['gender', 'age_range', 'income_bucket', 'net_worth_bucket', 'homeowner', 'marital_status', 'children', 'state']:
            temp_orders[col] = joined_df[col] if col in joined_df.columns else 'Unknown'

        temp_orders['customer_email'] = joined_df['email_match']
        temp_orders['lineitem_name']   = (
            joined_df[lineitem_col].astype(str).str.strip()
            if lineitem_col and lineitem_col in joined_df.columns
            else 'Enriched Import'
        )
        temp_orders['pixel_id']        = pixel_id

        if tenant_type == 'B2B':
            for b2b_col in ['company_name', 'company_industry', 'employee_count_range', 'job_title', 'seniority', 'company_revenue']:
                temp_orders[b2b_col] = joined_df[b2b_col] if b2b_col in joined_df.columns else 'Unknown'

        if not st.session_state.df_orders.empty:
            st.session_state.df_orders = pd.concat([st.session_state.df_orders, temp_orders], ignore_index=True)
        else:
            st.session_state.df_orders = temp_orders

        num_enriched = len(temp_orders)
        num_matched  = int(temp_orders[['gender', 'age_range', 'income_bucket']].ne('Unknown').any(axis=1).sum())

        # Store enriched orders so user can save them to BigQuery
        st.session_state.pending_save_orders     = temp_orders.copy()
        st.session_state.has_unsaved_enrichment  = True

        return True, f"✅ {num_enriched:,} orders enriched, {num_matched:,} matched with identity data."

    except Exception as e:
        return False, f"Error during enrichment: {e}"

# ================ 7B. SAVE ENRICHED ORDERS TO BIGQUERY =================
def save_enriched_orders_to_bq(pixel_id):
    """Save pending enriched orders to BigQuery, deduplicating by order_id."""
    try:
        pending = st.session_state.get('pending_save_orders', pd.DataFrame())
        if pending.empty:
            return False, "No enriched orders to save."

        client = get_bq_client()

        # Fetch existing order IDs for this pixel so we don't duplicate
        existing_query = f"""
        SELECT order_id FROM `{BQ_ORDERS_TABLE}`
        WHERE pixel_id = @pixel_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("pixel_id", "STRING", pixel_id)]
        )
        existing_df = client.query(existing_query, job_config=job_config).to_dataframe()
        existing_ids = set(existing_df['order_id'].astype(str).tolist()) if not existing_df.empty else set()

        # Filter to only new orders
        df_new = pending[~pending['Order_ID'].astype(str).isin(existing_ids)].copy()

        if df_new.empty:
            return True, "All orders already exist in the database — nothing new to save."

        # Rename columns to match BQ schema
        df_bq = df_new.rename(columns={'Order_ID': 'order_id', 'Total': 'revenue'})

        # Ensure all BQ columns are present
        bq_cols = ['pixel_id', 'order_id', 'order_date', 'customer_email', 'revenue',
                   'lineitem_name', 'state', 'gender', 'age_range', 'income_bucket',
                   'net_worth_bucket', 'homeowner', 'marital_status', 'children',
                   'company_name', 'company_industry', 'employee_count_range',
                   'job_title', 'seniority', 'company_revenue']
        for col in bq_cols:
            if col not in df_bq.columns:
                df_bq[col] = None
        df_bq = df_bq[bq_cols]

        # Convert order_date to datetime if needed
        df_bq['order_date'] = pd.to_datetime(df_bq['order_date'], errors='coerce')
        df_bq['revenue']    = pd.to_numeric(df_bq['revenue'], errors='coerce').fillna(0.0)

        # Append to BigQuery
        job = client.load_table_from_dataframe(
            df_bq,
            BQ_ORDERS_TABLE,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
        )
        job.result()

        # Clear the pending state
        st.session_state.pending_save_orders    = pd.DataFrame()
        st.session_state.has_unsaved_enrichment = False

        # Clear order cache so the dashboard reloads with the new data
        load_order_base.clear()

        return True, f"✅ {len(df_new):,} orders saved to the database."

    except Exception as e:
        return False, f"Error saving to database: {e}"

# ================ 8. DASHBOARD PAGE =================
def dashboard_page():
    tenant_type = st.session_state.tenant_type
    pixel_id    = st.session_state.pixel_id

    # ── Session state defaults ──
    if 'metric_choice' not in st.session_state:
        st.session_state.metric_choice = 'Revenue Per Visitor'
    # Migrate old key name if saved in session state
    if st.session_state.metric_choice == 'Conversion Percent':
        st.session_state.metric_choice = 'Conversion Rate'
    if 'sort_asc' not in st.session_state:
        st.session_state.sort_asc = False
    if 'date_range' not in st.session_state:
        st.session_state.date_range = (datetime.now() - timedelta(days=30), datetime.now())

    # ── Configs ──
    if tenant_type == "B2C":
        configs = [
            ("Gender", "gender"), ("Age", "age_range"), ("Income", "income_bucket"),
            ("State", "state"), ("Net Worth", "net_worth_bucket"),
            ("Children", "children"), ("Marital Status", "marital_status"), ("Homeowner", "homeowner"),
        ]
    else:
        configs = [
            ("Industry", "industry"), ("Company Size", "employee_count_range"),
            ("Job Title", "job_title"), ("Seniority", "seniority"), ("Revenue", "company_revenue"),
        ]

    # Display name → dataframe column name
    metric_map = {
        "Revenue Per Visitor": "Rev/Visitor",
        "Conversion Rate":  "Conv %",
        "Revenue":             "Revenue",
        "Purchases":           "Purchases",
        "Visitors":            "Visitors",
    }
    metrics = list(metric_map.keys())

    # =====================================================
    # SIDEBAR
    # =====================================================
    with st.sidebar:

        # Logo
        st.markdown(
            f'<div class="sidebar-logo">Lead<span style="color:{SIDEBAR_ACCENT};">Navigator</span></div>',
            unsafe_allow_html=True
        )
        st.markdown("---")

        # ── UPLOAD ORDERS (collapsible) ──
        with st.expander("Upload Orders", expanded=False):
            uploaded_file = st.file_uploader(
                "Upload CSV", type=['csv'],
                label_visibility="collapsed",
                key="sidebar_upload"
            )
            if uploaded_file is not None:
                upload_btn    = st.button("Upload & Enrich", type="primary", use_container_width=True, key="upload_btn")
                upload_status = st.empty()
                if upload_btn:
                    upload_status.markdown(
                        '<div style="background: rgba(124,58,237,0.15); border: 1px solid rgba(196,181,253,0.3); '
                        'border-radius: 8px; padding: 10px 8px; color: #C4B5FD; font-size: 0.75rem; text-align: center;">'
                        '⏳ <b>Uploading & enriching...</b><br>'
                        '<span style="font-size: 0.68rem; opacity: 0.8;">This can take up to 3 minutes</span>'
                        '</div>',
                        unsafe_allow_html=True
                    )
                    success, message = run_enrichment(uploaded_file, pixel_id, tenant_type)
                    if success:
                        upload_status.success(message)
                        st.rerun()
                    else:
                        upload_status.error(message)

        # ── SAVE TO DATABASE (shows only after successful enrichment) ──
        if st.session_state.get('has_unsaved_enrichment', False):
            pending = st.session_state.get('pending_save_orders', pd.DataFrame())
            n_pending = len(pending)
            st.markdown(
                f'<p style="font-family:Outfit,sans-serif;font-size:0.65rem;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.08em;color:#A78BFA;margin-bottom:5px;">'
                f'{n_pending:,} orders ready to save</p>',
                unsafe_allow_html=True
            )
            save_btn = st.button("💾  Save to Database", key="save_to_bq_btn",
                                 type="primary", use_container_width=True)
            save_slot = st.empty()
            if save_btn:
                save_slot.markdown(
                    '<div style="background:rgba(124,58,237,0.15);border:1px solid rgba(196,181,253,0.3);'
                    'border-radius:8px;padding:8px;color:#C4B5FD;font-size:0.72rem;text-align:center;">'
                    '⏳ Saving to database...</div>',
                    unsafe_allow_html=True
                )
                ok, msg = save_enriched_orders_to_bq(pixel_id)
                if ok:
                    save_slot.success(msg)
                    st.rerun()
                else:
                    save_slot.error(msg)
            st.markdown("---")

        # ── DATE RANGE ──
        with st.expander("Date Range", expanded=False):
            start_date = st.date_input("Start Date", st.session_state.date_range[0], key="sb_start")
            end_date   = st.date_input("End Date",   st.session_state.date_range[1], key="sb_end")
            st.session_state.date_range = (start_date, end_date)

        # ── RANK BY ──
        with st.expander("Rank By", expanded=False):
            for m in metrics:
                is_active = (st.session_state.metric_choice == m)
                if st.button(m, key=f"metric_{m}",
                             type="primary" if is_active else "secondary"):
                    st.session_state.metric_choice = m
                    st.rerun()

        # ── SORT BY ──
        with st.expander("Sort By", expanded=False):
            if st.button("High → Low", key="sort_htl",
                         type="primary" if not st.session_state.sort_asc else "secondary"):
                st.session_state.sort_asc = False
                st.rerun()
            if st.button("Low → High", key="sort_lth",
                         type="primary" if st.session_state.sort_asc else "secondary"):
                st.session_state.sort_asc = True
                st.rerun()

        # ── MIN PURCHASES ──
        with st.expander("Min Purchases", expanded=False):
            min_purchasers = st.number_input(
                "Minimum Purchases", value=1, min_value=0, label_visibility="collapsed"
            )

        selected_skus = []
        sku_toggle = False

        # Spacer to push logout/refresh to bottom
        st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)
        st.markdown("---")

        # ── LOGOUT / REFRESH (bottom) ──
        sb_c1, sb_c2 = st.columns(2)
        with sb_c1:
            if st.button("Logout", key="logout_btn", use_container_width=True):
                st.session_state.app_state = 'login'
                st.session_state.pixel_id  = None
                st.session_state.tenant_type = None
                st.session_state.username  = None
                st.rerun()
        with sb_c2:
            if st.button("Refresh", key="header_refresh_btn", use_container_width=True):
                load_order_base.clear()
                load_visitor_base.clear()
                st.session_state.app_state = "onboarding"
                st.rerun()

    # ── Resolve control values ──
    metric_choice = st.session_state.metric_choice
    is_ascending  = st.session_state.sort_asc
    sort_label    = "Low → High" if is_ascending else "High → Low"

    # =====================================================
    # MAIN AREA — data filtering
    # =====================================================
    df_demo   = st.session_state.df_demo
    df_state  = st.session_state.df_state if tenant_type == "B2C" else pd.DataFrame()
    df_orders = st.session_state.df_orders

    if not df_demo.empty:
        df_demo_filtered = df_demo[
            (df_demo['visit_date'] >= pd.Timestamp(start_date)) &
            (df_demo['visit_date'] <= pd.Timestamp(end_date))
        ].copy()
    else:
        df_demo_filtered = df_demo.copy()

    if not df_state.empty:
        df_state_filtered = df_state[
            (df_state['visit_date'] >= pd.Timestamp(start_date)) &
            (df_state['visit_date'] <= pd.Timestamp(end_date))
        ].copy()
    else:
        df_state_filtered = df_state.copy()

    if not df_orders.empty:
        _order_dates = pd.to_datetime(df_orders['order_date'], errors='coerce')
        if _order_dates.dt.tz is not None:
            _order_dates = _order_dates.dt.tz_convert(None)
        orders_in_range = df_orders[
            (_order_dates >= pd.Timestamp(start_date)) &
            (_order_dates <= pd.Timestamp(end_date))
        ].copy()
    else:
        orders_in_range = df_orders.copy()

    # Ghost day integrity shield
    st.session_state.df_demo_cube = df_demo_filtered
    st.session_state.df_state_map = df_state_filtered
    active_days = set(df_demo_filtered['visit_date'].dt.date.unique()) if not df_demo_filtered.empty else set()
    if not orders_in_range.empty and active_days:
        orders_in_range['order_date_only'] = orders_in_range['order_date'].dt.date
        df_p_filtered = orders_in_range[orders_in_range['order_date_only'].isin(active_days)].copy()
    else:
        df_p_filtered = orders_in_range.copy()

    # Exclude $0 orders — a purchase only counts if revenue > 0
    if not df_p_filtered.empty and 'Total' in df_p_filtered.columns:
        df_p_filtered = df_p_filtered[pd.to_numeric(df_p_filtered['Total'], errors='coerce').fillna(0) > 0]


    # =====================================================
    # DASHBOARD TITLE
    # =====================================================
    client_name = st.session_state.get('client_name') or st.session_state.get('username', '')
    st.markdown(
        f'<div style="text-align:center; margin-bottom: 0.8rem;">'
        f'<span class="serif-gradient-centerpiece" style="font-size: 2.6rem;">'
        f'{client_name} Conversion Dashboard</span></div>',
        unsafe_allow_html=True
    )

    # =====================================================
    # KPI CARDS
    # =====================================================
    total_visitors  = int(df_demo_filtered['total_visitors'].sum()) if not df_demo_filtered.empty else 0
    total_revenue   = float(df_p_filtered['Total'].sum()) if not df_p_filtered.empty and 'Total' in df_p_filtered.columns else 0.0
    total_purchases = int(df_p_filtered['Order_ID'].nunique()) if not df_p_filtered.empty and 'Order_ID' in df_p_filtered.columns else 0
    conv_rate       = (total_purchases / total_visitors * 100) if total_visitors > 0 else 0.0
    rev_per_visitor = (total_revenue / total_visitors) if total_visitors > 0 else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Total Visitors</div>'
            f'<div class="kpi-value">{total_visitors:,.0f}</div>'
            f'<div class="kpi-sub">{start_date} – {end_date}</div></div>',
            unsafe_allow_html=True
        )
    with k2:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Total Revenue</div>'
            f'<div class="kpi-value">${total_revenue:,.0f}</div>'
            f'<div class="kpi-sub">{total_purchases:,} orders</div></div>',
            unsafe_allow_html=True
        )
    with k3:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Conversion Rate</div>'
            f'<div class="kpi-value">{conv_rate:.2f}%</div>'
            f'<div class="kpi-sub">of visitors purchased</div></div>',
            unsafe_allow_html=True
        )
    with k4:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-label">Revenue Per Visitor</div>'
            f'<div class="kpi-value">${rev_per_visitor:,.2f}</div>'
            f'<div class="kpi-sub">avg across date range</div></div>',
            unsafe_allow_html=True
        )

    # =====================================================
    # ACTIVE FILTER PILLS
    # =====================================================
    pills_html = '<div class="filter-pills-row">'
    pills_html += f'<span class="filter-pill">📅 {start_date} → {end_date}</span>'
    pills_html += f'<span class="filter-pill">🏅 {metric_choice}</span>'
    pills_html += f'<span class="filter-pill">↕ {sort_label}</span>'
    if min_purchasers > 0:
        pills_html += f'<span class="filter-pill">≥ {min_purchasers} purchases</span>'
    pills_html += '</div>'
    st.markdown(pills_html, unsafe_allow_html=True)

    st.divider()

    # =====================================================
    # SINGLE VARIABLE DEEP DIVE
    # =====================================================
    st.markdown('<p class="section-title">Single Variable Deep Dive</p>', unsafe_allow_html=True)

    if "active_single_var" not in st.session_state:
        st.session_state.active_single_var = configs[0][0]

    labels = [label for label, _ in configs]
    current_idx = labels.index(st.session_state.active_single_var) if st.session_state.active_single_var in labels else 0

    active_var = st.radio(
        "Select variable",
        options=labels,
        index=current_idx,
        horizontal=True,
        label_visibility="collapsed",
        key="var_selector_radio"
    )
    st.session_state.active_single_var = active_var
    selected_col = dict(configs)[active_var]

    # Normalize values so visitor data and order data use the same labels before merging
    _VALUE_NORM = {'Homeowner': 'Yes', 'Renter': 'No', 'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}
    _demo_cube = st.session_state.df_demo_cube.copy()
    if selected_col in _demo_cube.columns:
        _demo_cube[selected_col] = _demo_cube[selected_col].replace(_VALUE_NORM)
    _p_filt = df_p_filtered.copy()
    if not _p_filt.empty and selected_col in _p_filt.columns:
        _p_filt[selected_col] = _p_filt[selected_col].replace(_VALUE_NORM)

    if selected_col == 'state' and tenant_type == 'B2C':
        df_v_grp = st.session_state.df_state_map[
            ~st.session_state.df_state_map['state'].isin(EXCLUDE_LIST)
        ].groupby('state', as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})
    else:
        df_v_grp = _demo_cube[
            ~_demo_cube[selected_col].isin(EXCLUDE_LIST)
        ].groupby(selected_col, as_index=False)['total_visitors'].sum().rename(columns={'total_visitors': 'Visitors'})

    df_p_grp = pd.DataFrame()
    if not _p_filt.empty and selected_col in _p_filt.columns:
        df_p_grp = _p_filt[~_p_filt[selected_col].isin(EXCLUDE_LIST)].groupby(selected_col).agg(
            Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')
        ).reset_index()

    if not df_p_grp.empty:
        df_merged = pd.merge(df_v_grp, df_p_grp, on=selected_col, how='outer').fillna(0)
    else:
        df_merged = df_v_grp.copy()
        df_merged['Purchases'] = 0
        df_merged['Revenue']   = 0.0

    if not df_merged.empty:
        df_merged['Visitors']    = df_merged.get('Visitors', 0) + df_merged['Purchases']
        df_merged['Conv %']      = (df_merged['Purchases'] / df_merged['Visitors'].replace(0, 1) * 100).round(2)
        df_merged['Rev/Visitor'] = (df_merged['Revenue']   / df_merged['Visitors'].replace(0, 1)).round(2)

        sort_col  = metric_map[metric_choice]
        df_merged = df_merged[df_merged['Purchases'] >= min_purchasers].sort_values(sort_col, ascending=is_ascending)

        if not df_merged.empty:
            # Display-level cleanup: map raw Y/N codes to readable labels
            _display_map = {'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}
            df_merged[selected_col] = df_merged[selected_col].replace(_display_map)
            df_merged.insert(0, 'Rank', range(1, len(df_merged) + 1))
            display_df   = df_merged.rename(columns={selected_col: st.session_state.active_single_var})
            display_cols = ['Rank', st.session_state.active_single_var, 'Revenue', 'Visitors', 'Purchases', 'Conv %', 'Rev/Visitor']
            bold_col = metric_map[metric_choice]  # the active rank-by column
            styler = display_df[display_cols].style\
                .set_properties(**{'font-weight': '800'}, subset=[bold_col])\
                .format({'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}', 'Revenue': '${:,.2f}',
                         'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'})\
                .background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient)
            render_premium_table(styler)

    # =====================================================
    # TIME SERIES CHART
    # =====================================================
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown('<p class="section-title">Single Variable Performance Over Time</p>', unsafe_allow_html=True)

    if 'time_gran' not in st.session_state:
        st.session_state.time_gran = 'Daily'

    gran_choice = st.radio(
        "Time granularity",
        options=['Daily', 'Weekly', 'Monthly'],
        index=['Daily', 'Weekly', 'Monthly'].index(st.session_state.time_gran),
        horizontal=True,
        key='time_gran_radio',
        label_visibility='collapsed'
    )
    st.session_state.time_gran = gran_choice

    st.markdown(
        f'<p style="font-family:Outfit,sans-serif;font-size:1.64rem;font-weight:600;'
        f'color:{PITCH_BRAND_COLOR};text-align:center;margin:8px 0 4px 0;text-transform:uppercase;letter-spacing:0.08em;font-size:1.35rem!important;">'
        f'{active_var} &nbsp;·&nbsp; {metric_choice}</p>',
        unsafe_allow_html=True
    )

    freq_map = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'MS'}
    freq = freq_map[gran_choice]

    _TS_NORM = {'Homeowner': 'Yes', 'Renter': 'No', 'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}

    # Visitor time series
    if selected_col == 'state' and tenant_type == 'B2C':
        v_ts = st.session_state.df_state_map.copy()
        v_ts = v_ts.rename(columns={'visit_date': 'ts_date', 'total_visitors': 'Visitors', 'state': selected_col})
    else:
        v_ts = st.session_state.df_demo_cube.copy()
        v_ts = v_ts.rename(columns={'visit_date': 'ts_date', 'total_visitors': 'Visitors'})

    if selected_col in v_ts.columns:
        v_ts[selected_col] = v_ts[selected_col].replace(_TS_NORM)
    v_ts = v_ts[~v_ts[selected_col].isin(EXCLUDE_LIST)]
    v_ts['ts_date'] = pd.to_datetime(v_ts['ts_date'])
    if v_ts['ts_date'].dt.tz is not None:
        v_ts['ts_date'] = v_ts['ts_date'].dt.tz_convert(None)
    v_agg = v_ts.groupby([pd.Grouper(key='ts_date', freq=freq), selected_col])['Visitors'].sum().reset_index()

    # Order time series
    if not df_p_filtered.empty and selected_col in df_p_filtered.columns:
        p_ts = df_p_filtered.copy()
        p_ts['ts_date'] = pd.to_datetime(p_ts['order_date']).dt.tz_convert(None).dt.normalize() if pd.to_datetime(p_ts['order_date']).dt.tz is not None else pd.to_datetime(p_ts['order_date']).dt.normalize()
        if selected_col in p_ts.columns:
            p_ts[selected_col] = p_ts[selected_col].replace(_TS_NORM)
        p_ts = p_ts[~p_ts[selected_col].isin(EXCLUDE_LIST)]
        p_agg = p_ts.groupby([pd.Grouper(key='ts_date', freq=freq), selected_col]).agg(
            Purchases=('Order_ID', 'nunique'),
            Revenue=('Total', 'sum')
        ).reset_index()
    else:
        p_agg = pd.DataFrame(columns=['ts_date', selected_col, 'Purchases', 'Revenue'])

    # Normalize ts_date to timezone-naive on both sides before merging
    v_agg['ts_date'] = pd.to_datetime(v_agg['ts_date']).dt.tz_localize(None) if pd.to_datetime(v_agg['ts_date']).dt.tz is not None else pd.to_datetime(v_agg['ts_date'])
    if not p_agg.empty:
        p_agg['ts_date'] = pd.to_datetime(p_agg['ts_date']).dt.tz_localize(None) if pd.to_datetime(p_agg['ts_date']).dt.tz is not None else pd.to_datetime(p_agg['ts_date'])

    # Merge and compute metrics
    ts_df = pd.merge(v_agg, p_agg, on=['ts_date', selected_col], how='outer').fillna(0)
    # Purchasers are also visitors — same fix as the main table
    ts_df['Visitors']    = ts_df['Visitors'] + ts_df['Purchases']
    ts_df['Conv %']      = (ts_df['Purchases'] / ts_df['Visitors'].replace(0, 1) * 100).round(2)
    ts_df['Rev/Visitor'] = (ts_df['Revenue']   / ts_df['Visitors'].replace(0, 1)).round(2)
    ts_df = ts_df.sort_values('ts_date')

    ts_metric_col = metric_map[metric_choice]  # e.g. 'Rev/Visitor'

    if not ts_df.empty:
        import altair as alt

        CHART_COLORS = ['#4D148C', '#7C3AED', '#20B2AA', '#F59E0B', '#E11D48', '#059669', '#A78BFA', '#0EA5E9']
        segments = sorted([s for s in ts_df[selected_col].unique() if str(s) not in EXCLUDE_LIST])
        active_segs = [s for s in segments if ts_df[ts_df[selected_col] == s][ts_metric_col].sum() > 0]

        if active_segs:
            chart_df = ts_df[ts_df[selected_col].isin(active_segs)].copy()
            chart_df = chart_df.rename(columns={selected_col: 'Segment', ts_metric_col: 'Value'})
            chart_df['ts_date'] = pd.to_datetime(chart_df['ts_date'])

            # Vega labelExpr for axis tick labels (avoids d3 % multiplier issue)
            label_expr = {
                'Rev/Visitor': "'$' + format(datum.value, ',.2f')",
                'Revenue':     "'$' + format(datum.value, ',.0f')",
                'Conv %':      "format(datum.value, '.1f') + '%'",
                'Purchases':   "format(datum.value, ',.0f')",
                'Visitors':    "format(datum.value, ',.0f')",
            }.get(ts_metric_col, "format(datum.value, ',.2f')")

            # d3 format for tooltip (no % needed — title carries it)
            tt_fmt   = {'Rev/Visitor': '$,.2f', 'Revenue': '$,.0f',
                        'Conv %': '.1f', 'Purchases': ',.0f', 'Visitors': ',.0f'}.get(ts_metric_col, ',.2f')
            tt_title = metric_choice + (' (%)' if ts_metric_col == 'Conv %' else '')

            color_scale = alt.Scale(domain=active_segs, range=CHART_COLORS[:len(active_segs)])

            x_enc = alt.X('ts_date:T', axis=alt.Axis(
                format='%b %d', labelColor='#0F172A', tickColor='#EBE4F4',
                domainColor='#EBE4F4', gridOpacity=0, labelFontSize=11, labelFont='Outfit',
                title=None,
            ))
            y_enc = alt.Y('Value:Q', title='', axis=alt.Axis(
                labelExpr=label_expr,
                labelColor='#0F172A', gridColor='#F1F5F9',
                domainOpacity=0, tickOpacity=0,
                labelFontSize=11, labelFont='Outfit',
            ))
            color_enc = alt.Color('Segment:N', scale=color_scale,
                legend=alt.Legend(
                    orient='top', title=None,
                    labelFontSize=12, labelFont='Outfit', labelColor='#0F172A',
                    symbolStrokeWidth=3, symbolSize=120, padding=6,
                ))

            line = alt.Chart(chart_df).mark_line(
                strokeWidth=2.5, interpolate='monotone', opacity=0.9
            ).encode(x=x_enc, y=y_enc, color=color_enc)

            points = alt.Chart(chart_df).mark_circle(size=55, opacity=1).encode(
                x=x_enc,
                y=y_enc,
                color=alt.Color('Segment:N', scale=color_scale, legend=None),
                tooltip=[
                    alt.Tooltip('ts_date:T',  title='Date',     format='%b %d, %Y'),
                    alt.Tooltip('Segment:N',  title=active_var),
                    alt.Tooltip('Value:Q',    title=tt_title,   format=tt_fmt),
                ]
            )

            st.altair_chart(
                (line + points).properties(height=320, background='transparent'),
                use_container_width=True
            )

    st.divider()

    # =====================================================
    # MULTI-VARIABLE COMBINATION MATRIX
    # =====================================================
    st.markdown('<p class="section-title">Multi-Variable Combination Matrix</p>', unsafe_allow_html=True)

    # Session state for matrix
    if 'matrix_vars' not in st.session_state:
        st.session_state.matrix_vars = []
    if 'matrix_filters' not in st.session_state:
        st.session_state.matrix_filters = {}

    valid_matrix_configs = [c for c in configs if c[1] != 'state']

    # ── VARIABLE CHIP ROW — st.pills naturally sizes to text, supports multi-select ──
    st.markdown('<p class="ctrl-label" style="margin-bottom:8px;text-transform:uppercase;font-size:0.72rem;font-weight:700;letter-spacing:0.09em;color:#94A3B8;">Select Variables</p>', unsafe_allow_html=True)

    var_labels   = [label for label, _ in valid_matrix_configs]
    label_to_col = {label: col for label, col in valid_matrix_configs}

    selected_labels = st.pills(
        "Variables",
        options=var_labels,
        selection_mode="multi",
        label_visibility="collapsed",
        key="mx_var_pills"
    )
    if selected_labels is None:
        selected_labels = []

    # Initialise filters and pills state for newly added variables
    for lbl in selected_labels:
        col_name = label_to_col[lbl]
        if col_name not in st.session_state.matrix_filters:
            st.session_state.matrix_filters[col_name] = []
            st.session_state[f"mx_ms_{col_name}"] = ["All"]  # auto-select All

    st.session_state.matrix_vars = [label_to_col[l] for l in selected_labels]

    # ── FILTER PANELS ──
    _MX_NORM     = {'Homeowner': 'Yes', 'Renter': 'No', 'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}
    included_types   = [col for col in st.session_state.matrix_vars
                        if col in [c[1] for c in valid_matrix_configs]]
    selected_filters = {}

    for label, col_name in valid_matrix_configs:
        if col_name not in included_types:
            continue

        raw_opts = sorted([str(x) for x in st.session_state.df_demo_cube[col_name].unique()
                           if str(x) not in EXCLUDE_LIST])
        opts    = list(dict.fromkeys([_MX_NORM.get(o, o) for o in raw_opts]))
        current = st.session_state.matrix_filters.get(col_name, [])
        is_all  = (current == [])

        pills_key = f"mx_ms_{col_name}"

        # Auto-select "All" when variable is first activated
        if pills_key not in st.session_state:
            st.session_state[pills_key] = ["All"]

        with st.container(border=True):
            st.markdown(
                f'<p style="font-family:Outfit,sans-serif;font-size:0.62rem;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.1em;color:#94A3B8;margin-bottom:6px;">'
                f'{label} — filter</p>',
                unsafe_allow_html=True
            )
            raw_sel = st.pills(
                label,
                options=["All"] + opts,
                selection_mode="multi",
                label_visibility="collapsed",
                key=pills_key,
            )

            # All logic: nothing selected → revert to All; All + specific → drop All
            if not raw_sel:
                new_val = []  # empty = all
            elif "All" in raw_sel and len(raw_sel) > 1:
                # User just picked a specific value while All was selected → drop All
                new_val = [v for v in raw_sel if v != "All"]
            elif "All" in raw_sel:
                new_val = []  # All selected, no filter
            else:
                new_val = list(raw_sel)

            st.session_state.matrix_filters[col_name] = new_val

        if new_val:
            selected_filters[col_name] = new_val

    if included_types:
        combos       = []
        filtered_cols   = list(selected_filters.keys())
        unfiltered_cols = [c for c in included_types if c not in filtered_cols]
        base_v = st.session_state.df_demo_cube.copy()
        base_p = df_p_filtered.copy()
        # Normalize all included columns in both datasets so they merge correctly
        for _nc in included_types:
            if _nc in base_v.columns: base_v[_nc] = base_v[_nc].replace(_MX_NORM)
            if not base_p.empty and _nc in base_p.columns: base_p[_nc] = base_p[_nc].replace(_MX_NORM)

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
            res['Conv %']      = (res['Purchases'] / res['Visitors'].replace(0, 1) * 100).round(2)
            res['Rev/Visitor'] = (res['Revenue']   / res['Visitors'].replace(0, 1)).round(2)
            sort_col   = metric_map[metric_choice]
            final_res  = res[res['Purchases'] >= min_purchasers].sort_values(sort_col, ascending=is_ascending)
            if not final_res.empty:
                # Display-level cleanup for raw Y/N codes
                _display_map = {'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}
                for _col in included_types:
                    if _col in final_res.columns:
                        final_res[_col] = final_res[_col].replace(_display_map)
                st.metric("Total Segments Found", f"{len(final_res):,}")
                final_res.insert(0, 'Rank', range(1, len(final_res) + 1))
                rename_dict  = {c[1]: c[0] for c in configs}
                display_cols = ['Rank'] + [rename_dict.get(c, c) for c in included_types] + ['Revenue', 'Visitors', 'Purchases', 'Conv %', 'Rev/Visitor']
                display_df   = final_res.head(50).rename(columns=rename_dict)[display_cols]
                render_premium_table(display_df.style\
                    .set_properties(**{'font-weight': '800'}, subset=[metric_map[metric_choice]])\
                    .format({
                        'Rank': '{:.0f}', 'Visitors': '{:,.0f}', 'Purchases': '{:,.0f}',
                        'Revenue': '${:,.2f}', 'Conv %': '{:.2f}%', 'Rev/Visitor': '${:,.2f}'
                    }).background_gradient(subset=['Rev/Visitor', 'Conv %'], cmap=brand_gradient))

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
