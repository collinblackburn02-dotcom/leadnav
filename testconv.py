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
BQ_USERS_TABLE       = "leadnav-hhs.leadnav_platform.platform_users"
BQ_LOGIN_LOGS_TABLE  = "leadnav-hhs.leadnav_platform.platform_login_logs"
BQ_PIXEL_RAW_TABLE   = "leadnav-hhs.leadnav_platform.pixel_events_raw"

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

        /* ── Filter option pills — unselected: faded white ── */
        [data-testid="stBaseButton-pills"] {{
            border-radius: 999px !important;
            font-size: 0.75rem !important;
            font-weight: 500 !important;
            padding: 4px 13px !important;
            border: 1px solid #E2D9F3 !important;
            background: #FFFFFF !important;
            color: #B0A0D0 !important;
            text-transform: none !important;
            letter-spacing: 0 !important;
        }}
        [data-testid="stBaseButton-pills"] p,
        [data-testid="stBaseButton-pills"] span {{
            font-size: 0.75rem !important;
            text-transform: none !important;
            color: #B0A0D0 !important;
        }}
        /* ── Filter option pills — selected: light purple like top filter chips ── */
        [data-testid="stBaseButton-pillsActive"] {{
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
        [data-testid="stBaseButton-pillsActive"] p,
        [data-testid="stBaseButton-pillsActive"] span {{
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            color: {primary_color} !important;
            text-transform: none !important;
        }}
        /* ── Variable selector pills override — keep solid purple ── */
        .st-key-mx_var_pills [data-testid="stBaseButton-pills"] {{
            border-radius: 8px !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            padding: 5px 13px !important;
            border: 1.5px solid #D8C8F5 !important;
            background: #FFFFFF !important;
            color: {primary_color} !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
        }}
        .st-key-mx_var_pills [data-testid="stBaseButton-pillsActive"] {{
            border-radius: 8px !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            padding: 5px 13px !important;
            border: 1.5px solid {primary_color} !important;
            background: {primary_color} !important;
            color: #FFFFFF !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
        }}
        .st-key-mx_var_pills [data-testid="stBaseButton-pills"] p,
        .st-key-mx_var_pills [data-testid="stBaseButton-pills"] span,
        .st-key-mx_var_pills [data-testid="stBaseButton-pillsActive"] p,
        .st-key-mx_var_pills [data-testid="stBaseButton-pillsActive"] span {{
            font-size: 0.85rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.09em !important;
            color: inherit !important;
        }}


        /* ── MAIN VIEW TAB SELECTOR — underline style, beats general radio CSS ── */
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector > div[role="radiogroup"],
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector > div {{
            gap: 0 !important;
            border-bottom: 1.5px solid #EBE4F4 !important;
            justify-content: flex-end !important;
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
        }}
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector [data-baseweb="radio"] {{
            border-radius: 0 !important;
            background: transparent !important;
            border: none !important;
            border-bottom: 1.5px solid transparent !important;
            padding: 3px 10px 5px !important;
            margin-bottom: -1.5px !important;
            cursor: pointer !important;
        }}
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector [data-baseweb="radio"]:has(input:checked) {{
            background: transparent !important;
            border-color: transparent !important;
            border-bottom-color: {primary_color} !important;
        }}
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector [data-baseweb="radio"] > div:first-child {{
            display: none !important;
        }}
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector [data-baseweb="radio"] > div:last-child p {{
            font-size: 0.42rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.12em !important;
            color: #94A3B8 !important;
            margin: 0 !important;
            white-space: nowrap !important;
        }}
        [data-testid="stMain"] .stRadio.st-key-main_tab_selector [data-baseweb="radio"]:has(input:checked) > div:last-child p {{
            color: {primary_color} !important;
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
@st.cache_data(ttl=3600, show_spinner=False)
def load_visitor_base(pixel_id, tenant_type):
    try:
        client = get_bq_client()
        # Support comma-separated pixel IDs for multi-pixel users
        pixel_ids = [p.strip() for p in str(pixel_id).split(',') if p.strip()]
        if tenant_type == "B2C":
            query = f"""
            SELECT pixel_id, visit_date, total_visitors, state, gender, age_range, income_bucket,
                   net_worth_bucket, homeowner, marital_status, children
            FROM `{BQ_B2C_VISITOR_TABLE}`
            WHERE pixel_id IN UNNEST(@pixel_ids)
            """
        else:
            query = f"""
            SELECT pixel_id, visit_date, total_visitors, industry, employee_count_range, job_title,
                   seniority, company_revenue
            FROM `{BQ_B2B_VISITOR_TABLE}`
            WHERE pixel_id IN UNNEST(@pixel_ids)
            """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pixel_ids", "STRING", pixel_ids)]
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
    elif num < 100000: return '$50k-100k'
    elif num < 200000: return '$100k-200k'
    else: return '$200k+'

def bucket_net_worth(val):
    num = get_real_number(val)
    if num is None: return 'Unknown'
    if num < 100000: return 'Under $100k'
    elif num < 500000: return '$100k-500k'
    elif num < 1000000: return '$500k-1M'
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

@st.cache_data(ttl=3600, show_spinner=False)
def load_order_base(pixel_id, tenant_type):
    try:
        client = get_bq_client()
        # Support comma-separated pixel IDs for multi-pixel users
        pixel_ids = [p.strip() for p in str(pixel_id).split(',') if p.strip()]
        query = f"""
        SELECT pixel_id, order_id, order_date, customer_email, revenue, lineitem_name, state,
               gender, age_range, income_bucket, net_worth_bucket, homeowner, marital_status, children,
               company_name, company_industry, employee_count_range, job_title, seniority, company_revenue
        FROM `{BQ_ORDERS_TABLE}`
        WHERE pixel_id IN UNNEST(@pixel_ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pixel_ids", "STRING", pixel_ids)]
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
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'main_tab_selector' not in st.session_state:
    st.session_state.main_tab_selector = 'Customer Insights'
if 'cust_metric' not in st.session_state:
    st.session_state.cust_metric = '% of Purchasers'

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
                user_info, auth_error = authenticate_user(username, password)
                if user_info:
                    st.session_state.username    = username
                    st.session_state.pixel_id    = user_info['pixel_id']
                    st.session_state.tenant_type = user_info['tenant_type']
                    st.session_state.client_name = user_info['client_name']
                    st.session_state.is_admin    = user_info.get('is_admin', False)
                    log_login(username, True, user_info['pixel_id'])
                    if user_info.get('is_admin'):
                        st.session_state.app_state = 'admin'
                        st.rerun()
                    else:
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
                            st.session_state.df_demo   = df_demo
                            st.session_state.df_state  = df_state
                            st.session_state.df_orders = df_orders
                            st.session_state.app_state = 'dashboard'
                            st.rerun()
                else:
                    log_login(username, False)
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
    # Use only the primary (first) pixel ID for saving enriched orders
    pixel_id = str(pixel_id).split(',')[0].strip()
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
    # Always save under the primary (first) pixel ID
    pixel_id = str(pixel_id).split(',')[0].strip()
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

# ================ 7C. ADMIN HELPER FUNCTIONS =================

def ensure_admin_tables(client):
    """Create admin tables if they don't exist."""
    users_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_USERS_TABLE}` (
        username STRING,
        password STRING,
        pixel_id STRING,
        tenant_type STRING,
        client_name STRING,
        is_admin BOOL,
        is_active BOOL,
        created_at TIMESTAMP
    )"""
    logs_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_LOGIN_LOGS_TABLE}` (
        username STRING,
        login_timestamp TIMESTAMP,
        success BOOL,
        pixel_id STRING
    )"""
    try:
        client.query(users_ddl).result()
        client.query(logs_ddl).result()
    except Exception:
        pass

def authenticate_user(username, password):
    """Authenticate via BQ users table, fall back to st.secrets."""
    try:
        client = get_bq_client()
        ensure_admin_tables(client)
        query = f"""
        SELECT * FROM `{BQ_USERS_TABLE}`
        WHERE username = @username AND password = @password AND is_active = true
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("password", "STRING", password),
        ])
        df = client.query(query, job_config=job_config).to_dataframe()
        if not df.empty:
            row = df.iloc[0]
            return {
                'pixel_id':   str(row['pixel_id']),
                'tenant_type': str(row['tenant_type']),
                'client_name': str(row['client_name']),
                'is_admin':    bool(row['is_admin']),
            }, None
    except Exception:
        pass
    # Fallback: st.secrets
    users = dict(st.secrets.get("users", {}))
    if username in users and users[username].get("password") == password:
        return {
            'pixel_id':    users[username].get('pixel_id', ''),
            'tenant_type': users[username].get('tenant_type', 'B2C'),
            'client_name': users[username].get('client_name', username.replace('_', ' ').title()),
            'is_admin':    users[username].get('is_admin', False),
        }, None
    return None, "Invalid username or password"

def log_login(username, success, pixel_id=""):
    """Log a login attempt to BigQuery."""
    try:
        client = get_bq_client()
        client.insert_rows_json(BQ_LOGIN_LOGS_TABLE, [{
            'username':        username,
            'login_timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
            'success':         success,
            'pixel_id':        pixel_id,
        }])
    except Exception:
        pass

def save_visitor_data_to_bq(df, pixel_id):
    """Upload raw pixel event CSV to pixel_events_raw. Visitor summary tables
    update automatically when the scheduled BigQuery query next runs."""
    # All expected columns in pixel_events_raw (uppercase)
    RAW_COLS = [
        'PIXEL_ID','HEM_SHA256','EVENT_TIMESTAMP','REFERRER_URL','FULL_URL','EDID',
        'FIRST_NAME','LAST_NAME','PERSONAL_ADDRESS','PERSONAL_CITY','PERSONAL_STATE',
        'PERSONAL_ZIP','PERSONAL_ZIP4','AGE_RANGE','CHILDREN','GENDER','HOMEOWNER',
        'MARRIED','NET_WORTH','INCOME_RANGE','ALL_LANDLINES','LANDLINE_DNC',
        'ALL_MOBILES','MOBILE_DNC','PERSONAL_EMAILS','PERSONAL_VERIFIED_EMAILS',
        'SHA256_PERSONAL_EMAIL','COMPANY_NAME','COMPANY_DESCRIPTION',
        'COMPANY_EMPLOYEE_COUNT','COMPANY_DOMAIN','COMPANY_ADDRESS','COMPANY_CITY',
        'COMPANY_STATE','COMPANY_ZIP','COMPANY_PHONE','COMPANY_REVENUE','COMPANY_SIC',
        'COMPANY_NAICS','COMPANY_INDUSTRY','BUSINESS_EMAIL','BUSINESS_VERIFIED_EMAILS',
        'SHA256_BUSINESS_EMAIL','JOB_TITLE','HEADLINE','DEPARTMENT','SENIORITY_LEVEL',
        'INFERRED_YEARS_EXPERIENCE','COMPANY_NAME_HISTORY','JOB_TITLE_HISTORY',
    ]
    try:
        client = get_bq_client()
        df = df.copy()
        # Normalise column names to uppercase to match BQ schema
        df.columns = [c.strip().upper() for c in df.columns]

        # Always set PIXEL_ID from the selected client
        df['PIXEL_ID'] = str(pixel_id).split(',')[0].strip()

        # Parse timestamp if present, keep as datetime for BQ TIMESTAMP type
        if 'EVENT_TIMESTAMP' in df.columns:
            df['EVENT_TIMESTAMP'] = pd.to_datetime(df['EVENT_TIMESTAMP'], errors='coerce')

        # Fill any missing schema columns with None
        for col in RAW_COLS:
            if col not in df.columns:
                df[col] = None

        df_upload = df[RAW_COLS].copy()

        # Cast all STRING columns to str — BQ schema is all STRING except EVENT_TIMESTAMP
        for col in RAW_COLS:
            if col != 'EVENT_TIMESTAMP':
                df_upload[col] = df_upload[col].astype(str).replace('nan', None)

        job = client.load_table_from_dataframe(
            df_upload, BQ_PIXEL_RAW_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        )
        job.result()
        return True, f"✅ {len(df_upload):,} raw events uploaded. Visitor summary tables will update on the next scheduled query run."
    except Exception as e:
        return False, str(e)

def run_visitor_rollup():
    """Run the B2C + B2B visitor rollup for all unprocessed dates across all pixels."""
    B2C_SQL = """
    INSERT INTO `leadnav-hhs.leadnav_platform.b2c_visitor_summary`
      (pixel_id, visit_date, total_visitors, state, gender, age_range,
       income_bucket, net_worth_bucket, homeowner, marital_status, children)
    SELECT
      r.PIXEL_ID, DATE(r.EVENT_TIMESTAMP,'America/Chicago'),
      COUNT(*),
      UPPER(TRIM(r.PERSONAL_STATE)),
      CASE WHEN UPPER(TRIM(r.GENDER)) IN ('M','MALE') THEN 'Male' WHEN UPPER(TRIM(r.GENDER)) IN ('F','FEMALE') THEN 'Female' ELSE 'Unknown' END,
      CASE WHEN r.AGE_RANGE IS NULL OR TRIM(r.AGE_RANGE)='' THEN 'Unknown' ELSE TRIM(r.AGE_RANGE) END,
      CASE WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.INCOME_RANGE,',',''),r'\\$(\\d+)') AS INT64)<50000 THEN 'Under $50k'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.INCOME_RANGE,',',''),r'\\$(\\d+)') AS INT64)<100000 THEN '$50k-$100k'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.INCOME_RANGE,',',''),r'\\$(\\d+)') AS INT64)<200000 THEN '$100k-$200k'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.INCOME_RANGE,',',''),r'\\$(\\d+)') AS INT64)>=200000 THEN '$200k+'
           ELSE 'Unknown' END,
      CASE WHEN LOWER(r.NET_WORTH) LIKE '%less than%' OR LOWER(r.NET_WORTH) LIKE '%-$%' OR SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.NET_WORTH,',',''),r'\\$(\\d+)') AS INT64)<100000 THEN 'Under $100k'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.NET_WORTH,',',''),r'\\$(\\d+)') AS INT64)<500000 THEN '$100k-$500k'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.NET_WORTH,',',''),r'\\$(\\d+)') AS INT64)<1000000 THEN '$500k-$1M'
           WHEN SAFE_CAST(REGEXP_EXTRACT(REPLACE(r.NET_WORTH,',',''),r'\\$(\\d+)') AS INT64)>=1000000 OR LOWER(r.NET_WORTH) LIKE '%1,000,000%' THEN '$1M+'
           ELSE 'Unknown' END,
      CASE WHEN LOWER(r.HOMEOWNER) LIKE '%homeowner%' THEN 'Homeowner' WHEN LOWER(r.HOMEOWNER) LIKE '%renter%' THEN 'Renter' ELSE 'Unknown' END,
      CASE WHEN LOWER(TRIM(r.MARRIED)) IN ('married','y','yes') THEN 'Married' WHEN LOWER(TRIM(r.MARRIED)) IN ('single','n','no') THEN 'Single' WHEN LOWER(TRIM(r.MARRIED)) LIKE '%divorced%' THEN 'Divorced' ELSE 'Unknown' END,
      CASE WHEN UPPER(TRIM(r.CHILDREN))='Y' THEN 'Yes' WHEN UPPER(TRIM(r.CHILDREN))='N' THEN 'No' ELSE 'Unknown' END
    FROM `leadnav-hhs.leadnav_platform.pixel_events_raw` r
    WHERE r.PIXEL_ID IS NOT NULL
      AND CONCAT(r.PIXEL_ID, CAST(DATE(r.EVENT_TIMESTAMP,'America/Chicago') AS STRING)) NOT IN (
        SELECT DISTINCT CONCAT(pixel_id, CAST(visit_date AS STRING)) FROM `leadnav-hhs.leadnav_platform.b2c_visitor_summary`
      )
    GROUP BY 1,2,4,5,6,7,8,9,10,11
    """

    B2B_SQL = """
    INSERT INTO `leadnav-hhs.leadnav_platform.b2b_visitor_summary`
      (pixel_id, visit_date, total_visitors, industry, employee_count_range, job_title, seniority, company_revenue)
    SELECT
      r.PIXEL_ID, DATE(r.EVENT_TIMESTAMP,'America/Chicago'),
      COUNT(*),
      COALESCE(NULLIF(TRIM(r.COMPANY_INDUSTRY),''),'Unknown'),
      COALESCE(NULLIF(TRIM(r.COMPANY_EMPLOYEE_COUNT),''),'Unknown'),
      COALESCE(NULLIF(TRIM(r.JOB_TITLE),''),'Unknown'),
      CASE WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='cxo' THEN 'CXO'
           WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='vp' THEN 'VP'
           WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='director' THEN 'Director'
           WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='manager' THEN 'Manager'
           WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='staff' THEN 'Staff'
           WHEN LOWER(TRIM(r.SENIORITY_LEVEL))='entry' THEN 'Entry'
           WHEN TRIM(r.SENIORITY_LEVEL)='' OR r.SENIORITY_LEVEL IS NULL THEN 'Unknown'
           ELSE INITCAP(TRIM(r.SENIORITY_LEVEL)) END,
      COALESCE(NULLIF(TRIM(r.COMPANY_REVENUE),''),'Unknown')
    FROM `leadnav-hhs.leadnav_platform.pixel_events_raw` r
    WHERE r.PIXEL_ID IS NOT NULL
      AND CONCAT(r.PIXEL_ID, CAST(DATE(r.EVENT_TIMESTAMP,'America/Chicago') AS STRING)) NOT IN (
        SELECT DISTINCT CONCAT(pixel_id, CAST(visit_date AS STRING)) FROM `leadnav-hhs.leadnav_platform.b2b_visitor_summary`
      )
    GROUP BY 1,2,4,5,6,7,8
    """
    try:
        client = get_bq_client()
        b2c_job = client.query(B2C_SQL)
        b2c_job.result()
        b2b_job = client.query(B2B_SQL)
        b2b_job.result()
        load_visitor_base.clear()
        return True, "✅ Visitor rollup complete — B2C and B2B summary tables updated for all unprocessed dates."
    except Exception as e:
        return False, f"Rollup error: {e}"

def get_all_users():
    try:
        client = get_bq_client()
        df = client.query(f"SELECT * FROM `{BQ_USERS_TABLE}` ORDER BY created_at DESC").to_dataframe()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def save_user_to_bq(username, password, pixel_id, tenant_type, client_name, is_admin=False, is_active=True):
    try:
        client = get_bq_client()
        # Upsert: delete existing then insert
        del_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username)
        ])
        client.query(f"DELETE FROM `{BQ_USERS_TABLE}` WHERE username = @username", job_config=del_cfg).result()
        errors = client.insert_rows_json(BQ_USERS_TABLE, [{
            'username': username, 'password': password, 'pixel_id': pixel_id,
            'tenant_type': tenant_type, 'client_name': client_name,
            'is_admin': is_admin, 'is_active': is_active,
            'created_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        }])
        return (False, str(errors)) if errors else (True, "User saved.")
    except Exception as e:
        return False, str(e)

def set_user_active(username, active):
    try:
        client = get_bq_client()
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("active",   "BOOL",   active),
        ])
        client.query(
            f"UPDATE `{BQ_USERS_TABLE}` SET is_active=@active WHERE username=@username",
            job_config=cfg
        ).result()
        return True, "Updated."
    except Exception as e:
        return False, str(e)

def delete_user_from_bq(username):
    try:
        client = get_bq_client()
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username)
        ])
        client.query(f"DELETE FROM `{BQ_USERS_TABLE}` WHERE username=@username", job_config=cfg).result()
        return True, "User deleted."
    except Exception as e:
        return False, str(e)

def get_usage_summary():
    """Per-client usage stats from BQ."""
    try:
        client = get_bq_client()
        orders_q = f"""
        SELECT pixel_id,
               COUNT(DISTINCT order_id) AS total_orders,
               ROUND(COUNTIF(gender IS NOT NULL AND gender != 'Unknown') / NULLIF(COUNT(*),0) * 100, 1) AS match_rate_pct,
               MAX(order_date) AS last_upload
        FROM `{BQ_ORDERS_TABLE}`
        GROUP BY pixel_id
        """
        logins_q = f"""
        SELECT pixel_id, MAX(login_timestamp) AS last_login
        FROM `{BQ_LOGIN_LOGS_TABLE}`
        WHERE success = true
        GROUP BY pixel_id
        """
        df_orders = client.query(orders_q).to_dataframe()
        df_logins = client.query(logins_q).to_dataframe()
        df = pd.merge(df_orders, df_logins, on='pixel_id', how='left')
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def get_login_history(limit=50):
    try:
        client = get_bq_client()
        df = client.query(f"""
        SELECT username, login_timestamp, success, pixel_id
        FROM `{BQ_LOGIN_LOGS_TABLE}`
        ORDER BY login_timestamp DESC
        LIMIT {limit}
        """).to_dataframe()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def get_aggregate_analytics(start_date=None, end_date=None):
    """Aggregate + per-client analytics from BQ visitor + orders tables."""
    try:
        client = get_bq_client()
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE order_date BETWEEN '{start_date}' AND '{end_date}'"
        per_client_q = f"""
        SELECT
            o.pixel_id,
            COUNT(DISTINCT o.order_id)                                              AS purchases,
            ROUND(SUM(o.revenue), 2)                                                AS revenue,
            MAX(v.total_visitors)                                                   AS visitors
        FROM `{BQ_ORDERS_TABLE}` o
        LEFT JOIN (
            SELECT pixel_id, SUM(total_visitors) AS total_visitors
            FROM `{BQ_B2C_VISITOR_TABLE}` GROUP BY pixel_id
        ) v USING (pixel_id)
        {date_filter}
        GROUP BY o.pixel_id
        """
        df = client.query(per_client_q).to_dataframe()
        df['conv_pct']     = (df['purchases'] / df['visitors'].replace(0, 1) * 100).round(2)
        df['rev_per_visit'] = (df['revenue'] / df['visitors'].replace(0, 1)).round(2)
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

# ================ 8. ADMIN PAGE =================
def admin_page():
    # Hide sidebar for admin
    st.markdown('<style>[data-testid="stSidebar"]{display:none!important;}[data-testid="collapsedControl"]{display:none!important;}</style>', unsafe_allow_html=True)

    # Header
    hdr_l, hdr_r = st.columns([8, 1])
    with hdr_l:
        st.markdown(
            f'<div style="font-family:\'Playfair Display\',serif;font-size:1.8rem;font-weight:700;color:#0F172A;padding:1rem 0 0.5rem 0;">'
            f'Lead<span style="color:{PITCH_BRAND_COLOR};">Navigator</span> '
            f'<span style="font-size:1rem;font-weight:600;color:#94A3B8;font-family:Outfit,sans-serif;"> Admin Console</span></div>',
            unsafe_allow_html=True
        )
    with hdr_r:
        if st.button("Logout", key="admin_logout"):
            for k in ['app_state','username','pixel_id','tenant_type','client_name','is_admin']:
                st.session_state[k] = 'login' if k == 'app_state' else None
            st.rerun()

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["👥  Users", "📁  Data Management", "📊  Usage", "🔢  Analytics"])

    # ── TAB 1: USER MANAGEMENT ──────────────────────────────
    with tab1:
        st.markdown('<p class="section-title">User Management</p>', unsafe_allow_html=True)
        users_df, err = get_all_users()
        if err:
            st.warning(f"Could not load users from database: {err}.")
            users_df = pd.DataFrame()

        # Show secrets users not yet in BQ
        secrets_users = dict(st.secrets.get("users", {}))
        bq_usernames  = set(users_df['username'].tolist()) if not users_df.empty else set()
        secrets_only  = {u: d for u, d in secrets_users.items() if u not in bq_usernames}

        if secrets_only:
            st.info(f"**{len(secrets_only)} user(s) are still in st.secrets and not yet in the database.** Use Migrate to add them.")
            for uname, data in secrets_only.items():
                with st.expander(f"⚠️ {uname}  —  {data.get('client_name', uname)}  (secrets only)"):
                    st.caption(f"Pixel ID: {data.get('pixel_id','')}  |  Tenant: {data.get('tenant_type','B2C')}  |  Admin: {data.get('is_admin', False)}")
                    if st.button(f"Migrate {uname} to database", key=f"migrate_{uname}", type="primary"):
                        ok, msg = save_user_to_bq(
                            uname,
                            data.get('password', ''),
                            data.get('pixel_id', ''),
                            data.get('tenant_type', 'B2C'),
                            data.get('client_name', uname),
                            data.get('is_admin', False),
                            True
                        )
                        st.success(msg) if ok else st.error(msg)
                        st.rerun()
            st.markdown("---")

        if not users_df.empty:
            for _, row in users_df.iterrows():
                uname = row['username']
                active = bool(row.get('is_active', True))
                with st.expander(f"{'🔑 ' if row.get('is_admin') else '👤 '} {uname}  —  {row.get('client_name','')}  ({row.get('pixel_id','')})"):
                    ec1, ec2, ec3 = st.columns(3)
                    new_pw     = ec1.text_input("New password", key=f"pw_{uname}", placeholder="leave blank to keep")
                    new_client = ec2.text_input("Client name", value=row.get('client_name',''), key=f"cn_{uname}")
                    new_pixel  = ec3.text_input("Pixel ID(s)", value=row.get('pixel_id',''), key=f"px_{uname}", help="Comma-separated for multiple pixels")
                    ec4, ec5, ec6, ec7 = st.columns(4)
                    new_tenant = ec4.selectbox("Tenant type", ["B2C","B2B"], index=0 if row.get('tenant_type')=="B2C" else 1, key=f"tt_{uname}")
                    new_admin  = ec5.checkbox("Admin", value=bool(row.get('is_admin',False)), key=f"adm_{uname}")

                    ba, bd, bt = ec6.button("💾 Save", key=f"save_{uname}"), ec7.button("🗑 Delete", key=f"del_{uname}"), ec6.button(f"{'🔴 Deactivate' if active else '🟢 Activate'}", key=f"tog_{uname}")
                    if ba:
                        pw = new_pw if new_pw else str(row.get('password',''))
                        ok, msg = save_user_to_bq(uname, pw, new_pixel, new_tenant, new_client, new_admin, active)
                        st.success(msg) if ok else st.error(msg)
                        st.rerun()
                    if bd:
                        ok, msg = delete_user_from_bq(uname)
                        st.success(msg) if ok else st.error(msg)
                        st.rerun()
                    if bt:
                        ok, msg = set_user_active(uname, not active)
                        st.success(msg) if ok else st.error(msg)
                        st.rerun()

        st.markdown("---")
        st.markdown('<p class="section-title">Add New User</p>', unsafe_allow_html=True)
        with st.form("add_user_form"):
            nc1, nc2, nc3 = st.columns(3)
            nu = nc1.text_input("Username")
            np_ = nc2.text_input("Password", type="password")
            npi = nc3.text_input("Pixel ID(s)", help="Comma-separated for multiple pixels e.g. px1,px2")
            nc4, nc5, nc6 = st.columns(3)
            ncn  = nc4.text_input("Client name")
            ntt  = nc5.selectbox("Tenant type", ["B2C","B2B"])
            nadm = nc6.checkbox("Admin user")
            if st.form_submit_button("Add User", type="primary"):
                if nu and np_ and npi:
                    ok, msg = save_user_to_bq(nu, np_, npi, ntt, ncn or nu, nadm, True)
                    st.success(msg) if ok else st.error(msg)
                    st.rerun()
                else:
                    st.error("Username, password and pixel ID are required.")

    # ── TAB 2: DATA MANAGEMENT ──────────────────────────────
    with tab2:
        st.markdown('<p class="section-title">Data Management</p>', unsafe_allow_html=True)
        all_users, _ = get_all_users()
        client_options = []
        if not all_users.empty:
            client_options = all_users[~all_users['is_admin'].fillna(False)][['client_name','pixel_id','tenant_type']].drop_duplicates().values.tolist()

        if client_options:
            client_labels = [f"{c[0]} ({c[1]})" for c in client_options]
            sel_idx = st.selectbox("Select client", range(len(client_labels)), format_func=lambda i: client_labels[i], key="admin_client_sel")
            sel_client = client_options[sel_idx]
            sel_pixel, sel_tenant = sel_client[1], sel_client[2]

            dcol1, dcol2 = st.columns(2)
            with dcol1:
                if st.button("👁 View as this client", type="primary"):
                    with st.spinner("Loading client data..."):
                        df_demo, df_state, err = load_visitor_base(sel_pixel, sel_tenant)
                        df_orders, oerr = load_order_base(sel_pixel, sel_tenant)
                    if not err and not oerr:
                        st.session_state.pixel_id    = sel_pixel
                        st.session_state.tenant_type = sel_tenant
                        st.session_state.client_name = sel_client[0]
                        st.session_state.df_demo     = df_demo
                        st.session_state.df_state    = df_state
                        st.session_state.df_orders   = df_orders
                        st.session_state.app_state   = 'dashboard'
                        st.rerun()
                    else:
                        st.error(err or oerr)

            st.markdown("---")
            st.markdown(f'<p class="ctrl-label">Upload orders for {sel_client[0]}</p>', unsafe_allow_html=True)
            adm_upload = st.file_uploader("Upload CSV", type=['csv'], key="admin_upload")
            if adm_upload:
                adm_btn = st.button("Upload & Enrich", type="primary", key="admin_enrich_btn")
                adm_slot = st.empty()
                if adm_btn:
                    adm_slot.info("⏳ Uploading & enriching... This can take up to 3 minutes.")
                    ok, msg = run_enrichment(adm_upload, sel_pixel, sel_tenant)
                    adm_slot.success(msg) if ok else adm_slot.error(msg)

            st.markdown("---")
            st.markdown(f'<p class="ctrl-label">Upload raw visitor data for {sel_client[0]}</p>', unsafe_allow_html=True)
            st.caption("Upload a raw pixel events CSV. It will be saved to `pixel_events_raw` — visitor summary tables update on the next scheduled query run.")
            vis_upload = st.file_uploader("Upload visitor CSV", type=['csv'], key="admin_vis_upload")
            if vis_upload:
                try:
                    vis_preview = pd.read_csv(vis_upload)
                    st.dataframe(vis_preview.head(3), use_container_width=True)
                    st.caption(f"{len(vis_preview):,} rows · {len(vis_preview.columns)} columns detected")
                    vis_btn  = st.button("💾 Save to pixel_events_raw", type="primary", key="admin_save_vis")
                    vis_slot = st.empty()
                    if vis_btn:
                        vis_slot.info("⏳ Uploading raw event data...")
                        ok, msg = save_visitor_data_to_bq(vis_preview, sel_pixel)
                        if ok:
                            vis_slot.success(msg)
                        else:
                            vis_slot.error(msg)
                except Exception as e:
                    st.error(str(e))

            st.markdown("---")
            st.markdown('<p class="ctrl-label">Run Visitor Rollup</p>', unsafe_allow_html=True)
            st.caption("Processes all unprocessed dates in pixel_events_raw and updates both B2C and B2B summary tables. Equivalent to triggering the scheduled query manually.")
            rollup_btn  = st.button("▶ Run Rollup Now", type="primary", key="admin_run_rollup")
            rollup_slot = st.empty()
            if rollup_btn:
                rollup_slot.info("⏳ Running visitor rollup — this may take a minute...")
                ok, msg = run_visitor_rollup()
                if ok:
                    rollup_slot.success(msg)
                else:
                    rollup_slot.error(msg)

            st.markdown("---")
            st.markdown('<p class="ctrl-label" style="color:#E11D48;">Danger Zone</p>', unsafe_allow_html=True)
            try:
                client_bq = get_bq_client()
                cfg       = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("pid","STRING",sel_pixel)])
                ord_cnt   = client_bq.query(f"SELECT COUNT(*) as n FROM `{BQ_ORDERS_TABLE}` WHERE pixel_id=@pid", job_config=cfg).to_dataframe().iloc[0]['n']
                vis_cnt   = client_bq.query(f"SELECT COUNT(*) as n FROM `{BQ_PIXEL_RAW_TABLE}` WHERE PIXEL_ID=@pid", job_config=cfg).to_dataframe().iloc[0]['n']

                dc1, dc2 = st.columns(2)
                with dc1:
                    st.warning(f"**{int(ord_cnt):,}** orders in database")
                    if st.button("🗑 Delete ALL orders", key="admin_del_orders"):
                        client_bq.query(f"DELETE FROM `{BQ_ORDERS_TABLE}` WHERE pixel_id=@pid", job_config=cfg).result()
                        load_order_base.clear()
                        st.success("All orders deleted.")
                        st.rerun()
                with dc2:
                    st.warning(f"**{int(vis_cnt):,}** visitor rows in database")
                    if st.button("🗑 Delete ALL visitor data", key="admin_del_visitors"):
                        client_bq.query(f"DELETE FROM `{BQ_PIXEL_RAW_TABLE}` WHERE PIXEL_ID=@pid", job_config=cfg).result()
                        load_visitor_base.clear()
                        st.success("All raw visitor data deleted.")
                        st.rerun()
            except Exception as e:
                st.error(str(e))
        else:
            st.info("No client users found. Add users in the Users tab first.")

    # ── TAB 3: USAGE ────────────────────────────────────────
    with tab3:
        st.markdown('<p class="section-title">Usage Summary</p>', unsafe_allow_html=True)
        usage_df, uerr = get_usage_summary()
        if uerr:
            st.error(f"Could not load usage data: {uerr}")
        elif not usage_df.empty:
            # Merge with user table to get client names
            if not all_users.empty:
                usage_df = usage_df.merge(all_users[['pixel_id','client_name']], on='pixel_id', how='left')
            usage_df['match_rate_pct'] = usage_df['match_rate_pct'].fillna(0).astype(str) + '%'
            usage_df['last_upload']    = pd.to_datetime(usage_df['last_upload'], errors='coerce').dt.strftime('%Y-%m-%d')
            usage_df['last_login']     = pd.to_datetime(usage_df['last_login'],  errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
            disp = usage_df[['client_name','pixel_id','total_orders','match_rate_pct','last_upload','last_login']].rename(columns={
                'client_name': 'Client', 'pixel_id': 'Pixel ID', 'total_orders': 'Orders Saved',
                'match_rate_pct': 'Match Rate', 'last_upload': 'Last Upload', 'last_login': 'Last Login'
            })
            st.dataframe(disp, use_container_width=True, hide_index=True)
        else:
            st.info("No usage data yet.")

        st.markdown('<p class="section-title" style="margin-top:2rem;">Login History</p>', unsafe_allow_html=True)
        log_df, lerr = get_login_history(50)
        if not lerr and not log_df.empty:
            log_df['login_timestamp'] = pd.to_datetime(log_df['login_timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            log_df['success'] = log_df['success'].map({True: '✅', False: '❌'})
            st.dataframe(log_df.rename(columns={'username':'User','login_timestamp':'Time','success':'Result','pixel_id':'Pixel'}),
                         use_container_width=True, hide_index=True)
        else:
            st.info("No login history yet.")

    # ── TAB 4: ANALYTICS ────────────────────────────────────
    with tab4:
        st.markdown('<p class="section-title">Analytics</p>', unsafe_allow_html=True)
        anal_df, aerr = get_aggregate_analytics()
        if aerr:
            st.error(f"Could not load analytics: {aerr}")
        elif not anal_df.empty:
            # Aggregate KPI cards
            tot_v = int(anal_df['visitors'].sum())
            tot_r = float(anal_df['revenue'].sum())
            tot_p = int(anal_df['purchases'].sum())
            avg_c = (tot_p / tot_v * 100) if tot_v > 0 else 0
            k1, k2, k3, k4 = st.columns(4)
            for col, lbl, val in zip([k1,k2,k3,k4],
                ['Total Visitors','Total Revenue','Total Purchases','Avg Conv Rate'],
                [f'{tot_v:,.0f}', f'${tot_r:,.0f}', f'{tot_p:,.0f}', f'{avg_c:.2f}%']):
                col.markdown(f'<div class="kpi-card"><div class="kpi-label">{lbl}</div><div class="kpi-value">{val}</div></div>', unsafe_allow_html=True)

            st.markdown('<p class="section-title" style="margin-top:1.5rem;">Per-Client Breakdown</p>', unsafe_allow_html=True)
            if not all_users.empty:
                anal_df = anal_df.merge(all_users[['pixel_id','client_name']], on='pixel_id', how='left')
            anal_df['conv_pct']      = anal_df['conv_pct'].map(lambda x: f'{x:.2f}%')
            anal_df['rev_per_visit'] = anal_df['rev_per_visit'].map(lambda x: f'${x:,.2f}')
            anal_df['revenue']       = anal_df['revenue'].map(lambda x: f'${x:,.0f}')
            disp_a = anal_df[['client_name','pixel_id','visitors','purchases','revenue','conv_pct','rev_per_visit']].rename(columns={
                'client_name':'Client','pixel_id':'Pixel ID','visitors':'Visitors',
                'purchases':'Purchases','revenue':'Revenue','conv_pct':'Conv %','rev_per_visit':'Rev/Visitor'
            })
            st.dataframe(disp_a, use_container_width=True, hide_index=True)
        else:
            st.info("No analytics data yet.")

# ================ 9. DASHBOARD PAGE =================
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
        _primary_px = str(pixel_id).split(',')[0].strip()
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

        # ── RANK BY (adapts to active tab) ──
        with st.expander("Rank By", expanded=False):
            _cur_tab = st.session_state.get('main_tab_selector', 'Customer Insights')
            if _cur_tab == 'Customer Insights':
                cust_rank_opts = ["% of Purchasers", "AOV", "Revenue", "Purchases"]
                for m in cust_rank_opts:
                    if st.button(m, key=f"cust_metric_{m}",
                                 type="primary" if st.session_state.cust_metric == m else "secondary"):
                        st.session_state.cust_metric = m
                        st.rerun()
            else:
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
    # TAB SELECTOR (top right) + TITLE
    # =====================================================
    client_name = st.session_state.get('client_name') or st.session_state.get('username', '')

    _, tab_r = st.columns([5, 3])
    with tab_r:
        active_tab = st.radio(
            "View",
            ["Customer Insights", "Conversion Insights"],
            horizontal=True,
            label_visibility="collapsed",
            key="main_tab_selector"
        )

    tab_title = f"{client_name}'s Customer Insights" if active_tab == 'Customer Insights' else f"{client_name}'s Conversion Insights"
    st.markdown(
        f'<div style="text-align:center; margin-bottom: 0.8rem;">'
        f'<span class="serif-gradient-centerpiece" style="font-size: 2.6rem;">'
        f'{tab_title}</span></div>',
        unsafe_allow_html=True
    )

    # =====================================================
    # KPI CARDS (adapt to active tab)
    # =====================================================
    total_revenue   = float(df_p_filtered['Total'].sum()) if not df_p_filtered.empty and 'Total' in df_p_filtered.columns else 0.0
    total_purchases = int(df_p_filtered['Order_ID'].nunique()) if not df_p_filtered.empty and 'Order_ID' in df_p_filtered.columns else 0
    total_visitors  = int(df_demo_filtered['total_visitors'].sum()) if not df_demo_filtered.empty else 0
    conv_rate       = (total_purchases / total_visitors * 100) if total_visitors > 0 else 0.0
    rev_per_visitor = (total_revenue / total_visitors) if total_visitors > 0 else 0.0
    overall_aov     = (total_revenue / total_purchases) if total_purchases > 0 else 0.0

    k1, k2, k3, k4 = st.columns(4)
    if active_tab == 'Customer Insights':
        with k1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Revenue</div><div class="kpi-value">${total_revenue:,.0f}</div><div class="kpi-sub">{total_purchases:,} orders</div></div>', unsafe_allow_html=True)
        with k2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Purchases</div><div class="kpi-value">{total_purchases:,}</div><div class="kpi-sub">{start_date} – {end_date}</div></div>', unsafe_allow_html=True)
        with k3:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Avg Order Value</div><div class="kpi-value">${overall_aov:,.2f}</div><div class="kpi-sub">across all segments</div></div>', unsafe_allow_html=True)
        with k4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Date Range</div><div class="kpi-value" style="font-size:1rem;">{start_date}</div><div class="kpi-sub">→ {end_date}</div></div>', unsafe_allow_html=True)
    else:
        with k1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Visitors</div><div class="kpi-value">{total_visitors:,.0f}</div><div class="kpi-sub">{start_date} – {end_date}</div></div>', unsafe_allow_html=True)
        with k2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total Revenue</div><div class="kpi-value">${total_revenue:,.0f}</div><div class="kpi-sub">{total_purchases:,} orders</div></div>', unsafe_allow_html=True)
        with k3:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Conversion Rate</div><div class="kpi-value">{conv_rate:.2f}%</div><div class="kpi-sub">of visitors purchased</div></div>', unsafe_allow_html=True)
        with k4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Revenue Per Visitor</div><div class="kpi-value">${rev_per_visitor:,.2f}</div><div class="kpi-sub">avg across date range</div></div>', unsafe_allow_html=True)

    # =====================================================
    # ACTIVE FILTER PILLS
    # =====================================================
    active_rank_label = st.session_state.cust_metric if active_tab == 'Customer Insights' else metric_choice
    pills_html = '<div class="filter-pills-row">'
    pills_html += f'<span class="filter-pill">📅 {start_date} → {end_date}</span>'
    pills_html += f'<span class="filter-pill">🏅 {active_rank_label}</span>'
    pills_html += f'<span class="filter-pill">↕ {sort_label}</span>'
    if min_purchasers > 0:
        pills_html += f'<span class="filter-pill">≥ {min_purchasers} purchases</span>'
    pills_html += '</div>'
    st.markdown(pills_html, unsafe_allow_html=True)

    st.divider()

    # =====================================================
    # CUSTOMER INSIGHTS TAB
    # =====================================================
    if active_tab == 'Customer Insights':
        _CUST_NORM = {'Homeowner': 'Yes', 'Renter': 'No', 'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'}
        cust_configs = [c for c in configs if c[1] != 'state']
        cust_metric  = st.session_state.cust_metric

        st.markdown('<p class="section-title">Customer Analysis</p>', unsafe_allow_html=True)

        if 'active_cust_var' not in st.session_state:
            st.session_state.active_cust_var = cust_configs[0][0]

        cust_var_labels = [label for label, _ in cust_configs]
        cust_idx = cust_var_labels.index(st.session_state.active_cust_var) if st.session_state.active_cust_var in cust_var_labels else 0
        active_cust_var = st.radio("Customer variable", options=cust_var_labels, index=cust_idx,
                                   horizontal=True, label_visibility="collapsed", key="cust_var_radio")
        st.session_state.active_cust_var = active_cust_var
        cust_col = dict(cust_configs)[active_cust_var]

        if not df_p_filtered.empty and cust_col in df_p_filtered.columns:
            cust_df = df_p_filtered.copy()
            cust_df[cust_col] = cust_df[cust_col].replace(_CUST_NORM)
            # Display-level remap
            cust_df[cust_col] = cust_df[cust_col].replace({'Y': 'Yes', 'N': 'No', 'M': 'Male', 'F': 'Female'})

            grp = cust_df[~cust_df[cust_col].isin(EXCLUDE_LIST)].groupby(cust_col).agg(
                Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')
            ).reset_index()

            grp = grp[grp['Purchases'] >= min_purchasers]
            if not grp.empty:
                total_purch = grp['Purchases'].sum()
                grp['AOV']              = (grp['Revenue'] / grp['Purchases'].replace(0, 1)).round(2)
                grp['% of Purchasers']  = (grp['Purchases'] / total_purch * 100).round(2)
                grp = grp.sort_values(cust_metric, ascending=is_ascending)
                grp.insert(0, 'Rank', range(1, len(grp) + 1))

                display_df   = grp.rename(columns={cust_col: active_cust_var})
                display_cols = ['Rank', active_cust_var, 'Revenue', 'Purchases', '% of Purchasers', 'AOV']
                styler = display_df[display_cols].style\
                    .set_properties(**{'font-weight': '800'}, subset=[cust_metric] if cust_metric in display_cols else [])\
                    .format({'Revenue': '${:,.2f}', 'Purchases': '{:,.0f}',
                             '% of Purchasers': '{:.2f}%', 'AOV': '${:,.2f}'})
                render_premium_table(styler)
        else:
            st.info("Upload and enrich orders to see customer analysis.")

        # ── Customer Insights Time Series ──
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<p class="section-title">Customer Performance Over Time</p>', unsafe_allow_html=True)

        if 'cust_time_gran' not in st.session_state:
            st.session_state.cust_time_gran = 'Daily'
        cust_gran = st.radio("Granularity", ['Daily', 'Weekly', 'Monthly'],
                             index=['Daily','Weekly','Monthly'].index(st.session_state.cust_time_gran),
                             horizontal=True, key='cust_time_gran_radio', label_visibility='collapsed')
        st.session_state.cust_time_gran = cust_gran

        st.markdown(
            f'<p style="font-family:Outfit,sans-serif;font-size:1.35rem;font-weight:600;'
            f'color:{PITCH_BRAND_COLOR};text-align:center;margin:8px 0 4px 0;'
            f'text-transform:uppercase;letter-spacing:0.08em;font-size:1.35rem!important;">'
            f'{active_cust_var} &nbsp;·&nbsp; {cust_metric}</p>',
            unsafe_allow_html=True
        )

        if not df_p_filtered.empty and cust_col in df_p_filtered.columns:
            import altair as alt
            freq_map = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'MS'}
            freq = freq_map[cust_gran]
            p_ts = df_p_filtered.copy()
            p_ts['ts_date'] = pd.to_datetime(p_ts['order_date']).dt.tz_convert(None).dt.normalize() if pd.to_datetime(p_ts['order_date']).dt.tz is not None else pd.to_datetime(p_ts['order_date']).dt.normalize()
            p_ts[cust_col]  = p_ts[cust_col].replace(_CUST_NORM)
            p_ts = p_ts[~p_ts[cust_col].isin(EXCLUDE_LIST)]
            ct_agg = p_ts.groupby([pd.Grouper(key='ts_date', freq=freq), cust_col]).agg(
                Purchases=('Order_ID', 'nunique'), Revenue=('Total', 'sum')
            ).reset_index()
            ct_agg['AOV']             = (ct_agg['Revenue'] / ct_agg['Purchases'].replace(0,1)).round(2)
            # % of purchasers per period
            period_totals = ct_agg.groupby('ts_date')['Purchases'].transform('sum')
            ct_agg['% of Purchasers'] = (ct_agg['Purchases'] / period_totals.replace(0,1) * 100).round(2)
            ct_agg = ct_agg.sort_values('ts_date')

            cust_ts_col_map = {'% of Purchasers': '% of Purchasers', 'AOV': 'AOV', 'Revenue': 'Revenue', 'Purchases': 'Purchases'}
            cust_ts_col = cust_ts_col_map.get(cust_metric, 'AOV')

            CHART_COLORS = ['#4D148C', '#7C3AED', '#20B2AA', '#F59E0B', '#E11D48', '#059669']
            segs = sorted(ct_agg[cust_col].unique())
            active_segs = [s for s in segs if ct_agg[ct_agg[cust_col]==s][cust_ts_col].sum() > 0]

            if active_segs:
                chart_df = ct_agg[ct_agg[cust_col].isin(active_segs)].rename(columns={cust_col:'Segment', cust_ts_col:'Value'})
                chart_df['ts_date'] = pd.to_datetime(chart_df['ts_date'])
                label_expr_c = {
                    '% of Purchasers': "format(datum.value,'.1f')+'%'",
                    'AOV':             "'$'+format(datum.value,',.2f')",
                    'Revenue':         "'$'+format(datum.value,',.0f')",
                    'Purchases':       "format(datum.value,',.0f')",
                }.get(cust_metric, "format(datum.value,',.2f')")
                tt_fmt_c = {'% of Purchasers':'.1f','AOV':'$,.2f','Revenue':'$,.0f','Purchases':',.0f'}.get(cust_metric,',.2f')
                tt_title_c = cust_metric + (' (%)' if cust_metric == '% of Purchasers' else '')
                color_scale_c = alt.Scale(domain=active_segs, range=CHART_COLORS[:len(active_segs)])
                x_enc_c = alt.X('ts_date:T', axis=alt.Axis(format='%b %d', labelColor='#0F172A', tickColor='#EBE4F4', domainColor='#EBE4F4', gridOpacity=0, labelFontSize=11, labelFont='Outfit', title=None))
                y_enc_c = alt.Y('Value:Q', title='', axis=alt.Axis(labelExpr=label_expr_c, labelColor='#0F172A', gridColor='#F1F5F9', domainOpacity=0, tickOpacity=0, labelFontSize=11, labelFont='Outfit'))
                color_enc_c = alt.Color('Segment:N', scale=color_scale_c, legend=alt.Legend(orient='top', title=None, labelFontSize=12, labelFont='Outfit', labelColor='#0F172A', symbolStrokeWidth=3, symbolSize=120, padding=6))
                line_c   = alt.Chart(chart_df).mark_line(strokeWidth=2.5, interpolate='monotone').encode(x=x_enc_c, y=y_enc_c, color=color_enc_c)
                points_c = alt.Chart(chart_df).mark_circle(size=55).encode(x=x_enc_c, y=y_enc_c,
                    color=alt.Color('Segment:N', scale=color_scale_c, legend=None),
                    tooltip=[alt.Tooltip('ts_date:T', title='Date', format='%b %d, %Y'), alt.Tooltip('Segment:N', title=active_cust_var), alt.Tooltip('Value:Q', title=tt_title_c, format=tt_fmt_c)])
                st.altair_chart((line_c + points_c).properties(height=320, background='transparent'), use_container_width=True)

        return  # Don't render Conversion Insights content when on Customer Insights tab

    # =====================================================
    # CONVERSION INSIGHTS — SINGLE VARIABLE DEEP DIVE
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

    # Initialise filters for newly added variables
    for lbl in selected_labels:
        col_name = label_to_col[lbl]
        if col_name not in st.session_state.matrix_filters:
            st.session_state.matrix_filters[col_name] = []
            # Clear stale pills state so it re-initialises to all-selected on next render
            pk = f"mx_ms_{col_name}"
            if pk in st.session_state:
                del st.session_state[pk]

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

        # Escape $ in labels so Streamlit doesn't render them as LaTeX math
        def _safe(s): return str(s).replace('$', r'\$')
        safe_opts   = [_safe(o) for o in opts]
        safe_to_raw = {_safe(o): o for o in opts}   # reverse map for filtering

        # Pre-select ALL options when variable is first activated
        if pills_key not in st.session_state:
            st.session_state[pills_key] = safe_opts

        with st.container(border=True):
            st.markdown(
                f'<p style="font-family:Outfit,sans-serif;font-size:0.62rem;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.1em;color:#94A3B8;margin-bottom:6px;">'
                f'{label} — filter</p>',
                unsafe_allow_html=True
            )
            raw_sel = st.pills(
                label,
                options=safe_opts,
                selection_mode="multi",
                label_visibility="collapsed",
                key=pills_key,
            )

            # Map display labels back to original BQ values for filtering
            sel_orig = [safe_to_raw.get(v, v) for v in (raw_sel or [])]

            # All selected (or nothing) = no filter; partial = filter to those
            if not sel_orig or set(sel_orig) == set(opts):
                new_val = []
            else:
                new_val = sel_orig

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
    elif st.session_state.app_state == 'admin':
        admin_page()
    elif st.session_state.app_state == 'onboarding':
        onboarding_page()
    elif st.session_state.app_state == 'dashboard':
        dashboard_page()

if __name__ == "__main__":
    main()
