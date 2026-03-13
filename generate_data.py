import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- EXACT MESSY DATA FROM YOUR SAMPLES ---
INCOME_RANGES = [
    "$100,000 - $149,999", "$200,000 - $249,999", "Under $10,000", "$50,000 - $54,999", 
    "$250,000 +", "$30,000 - $34,999", "$25,000 - $29,999", "$75,000 - $99,999", 
    "$60,000 - $64,999", "$20,000 - $24,999", "$45,000 - $49,999", "$55,000 - $59,999", 
    "$175,000 - $199,999", "Less than $20,000", "$40,000 - $44,999", "$150,000 - $174,999", 
    "$65,000 - $74,999", "$10,000 - $14,999"
]

NET_WORTHS = [
    "$250,000 - $499,999", "$50,000 - $99,999", "$499,999 or more", "Less than $1", 
    "$100,000 - $249,999", "$25,000 - $49,999", "-$2,499 to $2,499", "$1 - $4,999", 
    "$75,000 to $99,999", "$10,000 - $24,999", "$5,000 - $9,999"
]

STATES = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
GENDERS = ['M', 'F']
MARITAL = ['Married', 'Single']
AGES = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']

# --- GENERATE UNIQUE PURCHASERS (600 profiles) ---
num_profiles = 600
base_emails = [f"shopper_{i}@example.com" for i in range(num_profiles)]

# --- 1. GENERATE SHOPIFY ORDERS (1000 orders) ---
# This ensures some of the 600 people buy multiple times
order_emails = random.choices(base_emails, k=1000)
order_ids = [f"#LN-{1000 + i}" for i in range(1000)]
totals = [round(random.uniform(49.99, 499.99), 2) for _ in range(1000)]

# Random dates over the last 90 days
base_date = datetime(2026, 1, 1)
dates = [(base_date + timedelta(days=random.randint(0, 89))).strftime('%Y-%m-%d') for _ in range(1000)]

shopify_df = pd.DataFrame({
    'Order ID': order_ids,
    'Date': dates,
    'Email': order_emails,
    'Total': totals
})

shopify_df.to_csv("shopify_orders_sample.csv", index=False)
print("✅ Created shopify_orders_sample.csv (1000 rows)")


# --- 2. GENERATE N8N ENRICHED DATA (600 rows) ---
# Create comma-separated messy emails
messy_emails = []
for email in base_emails:
    burner1 = f"{email.split('@')[0]}@yahoo.com"
    burner2 = f"{email.split('_')[1]}123@aol.com"
    # Randomly assign 1, 2, or 3 emails to the cell
    email_list = random.choice([
        f"{email}", 
        f"{email},{burner1}", 
        f"{burner2},{email},{burner1}"
    ])
    messy_emails.append(email_list)

# Build the massive n8n payload structure
n8n_df = pd.DataFrame({
    'PIXEL_ID': [f"uuid-{random.randint(1000,9999)}" for _ in range(num_profiles)],
    'EVENT_TIMESTAMP': [datetime.now().isoformat() for _ in range(num_profiles)],
    'FIRST_NAME': [f"User{i}" for i in range(num_profiles)],
    'LAST_NAME': [f"Test{i}" for i in range(num_profiles)],
    'PERSONAL_CITY': ["Cityville" for _ in range(num_profiles)],
    'PERSONAL_STATE': random.choices(STATES, k=num_profiles),
    'PERSONAL_ZIP': [str(random.randint(10000, 99999)) for _ in range(num_profiles)],
    'AGE_RANGE': random.choices(AGES, k=num_profiles),
    'GENDER': random.choices(GENDERS, k=num_profiles),
    'MARRIED': random.choices(MARITAL, k=num_profiles),
    'NET_WORTH': random.choices(NET_WORTHS, k=num_profiles),
    'INCOME_RANGE': random.choices(INCOME_RANGES, k=num_profiles),
    'PERSONAL_EMAILS': messy_emails, # The messy comma-separated string
    'JOB_TITLE': ["Manager" if random.random() > 0.5 else "Director" for _ in range(num_profiles)],
    'COMPANY_NAME': ["Acme Corp" for _ in range(num_profiles)]
})

# Simulate missing data (Null out 15% of Demographics randomly)
for col in ['GENDER', 'MARRIED', 'AGE_RANGE', 'NET_WORTH', 'INCOME_RANGE']:
    n8n_df.loc[n8n_df.sample(frac=0.15).index, col] = np.nan

n8n_df.to_csv("n8n_enriched_sample.csv", index=False)
print("✅ Created n8n_enriched_sample.csv (600 rows with messy data)")
