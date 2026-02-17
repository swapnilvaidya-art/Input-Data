import os
import time
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV VARIABLES --------------------
sec = os.getenv("PRABHAT_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")
QUERY_URL = os.getenv("DAILY_INPUT_QUERY")
SAK = os.getenv("SHEET_ACCESS_KEY")

TARGET_SHEET = "Helper Call Dump"

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)

creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

# -------------------- METABASE LOGIN --------------------
print("🔐 Creating Metabase session...")

res = requests.post(
    MB_URL,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec},
    timeout=60
)

res.raise_for_status()
token = res.json()['id']

METABASE_HEADERS = {
    'Content-Type': 'application/json',
    'X-Metabase-Session': token
}

print("✅ Metabase session created")

# -------------------- FETCH WITH RETRY --------------------
def fetch_with_retry(url, headers, retries=5):
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=180)
            response.raise_for_status()
            return response

        except Exception as e:
            wait_time = 10 * attempt
            print(f"[Metabase] Attempt {attempt} failed: {e}")

            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- SAFE SHEET UPDATE --------------------
def safe_update_sheet(worksheet, df, retries=5):

    values = [df.columns.tolist()] + df.values.tolist()

    for attempt in range(1, retries + 1):
        try:
            print(f"🔄 Clearing sheet: {worksheet.title}")
            worksheet.clear()

            print(f"⬆️ Writing {len(df)} rows to sheet...")
            worksheet.update(
                values,
                value_input_option="RAW"
            )

            print(f"✅ Sheet updated successfully")
            return True

        except Exception as e:
            wait_time = 20 * attempt
            print(f"[Sheets] Attempt {attempt} failed: {e}")

            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- MAIN EXECUTION --------------------
print("📥 Fetching Input query from Metabase...")

response = fetch_with_retry(QUERY_URL, METABASE_HEADERS)
df_Input = pd.DataFrame(response.json())

if df_Input.empty:
    print("⚠️ WARNING: Input query returned empty dataset.")

required_cols = [
    'lead_created_on', 'modified_on', 'prospect_email', 'prospect_stage',
    'mx_prospect_status', 'crm_user_role', 'sales_user_email',
    'mx_utm_medium', 'mx_utm_source', 'mx_lead_quality_grade',
    'mx_lead_inherent_intent', 'mx_priority_status',
    'mx_organic_inbound', 'lead_last_call_status',
    'mx_city', 'event', 'current_stage', 'previous_stage',
    'mx_identifer', 'mx_phoenix_identifer',
    'call_type', 'duration'
]

missing_cols = [col for col in required_cols if col not in df_Input.columns]
if missing_cols:
    raise ValueError(f"❌ Missing columns from query: {missing_cols}")

df_Input = df_Input[required_cols]

# -------------------- HARD SANITIZE FOR GOOGLE SHEETS --------------------

import numpy as np

# Replace inf values
df_Input.replace([np.inf, -np.inf], None, inplace=True)

# Replace NaN with None
df_Input = df_Input.astype(object).where(pd.notnull(df_Input), None)

# Convert numpy types to pure python types (pandas 2.x safe)
df_Input = df_Input.map(lambda x: x.item() if hasattr(x, "item") else x)



print("📊 Rows fetched:", len(df_Input))

print("🔗 Connecting to Google Sheets...")
sheet = gc.open_by_key(SAK)
ws_input = sheet.worksheet(TARGET_SHEET)

print("⬆️ Updating Helper Call Dump...")
safe_update_sheet(ws_input, df_Input)

# -------------------- TIMER SUMMARY --------------------
end_time = time.time()
elapsed = end_time - start_time
mins, secs = divmod(elapsed, 60)

print(f"⏱ Total execution time: {int(mins)}m {int(secs)}s")
print("🎯 Input Data Automation Completed Successfully!")
