# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import calendar
import sys
import os

# Add root directory to path
sys.path.append(r"D:\\")
from mail import send_mail

# --- SETTINGS ---
ZID_MAP = {
    100001: "Gulshan",
    100002: "Central",
    100003: "Ecommerce",
}

NOW = datetime.now()
CURRENT_YEAR = NOW.year
CURRENT_MONTH = NOW.month
OUTPUT_FILE = f"H_82_Inventory_Value_{CURRENT_YEAR}.xlsx"

engine = create_engine("postgresql://postgres:postgres@localhost:5432/da")

# --- HELPER: Get last date of month ---
def last_day_of_month(year, month):
    return calendar.monthrange(year, month)[1]

# --- LOGIC: Define Time Window ---
target_months = []

if CURRENT_MONTH <= 6:
    # Logic: Get last 6 months (rolling window)
    for i in range(5, -1, -1):
        m = CURRENT_MONTH - i
        y = CURRENT_YEAR
        while m <= 0:
            m += 12
            y -= 1
        target_months.append((y, m))
else:
    # Logic: From January of current year to current month
    for m in range(1, CURRENT_MONTH + 1):
        target_months.append((CURRENT_YEAR, m))

# --- MAIN LOGIC: Generate Report ---
print("Generating inventory report...")

with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
    for zid, business_name in ZID_MAP.items():
        print(f"Processing: {business_name}")
        monthly_data = pd.DataFrame()

        for y, m in target_months:
            last_day = last_day_of_month(y, m)
            query_date = f"{y}-{m:02d}-{last_day}"
            month_col_name = f"{calendar.month_name[m]}-{str(y)[-2:]}"

            query = f"""
                SELECT xwh, COALESCE(SUM(xval * xsign), 0) AS value
                FROM imtrn
                WHERE zid = {zid} AND xdate <= '{query_date}'
                GROUP BY xwh
                ORDER BY xwh
            """

            try:
                df_month = pd.read_sql(query, engine)
                df_month.rename(columns={"value": month_col_name}, inplace=True)

                if monthly_data.empty:
                    monthly_data = df_month
                else:
                    monthly_data = pd.merge(monthly_data, df_month, on="xwh", how="outer")
            except Exception as e:
                print(f"Error fetching data for {month_col_name}: {e}")

        if not monthly_data.empty:
            monthly_data.fillna(0, inplace=True)
            monthly_data.to_excel(writer, sheet_name=business_name[:31], index=False)

print(f"Inventory report saved to {OUTPUT_FILE}")

# --- EMAIL PREPARATION ---
start_label = f"{calendar.month_name[target_months[0][1]]} {target_months[0][0]}"
end_label = f"{calendar.month_name[target_months[-1][1]]} {target_months[-1][0]}"
reporting_mode = "Rolling 6 Months" if CURRENT_MONTH <= 6 else "Year-to-Date"

subject = f"H_82 Fixit Inventory Value - {end_label}"

# Create plain text business list
business_list_text = "\n".join([f"- {name} (ZID: {zid})" for zid, name in ZID_MAP.items()])

body_text = f"""Dear Sir,

Please find the attached Inventory Value report (H_82).

Reporting Mode: {reporting_mode}
Period: {start_label} to {end_label}

Businesses Included:
{business_list_text}

Best regards,
Automated Reporting System"""

mail_to = ['ithmbrbd@gmail.com', 'motiurhmbr@gmail.com']

try:
    send_mail(subject, body_text, [OUTPUT_FILE], mail_to, [])
    print("Report emailed successfully.")
except Exception as e:
    print(f"Failed to send email: {e}")