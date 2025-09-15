# mail.py

import os
import sys
import pandas as pd
from dotenv import load_dotenv
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from project_config import DATABASE_URL


# Load email credentials
load_dotenv()
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

if not EMAIL_USER or not EMAIL_PASSWORD:
    raise EnvironmentError("EMAIL_USER or EMAIL_PASSWORD not set in .env")


# === Reusable: Get recipients by report name ===
def get_email_recipients(report_name, csv_path=None):
    """
    Return list of emails for a given report_name.
    Args:
        report_name (str): e.g., 'HM_01_Acct_Rec'
        csv_path (str): Path to email_list.csv (optional)
    Returns:
        list: List of email addresses
    """
    if csv_path is None:
        csv_path = os.path.join(os.path.dirname(__file__), "email_list.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Email list not found: {csv_path}")

    # ? Fix: Use dtype=str to avoid mixed-type warning
    df = pd.read_csv(
        csv_path,
        dtype=str,           # ? Treat all columns as string
        na_filter=False,     # ? Faster: don't convert to NaN
        keep_default_na=False
    )

    # Check required columns
    if 'report_name' not in df.columns or 'email_number' not in df.columns:
        raise ValueError("CSV must have columns: 'report_name', 'email_number'")

    # Filter by report_name (case-insensitive strip)
    df['report_name'] = df['report_name'].str.strip()
    row = df[df['report_name'].str.upper() == report_name.upper()]

    if row.empty:
        raise ValueError(f"No email list found for: {report_name}")

    email_str = row['email_number'].iloc[0]
    if pd.isna(email_str) or not email_str.strip():
        return []

    return [e.strip() for e in email_str.split(',') if e.strip()]


# === Send Mail Function ===
def send_mail(subject, bodyText, attachment=[], recipient=None, html_body=None):
    """
    Send email to a given list of recipients.
    If recipient is None, will try to auto-detect (fallback).
    """
    if recipient is None:
        try:
            # Auto-detect from folder name
            current_folder = os.path.basename(os.path.dirname(os.path.abspath(sys.argv[0])))
            recipient = get_email_recipients(current_folder)
            print(f"?? Auto-recipients ({current_folder}): {recipient}")
        except Exception as e:
            print(f"?? Fallback: {e}")
            recipient = [EMAIL_USER]  # Fallback to sender
    else:
        print(f"?? Using provided recipients: {recipient}")

    # Build message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_USER
    msg['To'] = ", ".join(recipient)
    part1 = MIMEText(bodyText, 'plain')
    msg.attach(part1)

    if html_body:
        html = '<html><body><p>' + bodyText + '</p>'
        for df, heading in html_body:
            html += f'<h2 style="color:red;">{heading}</h2>'
            html += build_table(df, 'blue_dark')
        html += '</body></html>'
        part2 = MIMEText(html, 'html')
        msg.attach(part2)

    # Attach files
    for file_path in attachment:
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = os.path.basename(file_path)
            part.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(part)

    # Send email
    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_USER, recipient, msg.as_string())
        server.quit()
        print("? Email sent successfully!")
    except Exception as e:
        print(f"? Failed to send email: {e}")