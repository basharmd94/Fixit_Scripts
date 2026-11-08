from pretty_html_table import build_table
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib

def send_mail(subject, bodyText, attachment=[], recipient = [], html_body=None):
    me = "pythonhmbr12@gmail.com"
    you = recipient
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ", ".join(you)
    text = bodyText

    # If html_body is provided, create an HTML part
    if html_body:
        html = '<html><body>'
        html += '<p>' + text + '</p>'
        for df, heading in html_body:
            # Add the dataframe with its heading to the email body
            html += '<h2>' + heading + '</h2>'
            html += build_table(df, 'blue_dark')
        html += '</body></html>'
        part1 = MIMEText(html, 'html')
    else:
        part1 = MIMEText(text, 'plain')

    msg.attach(part1)

    # Attachments
    if attachment:
        for file_path in attachment:
            part2 = MIMEBase('application', "octet-stream")
            part2.set_payload(open(file_path, "rb").read())
            encoders.encode_base64(part2)
            part2.add_header('Content-Disposition', 'attachment', filename=file_path)
            msg.attach(part2)

    username = 'pythonhmbr12@gmail.com'
    password = 'jkhqqkgajfphbpoq'

    s = smtplib.SMTP('smtp.gmail.com:587')
    s.starttls()
    s.login(username, password)
    s.sendmail(me, you, msg.as_string())
    s.quit()


#  call the function

# subject = "SALES & PURCHASE CASH ANALYSIS PLANNING"
# body_text = "Please find the attachment.\n"
# excel_files = ['net_sales_analysis.xlsx', 'purchase_stock.xlsx'] #optional if any
# mail_to = ['ithmbrbd@gmail.com', 'mat197194@gmail.com']
# html_df_list = [(df_sale_for_email_body, 'SALES PART '), (grouped_df, 'PURCHASE PART')] #optional if any

# send_mail(subject, body_text, excel_files, mail_to,html_df_list )