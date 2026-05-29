"""Email notification service (Gmail SMTP)."""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from iot_backend.config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD


def send_email_alert(to_email: str, subject: str, html_body: str) -> tuple[bool, str]:
    """Send HTML email using configured Gmail SMTP credentials."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        return False, "GMAIL_ADDRESS or GMAIL_APP_PASSWORD is not configured"
    if not to_email:
        return False, "recipient email is required"

    msg = MIMEMultipart("alternative")
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, [to_email], msg.as_string())
        return True, "sent"
    except Exception as exc:
        return False, str(exc)

