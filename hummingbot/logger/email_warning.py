import logging
import os
import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from dotenv import load_dotenv

load_dotenv(override=True)

logger = logging.getLogger(__name__)


def load_email_list(file_path: str = "emails.txt") -> List[str]:
    """Load email addresses from a file, one per line."""
    emails = []
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                emails = [line.strip() for line in f if line.strip() and '@' in line]
            logger.info(f"Loaded {len(emails)} email addresses from {file_path}")
        else:
            logger.warning(f"Email list file {file_path} not found")
    except Exception as e:
        logger.error(f"Failed to load email list from {file_path}: {str(e)}")
    return emails


def send_email(
    subject: str,
    body: str,
    email_list_file: str = "emails.txt"
) -> bool:
    """Send deployment notification email to a list of recipients."""

    # Load configuration from environment variables
    smtp_server = os.getenv("SMTP_SERVER", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("FROM_EMAIL", smtp_username)

    if not smtp_username or not smtp_password or not from_email:
        logger.error("Email configuration missing. Please set SMTP_USERNAME, SMTP_PASSWORD, and optionally FROM_EMAIL")
        return False

    # Load email recipients
    recipients = load_email_list(email_list_file)
    if not recipients:
        logger.warning("No email recipients found")
        return False

    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject

        # Add body to email
        msg.attach(MIMEText(body, 'plain'))

        # Setup SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Enable security
        server.login(smtp_username, smtp_password)

        # Send email
        text = msg.as_string()
        server.sendmail(from_email, recipients, text)
        server.quit()

        logger.info(f"Email sent successfully to {len(recipients)} recipients")
        return True

    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(f"Failed to send email: {str(e)}")
        return False


def send_email_critical_issue(
    subject: str,
    msg: str,
    email_list_file: str = "emails.txt"
) -> bool:
    """Send critical issue notification email to a list of recipients."""
    body = (
        "<html>"
        "<body style='font-family: Arial, sans-serif; background-color: #fff8f0; padding: 30px;'>"
        "<div style='max-width: 600px; margin: auto; border: 2px solid #ff4d4f; border-radius: 10px; background: #fff0f0; box-shadow: 0 2px 8px rgba(255,77,79,0.1);'>"
        "<h2 style='color: #ff4d4f; text-align: center; margin-top: 20px;'>🚨 Critical Issue Detected! 🚨</h2>"
        "<div style='padding: 20px; font-size: 1.1em; color: #333;'>"
        f"{msg}"
        "</div>"
        "<div style='text-align: center; margin-bottom: 20px; color: #888; font-size: 0.95em;'>"
        "Please address this issue as soon as possible."
        "</div>"
        "</div>"
        "</body>"
        "</html>"
    )
    return send_email(subject, body, email_list_file)
