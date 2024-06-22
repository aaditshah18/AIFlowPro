import smtplib
from email.mime.text import MIMEText
import logging
from airflow.models import Variable


def send_email():
    sender_email = Variable.get('EMAIL_USER')
    receiver_email = "your_email@example.com"
    password = Variable.get('EMAIL_PASSWORD')

    subject = "Sample email from Airflow"
    body = "Hello, this is a test email from Python."

    # Create the email headers and content
    email_message = MIMEText(body)
    email_message['Subject'] = subject
    email_message['From'] = sender_email
    email_message['To'] = receiver_email

    try:
        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Using Gmail's SMTP server
        server.starttls()  # Secure the connection
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, email_message.as_string())
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
    finally:
        server.quit()
