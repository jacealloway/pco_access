
import smtplib 
import ssl 
from numpy import loadtxt
from email.message import EmailMessage
import logging 

logging.basicConfig(filename = 'LOG/email_log.txt', 
                filemode='a',
                level = logging.INFO, 
                format = '%(asctime)s - %(levelname)s - %(message)s'
                )

# Gmail SMTP server settings
smtp_server = "smtp.gmail.com"
smtp_port = 465  # For SSL

# Specify directory for email
sender_email = "jacealloway@gmail.com"
reciever_email = "jacealloway@gmail.com"
app_password = loadtxt(f"apikey/keys.txt", dtype = str)[3]

# Format the app password, since it must be stored as 1 column we need to break it up with a delimiter
x = app_password.split('_')
apw = ''
for i in x:
    apw = apw + i + ' '


subject = "PCO_ETL.py has completed running. See attached log for details."

with open('LOG/PCO_ETL_log.txt', 'r') as f:
    file = f.read()

msg = EmailMessage() 

msg['Subject'] = subject
msg['From'] = sender_email
msg['To'] = reciever_email
msg.add_attachment(file, filename = 'PCO_ETL_log.txt')




# Create a secure SSL context
context = ssl.create_default_context()

try:
    # Connect to the server and send the email
    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
        server.login(sender_email, apw)
        server.send_message(msg)
        logging.info('Email sent successfully.')
except Exception as e:
    logging.error(f"Error sending email: {e}")
