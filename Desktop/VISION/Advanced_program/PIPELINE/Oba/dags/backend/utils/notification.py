import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoder


class EmailSender():
    def __init__(destinators, subject, content, files):
        self.username = "admqv"
        self.password = "@Ql1kV13w@"
        self.mail_from = "admqv.oci@orange.com"
        self.mail_to = destinators
        self.subject = subject
        self.content = content
        self.files = files

    def send():
        mimemsg = MIMEMultipart('alternative')
        mimemsg['From'] = self.mail_from
        mimemsg['To'] = self.mail_to
        mimemsg['Subject'] = self.subject
        html = self.content
        part1 = MIMEText(html, 'html')
        mimemsg.attach(part1)
        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename={}'.format(Path(path).name))
            mimemsg.attach(part)

        connection = smtplib.SMTP(host='192.168.4.162', port=25)
        connection.starttls()
        connection.login(username,password)
        connection.send_message(mimemsg)
        connection.quit()

