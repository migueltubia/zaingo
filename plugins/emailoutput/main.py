from plugins import OutputPlugin as op
import logging
import ssl
from smtplib import SMTP, SMTP_SSL
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import importlib

class EmailOutput(op.OutputPlugin):
    
    CONFIG_MAIL_FROM="mail_from"
    CONFIG_MAIL_FROM_LONG="mail_from_long"
    CONFIG_MAIL_TO="mail_to"
    CONFIG_SERVER="server"
    CONFIG_PORT="port"
    CONFIG_USER="user"
    CONFIG_PASSWORD="password"
    CONFIG_SUBJECT="subject"
    CONFIG_BODY="body"
    CONFIG_SSL="ssl"

    mail_from=""
    mail_from_long=""
    mail_to=""
    server=""
    port=0
    user=""
    password=""
    subject=""
    body=""
    ssl=False

    def __init__(self):
        super(EmailOutput, self).__init__()
        self.logger = logging.getLogger('EmailOutput')
        self.name="EmailOutput"
        self.description="Sent email when alerting"
            
    def create_configuration(self):
        configuration={}
        configuration[self.CONFIG_MAIL_FROM]=self.mail_from
        configuration[self.CONFIG_MAIL_FROM_LONG]=self.mail_from_long
        configuration[self.CONFIG_MAIL_TO]=self.mail_to
        configuration[self.CONFIG_SERVER]=self.server
        configuration[self.CONFIG_PORT]=self.port
        configuration[self.CONFIG_USER]=self.user
        configuration[self.CONFIG_PASSWORD]=self.password
        configuration[self.CONFIG_SUBJECT]=self.subject
        configuration[self.CONFIG_BODY]=self.body
        configuration[self.CONFIG_SSL]=self.ssl
        return configuration

    def create_message(self, body):
        msg=""

        msg=MIMEMultipart('alternative')
        text=MIMEText(body+"\r\n", 'html')
        msg.attach(text)

        msg['From'] = "Notificaciones zaingo <"+self.mail_from+">"
        msg['To'] = self.mail_to
        msg['Subject'] = self.subject

        return msg

    def execute(self, alert):        
        body=self.generate_body(alert)
        msg=""
        try:
            msg=self.create_message(body)
        except Exception as error:
            self.logger.error("Error generating mail message: %s", error)
            msg="Error generating message"

        s=None
        try:
            if self.ssl==False:
                s=SMTP(host=self.server, port=self.port)
            else:
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                s=SMTP_SSL(host=self.server, port=self.port, context=context)
                s.ehlo()
            if self.user!="" and self.password!="":
                s.login(self.user, self.password)
        except Exception as error:
            self.logger.error("Error connecting to server %s:%s, error %s", self.server, self.port, error)

        if s==None:
            return

        self.logger.debug("Sending email %s", self.subject)

        try:
            s.sendmail(self.mail_from, [self.mail_to], msg.as_string())
        except Exception as error:
            self.logger.error("Error sending mail message: %s", error)
        s.quit()

    def generate_body(self, alert):
        modulo=importlib.import_module("plugins.emailoutput.email."+self.body)
        klass= getattr(modulo, self.body)
        instance=klass()
        body=""
        try:
            body=instance.generateMessage(alert)
        except Exception as error:
            self.logger.error("Error generating email body: %s", error)
            body="Error generating email body "+self.body+" on alert "+str(alert)
        self.logger.debug("Output body: %s", body)
        return body

class BodyBase(object):
    def generateMessage(self, alert):
        pass