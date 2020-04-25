import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from shared import common


FROM = 'USERNAME'
PW = 'PASSWORD'


def email(to, subject, html_body):
    # create email
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html'))

    # send the message via our SMTP server
    smtp_server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    smtp_server.login(FROM, PW)

    smtp_server.sendmail(FROM, to, msg.as_string())
    smtp_server.quit()


def send_email(schedule_name, recipient, subject_title, html_body, scheduled_time, class_time, grace_time, logs_dirpath):
    # configure logging
    logging = common.setup_logging(__file__, logs_dirpath)

    # log info
    key = '::::'.join([str(schedule_name), str(recipient), str(subject_title),
                       str(scheduled_time), str(class_time), str(grace_time)])
    logging.info(key)

    try:
        email([recipient], subject_title, html_body)
    except:
        logging.error('Could not deliver {}'.format(key))
        # TODO: reschedule?


if __name__ == '__main__':
    pass
