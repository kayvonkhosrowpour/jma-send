import pandas as pd
import numpy as np
import os
import util
import logging
import time
import sys
from constants import (CONFIG_FILEPATH, LOGFILE, TRANSACTION_FILEPATH)

# setup logging
logFormatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(os.path.normpath(LOGFILE))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

logging.info('email_sender.py launched at ' + str(pd.Timestamp.today()))

# load config
config = util.read_config(CONFIG_FILEPATH)
customers = util.read_frame(config['customers_path'])
schedule = util.read_frame(config['schedule_path'], index_col=0)
BATCH_SIZE = int(config['batch_size'])
BATCH_WAIT_TIME = int(config['batch_wait_time_sec'])

# process schedule
today = util.get_todays_weekday()

if today not in schedule.columns:
    logging.warning('No classes found for today ({}). Only found for {}'
                    .format(today, str(schedule.columns.tolist())))
    logging.warning('0 emails will be sent.')
    sys.exit(0)

todays_classes = schedule[today].apply(str)
todays_classes = pd.to_datetime(todays_classes, errors='coerce').dropna()

# process customers
#customers = customers[customers['Subscribed']]
customers = customers[['Emails', 'Programs']]
customers = util.explode_str(customers, 'Programs', ',')
customers = util.explode_str(customers, 'Emails', ',')
customers = customers.rename({'Emails': 'Email',
                              'Programs': 'Program'}, axis=1)
customers = customers[['Email', 'Program']]

# validate and initialize to send
email_groups = []

for email_group in config['email_groups']:

    # get fields
    sch_name = email_group['schedule_name']
    recipients = email_group['kicksite_recipients']
    templ_path = email_group['template_path']
    subject_title = email_group['subject_title']

    # validate fields
    valid_sch_name = sch_name is not None and len(sch_name) > 0
    valid_recipients = recipients is not None and len(recipients) > 0
    valid_templ = templ_path is not None and len(templ_path) > 0 \
        and os.path.exists(os.path.normpath(templ_path))
    valid_sub = subject_title is not None and len(subject_title) > 0

    valid_html = False
    if valid_templ:
        valid_html, errors = util.is_valid_html(templ_path)
        if not valid_html:
            logging.error('Invalid HTML at {} with errors: {}'
                          .format(templ_path, errors))

    if (valid_sch_name and valid_recipients and valid_templ and valid_html and valid_sub):
        email_groups.append(email_group)

if not email_groups:
    logging.error('No valid email groups were found!')
    sys.exit(1)


# create dataframe to prepare sending of emails
email_log_df = pd.DataFrame(columns=['Recipient', 'EmailGroup', 'SubjectTitle',
                                     'TemplatePath', 'DateTimeSent',
                                     'ConfirmedSent'])

for eg in email_groups:

    # get all recipients for this email group (CASE-SENSITIVE)
    lower_recips = [recip.lower() for recip in eg['kicksite_recipients']]
    recipients = customers[customers.Program.str.lower().isin(lower_recips)]

    # get schedule info
    today = pd.Timestamp.today()

    # create new dataframe for this group and concat
    eg_df = pd.DataFrame(data={'Recipient': recipients.Email})
    eg_df['EmailGroup'] = eg['schedule_name']
    eg_df['SubjectTitle'] = eg['subject_title']
    eg_df['TemplatePath'] = eg['template_path']
    eg_df['ConfirmedSent'] = False
    email_log_df = pd.concat((email_log_df, eg_df))

# dataframe saved for sending of emails
util.save_append_dataframe(TRANSACTION_FILEPATH, email_log_df)

# process each email group
for template in email_log_df.TemplatePath.unique():

    # get rows for just this email template
    email_df = email_log_df[email_log_df.TemplatePath == template].reset_index()
    assert len(email_df.EmailGroup.unique()) == 1
    email_group = email_df.EmailGroup.unique()[0]

    logging.info('Sending {} emails for {}'.format(len(email_df.index),
                 email_df.EmailGroup[0]))

    idx = 0

    while idx < len(email_df.index):

        # get next portion of emails to send
        max_idx = min(idx + BATCH_SIZE, len(email_df.index))
        logging.info('- [{}:{}]'.format(idx, max_idx))
        batch_df = email_df.iloc[idx:max_idx]

        # make sure we haven't already sent this today... --- TODO...

        # confirm we're sending at least 30 min before start time
        now = pd.to_datetime('today')
        class_time = todays_classes[email_group]
        if np.timedelta64(class_time - now, 'm') >= np.timedelta64(30, 'm'):

            # TODO: send emails
            logging.info('  - Sent emails!')

            # confirm logs -- TODO: more complicated...
            email_df['ConfirmedSent'] = True
            # util.save_append_dataframe(TRANSACTION_FILEPATH, email_log_df) # TODO

            # sleep
            logging.info('  - Sleeping for {} sec...'.format(BATCH_WAIT_TIME))
            time.sleep(BATCH_WAIT_TIME)

        # move to next batch
        idx = max_idx
