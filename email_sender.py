import pandas as pd
import os
import util
import logging
import time

# constants
CONFIG_FILEPATH = './config.json'
LOGFILE = './logs/jma_send.log'
TRANS_FILE = './logs/transaction_log.csv'

logging.basicConfig(filename=LOGFILE,
                    level=logging.DEBUG)

# load config
config = util.read_config(CONFIG_FILEPATH)
customers = util.read_frame(config['customers_path'])
schedule = util.read_frame(config['schedule_path'])
BATCH_SIZE = int(config['batch_size'])
BATCH_WAIT_TIME = int(config['batch_wait_time_sec'])

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

    # validate fields
    valid_sch_name = sch_name is not None and len(sch_name) > 0
    valid_recipients = recipients is not None and len(recipients) > 0
    valid_templ = templ_path is not None and len(templ_path) > 0 \
        and os.path.exists(os.path.normpath(templ_path))

    if valid_templ:
        valid_html, errors = util.is_valid_html(templ_path)

    if valid_sch_name and valid_recipients and valid_templ:
        email_groups.append(email_group)


# create dataframe to prepare sending of emails
email_log_df = pd.DataFrame(columns=['Recipient', 'EmailGroup', 'TemplatePath',
                                     'DateTimeSent', 'ConfirmedSent'])

for eg in email_groups:

    # get all recipients for this email group (CASE-SENSITIVE)
    lower_recips = [recip.lower() for recip in eg['kicksite_recipients']]
    recipients = customers[customers.Program.str.lower().isin(lower_recips)]

    # create new dataframe for this group and concat
    eg_df = pd.DataFrame(data={'Recipient': recipients.Email})
    eg_df['EmailGroup'] = eg['schedule_name']
    eg_df['TemplatePath'] = eg['template_path']
    eg_df['ConfirmedSent'] = False
    email_log_df = pd.concat((email_log_df, eg_df))

# dataframe saved for sending of emails
util.save_append_dataframe(TRANS_FILE, email_log_df)

# process each email group
for template in email_log_df.TemplatePath.unique():

    # get rows for just this email template
    email_df = email_log_df[email_log_df.TemplatePath == template].reset_index()
    assert len(email_df.EmailGroup.unique()) == 1

    print('Sending', len(email_df.index), 'emails for', email_df.EmailGroup[0])

    idx = 0

    while idx < len(email_df.index):

        # get next portion of emails to send
        max_idx = min(idx + BATCH_SIZE, len(email_df.index))
        print('- [{}:{}] of 0 to {}'.format(idx, max_idx, len(email_df.index)-1))
        batch_df = email_df.iloc[idx:max_idx]

        # make sure we haven't already sent this today... --- TODO...

        # confirm we're sending 30 min before start time... --- TODO...

        # TODO: send emails

        # confirm logs -- TODO: more complicated...
        email_df['ConfirmedSent'] = True
        # util.save_append_dataframe(TRANS_FILE, email_log_df) # TODO

        # sleep
        print('  - Sleeping for', BATCH_WAIT_TIME, 'sec')
        time.sleep(BATCH_WAIT_TIME)

        # move to next batch
        idx = max_idx

