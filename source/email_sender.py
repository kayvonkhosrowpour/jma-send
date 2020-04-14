import pandas as pd
import numpy as np
import logging
import time

# TODO: read dataframes based on CMD line, and then send emails out according to timing logic defined in config.json

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
