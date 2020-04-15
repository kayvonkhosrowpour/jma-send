import pandas as pd
import numpy as np
import os
import sys
import traceback
import common


def get_classes_today(config, logger):
    schedule = common.read_frame(config['schedule_path'], index_col=0)
    today = common.get_todays_weekday()

    classes_today = None

    if today not in schedule.columns:
        logger.warning('No classes found for today ({}). Only found for {}'
                       .format(today, str(schedule.columns.tolist())))
        logger.warning('0 emails will be sent.')
    else:
        classes_today = schedule[today].apply(str)
        classes_today = pd.to_datetime(classes_today, errors='coerce').dropna()

    return classes_today


def get_customers(config, logger):
    customers = None

    try:
        customers = common.read_frame(config['customers_path'])
        customers = customers[customers['Subscribed']]
        customers = customers[['Emails', 'Programs']]
        customers = common.explode_str(customers, 'Programs', ',')
        customers = common.explode_str(customers, 'Emails', ',')
        customers = customers.rename({'Emails': 'Email',
                                      'Programs': 'Program'}, axis=1)
        customers = customers[['Email', 'Program']]
    except Exception:
        logger.error('Could not process customers with exception: {}'.format(traceback.format_exc()))

    return customers


def get_valid_email_groups(config, logger):

    email_groups = []

    for email_group in config['email_groups']:

        # get fields
        sch_name = email_group['schedule_name']
        recipients = email_group['kicksite_recipients']
        body_path = email_group['body_path']
        subject_title = email_group['subject_title']

        # validate fields
        valid_schedule_name = sch_name is not None and len(sch_name) > 0
        valid_recipients = recipients is not None and len(recipients) > 0

        valid_html_body = body_path is not None
        valid_html_body = valid_html_body and len(body_path) > 0
        valid_html_body = valid_html_body and os.path.exists(os.path.normpath(body_path))

        valid_sub = subject_title is not None and len(subject_title) > 0

        valid_html = False
        if valid_html_body:
            valid_html, errors = common.is_valid_html(body_path)
            if not valid_html:
                logger.error('Invalid HTML at {} with errors: {}'
                             .format(body_path, errors))

        if valid_schedule_name and valid_recipients and valid_html_body and valid_html and valid_sub:
            email_groups.append(email_group)

    return email_groups


def schedule_subset_time(schedule_subset_df, start_datetime, batch_size, batch_wait_time_sec):
    scheduled_df = pd.DataFrame()

    current_datetime = start_datetime

    for email_group in schedule_subset_df.EmailGroup.unique():

        schedule_by_email_group = schedule_subset_df[schedule_subset_df.EmailGroup == email_group].copy()

        idx = 0

        while idx < schedule_by_email_group.shape[0]:

            # get current batch
            start_idx = idx
            end_idx = min(start_idx + batch_size, schedule_by_email_group.shape[0])

            # schedule current batch
            logging.info('Setting {} [{}:{}] to {}'.format(email_group, start_idx, end_idx, current_datetime))
            schedule_by_email_group.iloc[start_idx:end_idx].ScheduledTime = current_datetime

            # move to next batch and datetime
            idx += batch_size
            current_datetime += batch_wait_time_sec

        scheduled_df = pd.concat((scheduled_df, schedule_by_email_group))

    return scheduled_df


def compute_email_schedule(config, email_groups, classes_today, logger):
    # create df and define types
    scheduled_df = pd.DataFrame(columns=['Recipient', 'EmailGroup', 'SubjectTitle',
                                         'BodyPath', 'ScheduledTime', 'DatetimeSent'])
    scheduled_df.Recipient = scheduled_df.Recipient.astype(str)
    scheduled_df.EmailGroup = scheduled_df.EmailGroup.astype(str)
    scheduled_df.SubjectTitle = scheduled_df.SubjectTitle.astype(str)
    scheduled_df.BodyPath = scheduled_df.BodyPath.astype(str)
    scheduled_df.ScheduledTime = scheduled_df.ScheduledTime.astype(np.datetime64)
    scheduled_df.DatetimeSent = scheduled_df.DatetimeSent.astype(np.datetime64)

    # create df for each recipient in an email group
    for email_group in email_groups:

        # get all recipients for this email group (CASE-SENSITIVE)
        lower_recipients = [recip.lower() for recip in email_group['kicksite_recipients']]
        recipients = customers[customers.Program.str.lower().isin(lower_recipients)]

        # create new df for this group and concat
        eg_df = pd.DataFrame(data={'Recipient': recipients.Email,
                                   'EmailGroup': [email_group['schedule_name']] * recipients.shape[0],
                                   'SubjectTitle': [email_group['subject_title']] * recipients.shape[0],
                                   'BodyPath': [email_group['body_path']] * recipients.shape[0]})

        scheduled_df = pd.concat((scheduled_df, eg_df))

    scheduled_df.reset_index(inplace=True, drop=True)

    logger.info('Prepared to send {} emails out today'.format(scheduled_df.shape[0]))

    # split into morning_and_noon and afternoon by class time
    today_at_noon = np.datetime64(str(np.datetime64('today')) + 'T12:00')

    morning_and_noon_classes = classes_today[classes_today <= today_at_noon].index.tolist()
    afternoon_classes = classes_today[classes_today > today_at_noon].index.tolist()

    morning_and_noon_scheduled_df = scheduled_df[scheduled_df.EmailGroup.isin(morning_and_noon_classes)]
    afternoon_scheduled_df = scheduled_df[scheduled_df.EmailGroup.isin(afternoon_classes)]

    # get scheduling configuration
    batch_size = config['batch_size']
    batch_wait_time_sec = config['batch_wait_time_sec']

    # schedule the emails
    scheduled_df = pd.concat((schedule_subset_time(morning_and_noon_scheduled_df,
                                                   config['start_send_time_map']['morning_and_noon'],
                                                   batch_size,
                                                   batch_wait_time_sec),
                              schedule_subset_time(afternoon_scheduled_df,
                                                   config['start_send_time_map']['afternoon'],
                                                   batch_size,
                                                   batch_wait_time_sec)))

    return scheduled_df


if __name__ == '__main__':
    # configure logging
    logging = common.setup_logging(__file__)

    # load config
    config = common.read_config(logging)

    # process schedule and customer files
    classes_today = get_classes_today(config, logging)
    customers = get_customers(config, logging)

    if classes_today is None or customers is None:
        logging.warning('Exiting scheduler: no emails will be scheduled today.')
        sys.exit(1)

    # get valid email groups
    email_groups = get_valid_email_groups(config, logging)

    if not email_groups:
        logging.error('No valid email groups were found!')
        sys.exit(1)

    # schedule the emails
    scheduled_df = compute_email_schedule(config, email_groups, classes_today, logging)
    print(scheduled_df)