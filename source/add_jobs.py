import pandas as pd
import numpy as np
import sys
import traceback
import pytz
from shared import common
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore


def get_classes_today(config, logger):
    schedule = common.read_df(config['schedule_path'], index_col=0)
    lookup = {i: weekday for i, weekday in enumerate(common.DAYS)}
    today = pd.Timestamp.today().weekday()
    # today = 5  # DEBUG: HARDCODE SATURDAY
    if today == 6:
        logger.info('No classes on Sunday')
        return None

    today = lookup[today]

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
        customers = common.read_df(config['customers_path'])
        customers = customers[customers['Subscribed']]
        customers = customers[['Emails', 'Programs']]
        customers = common.explode_str(customers, 'Programs', ',')
        customers = common.explode_str(customers, 'Emails', ',')
        customers = customers.rename({'Emails': 'Email',
                                      'Programs': 'Program'}, axis=1)
        customers = customers[['Email', 'Program']]
        customers.Email = customers.Email.str.lower()
        customers.drop_duplicates(subset=['Email', 'Program'], inplace=True)
    except Exception:
        logger.error('Could not process customers with exception: {}'
                     .format(traceback.format_exc()))

    return customers


def schedule_subset_time(subset_df, start_datetime, batch_size, wait_time, logger):
    scheduled_emails_df = pd.DataFrame()
    current_datetime = start_datetime

    # process each email group
    for email_group in subset_df.EmailGroup.unique():

        schedule_by_email_group = subset_df[subset_df.EmailGroup == email_group].copy()

        logger.info("email_group '{}' has {} recipients".format(email_group,
                                                                schedule_by_email_group.shape[0]))

        idx = 0

        # process each batch within an email group
        while idx < schedule_by_email_group.shape[0]:

            # get current batch
            start = idx
            end = min(start + batch_size, schedule_by_email_group.shape[0])
            size = end - start

            # schedule current batch
            logging.info('Scheduling {} [{}:{}/{}] to {}'
                         .format(email_group, start, end, schedule_by_email_group.shape[0], current_datetime))

            my_idx = schedule_by_email_group.iloc[start:end].index
            schedule_by_email_group.loc[my_idx, 'ScheduledTime'] = [current_datetime] * size

            # move to next batch and datetime
            idx += batch_size
            current_datetime += wait_time

        scheduled_emails_df = pd.concat((scheduled_emails_df, schedule_by_email_group))

    return scheduled_emails_df


def compute_email_schedule(config, classes_today, logger):
    logger.info('Computing email schedule')

    # create df and define types
    schedule_df = pd.DataFrame(columns=['Recipient', 'EmailGroup', 'SubjectTitle',
                                        'BodyPath', 'ScheduledTime', 'DatetimeSent'])
    schedule_df.Recipient = schedule_df.Recipient.astype(str)
    schedule_df.EmailGroup = schedule_df.EmailGroup.astype(str)
    schedule_df.SubjectTitle = schedule_df.SubjectTitle.astype(str)
    schedule_df.BodyPath = schedule_df.BodyPath.astype(str)
    schedule_df.ScheduledTime = schedule_df.ScheduledTime.astype(np.datetime64)
    schedule_df.DatetimeSent = schedule_df.DatetimeSent.astype(np.datetime64)

    # create df for each recipient in an email group
    for email_group in config['email_groups']:

        # get all recipients for this email group (CASE-SENSITIVE)
        lower_recipients = [recip.lower() for recip in email_group['kicksite_recipients']]
        recipients = customers[customers.Program.str.lower().isin(lower_recipients)]

        # create new df for this group and concat
        eg_df = pd.DataFrame(data={'Recipient': recipients.Email,
                                   'EmailGroup': [email_group['schedule_name']] * recipients.shape[0],
                                   'SubjectTitle': [email_group['subject_title']] * recipients.shape[0],
                                   'BodyPath': [email_group['body_path']] * recipients.shape[0]})

        schedule_df = pd.concat((schedule_df, eg_df))

    schedule_df.reset_index(inplace=True, drop=True)

    # split into morning_and_noon and afternoon by class time
    today_at_noon = np.datetime64(str(np.datetime64('today')) + 'T12:00')
    morning_and_noon_classes = classes_today[classes_today <= today_at_noon].index.tolist()
    afternoon_classes = classes_today[classes_today > today_at_noon].index.tolist()

    morning_and_noon_scheduled_df = schedule_df[schedule_df.EmailGroup.isin(morning_and_noon_classes)]
    afternoon_scheduled_df = schedule_df[schedule_df.EmailGroup.isin(afternoon_classes)]

    # get scheduling configuration
    batch_size = config['batch_size']
    batch_wait_time_sec = config['batch_wait_time_sec']

    # schedule the emails
    schedule_df = pd.concat((schedule_subset_time(morning_and_noon_scheduled_df,
                                                  config['start_send_time_map']['morning_and_noon'],
                                                  batch_size,
                                                  batch_wait_time_sec,
                                                  logger),
                             schedule_subset_time(afternoon_scheduled_df,
                                                  config['start_send_time_map']['afternoon'],
                                                  batch_size,
                                                  batch_wait_time_sec,
                                                  logger)))

    logger.info('Scheduled {} emails'.format(schedule_df.shape[0]))

    return schedule_df


if __name__ == '__main__':
    # get command line args
    args = common.handle_argparse()

    # configure logging
    logging = common.setup_logging(__file__, args.logs_dirpath)

    # load config
    config = common.read_config(args.config_filepath, logging)

    if config is None:
        logging.error('Invalid or missing config. Exiting {}...'.format(__file__))
        sys.exit(1)

    # process schedule and customer files
    classes_today = get_classes_today(config, logging)
    customers = get_customers(config, logging)

    if classes_today is None or customers is None:
        logging.warning('Exiting scheduler: no emails will be scheduled today.')
        sys.exit(0)

    # get df with email schedule
    scheduled_df = compute_email_schedule(config, classes_today, logging)

    # save to cache
    common.save_df(scheduled_df, common.get_cache_schedule_path(), logging)
    common.copyfile(args.config_filepath, common.get_cache_config_path(), logging)

    logging.info('Emails successfully saved and scheduled')