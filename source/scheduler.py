import pandas as pd
import numpy as np
import traceback
import pytz
import send_emails
import threading
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from pid.decorator import pidfile
from time import sleep
from shared import common


def setup_scheduler(scheduler_type, job_type, logger, debug=''):
    logger.info('Creating {} scheduler for {} jobs [{}-{}]'.format(scheduler_type, job_type, debug,
                                                                   threading.current_thread().ident))

    scheduler = scheduler_type()
    scheduler.add_jobstore(alias='mongodb-{}'.format(job_type),
                           jobstore=MongoDBJobStore(database='EmailSchedule', collection=job_type))

    scheduler.add_executor(alias='executor-{}'.format(job_type),
                           executor=ProcessPoolExecutor(max_workers=20))
    scheduler.timezone = pytz.timezone('Etc/GMT+5')

    return scheduler


def schedule_email_jobs(logs_dirpath, config_filepath):
    # configure logging
    logging = common.setup_logging(__file__, logs_dirpath)

    # load config
    config = common.read_config(config_filepath, logging)

    if config is None:
        logging.error('Invalid or missing config. Exiting {}...'.format(__file__))
        return

    # process schedule and customer files
    classes_today = get_classes_today(config, logging)
    customers = get_customers(config, logging)

    if classes_today is None or customers is None:
        logging.warning('Exiting scheduler: no emails will be scheduled today.')
        return

    # get df with email schedule
    scheduled_df = compute_email_schedule(config, classes_today, customers, logging)

    scheduler = setup_scheduler(BackgroundScheduler, 'EmailJob', logging, 'schedule_email_jobs')
    scheduler.start()
    logging.info('Scheduling {} tasks'.format(scheduled_df.shape[0]))

    for _, row in scheduled_df.iterrows():
        job = scheduler.add_job(func=send_emails.send_email,
                                trigger='date',
                                args=row.tolist() + [logs_dirpath],
                                jobstore='mongodb-EmailJob',
                                executor='executor-EmailJob',
                                name='::'.join([str(c) for c in row[['EmailGroup', 'Recipient']]]),
                                misfire_grace_time=int(row['GraceTimeSeconds']), # 100000000
                                coalesce=False,
                                max_instances=1,
                                next_run_time=row['ScheduledTime'],  # DEBUG datetime.now(pytz.timezone('Etc/GMT+5')) + timedelta(seconds=10),
                                replace_existing=True)
        logging.info('Added job with ID {} expiring at {}'.format(job.id, int(row['GraceTimeSeconds'])))

    logging.info('Sleeping until jobs are added to queue...')
    while len(scheduler.get_jobs(pending=True)) > 0:
        sleep(1)

    scheduler.shutdown()  # remove connection

    logging.info('Emails successfully scheduled')


def get_classes_today(config, logger):
    logger.info('Loading classes and customers')

    schedule = common.read_df(config['schedule_path'], index_col=0)
    lookup = {i: weekday for i, weekday in enumerate(common.DAYS)}
    today = pd.Timestamp.today(tz=pytz.timezone('Etc/GMT+5')).weekday()
    # today = 5  # DEBUG: HARDCODE SATURDAY
    if today == 6:
        logger.warning('No classes on Sunday')
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
    except Exception:
        logger.error('Could not process customers with exception: {}'
                     .format(traceback.format_exc()))

    return customers


def compute_email_schedule(config, classes_today, customers, logger):
    logger.info('Computing email schedule')

    # create df and define types
    schedule_df = pd.DataFrame(columns=['EmailGroup', 'Recipient', 'SubjectTitle', 'HtmlBody', 'ScheduledTime',
                                        'ClassTime', 'GraceTimeSeconds'])
    schedule_df.EmailGroup = schedule_df.EmailGroup.astype(str)
    schedule_df.Recipient = schedule_df.Recipient.astype(str)
    schedule_df.SubjectTitle = schedule_df.SubjectTitle.astype(str)
    schedule_df.HtmlBody = schedule_df.HtmlBody.astype(str)
    schedule_df.ScheduledTime = pd.to_datetime(schedule_df.ScheduledTime)
    schedule_df.ClassTime = pd.to_datetime(schedule_df.ClassTime)
    schedule_df.GraceTimeSeconds = schedule_df.GraceTimeSeconds.astype(int)

    # create df for each recipient in an email group
    for email_group in config['email_groups']:

        # get all recipients for this email group (CASE-SENSITIVE)
        lower_recipients = [recip.lower() for recip in email_group['kicksite_recipients']]
        recipients = customers[customers.Program.str.lower().isin(lower_recipients)]

        # read HTML
        _, html = common.read_html(email_group['body_path'])

        # create new df for this group and concat
        eg_df = pd.DataFrame(data={'EmailGroup': [email_group['schedule_name']] * recipients.shape[0],
                                   'Recipient': recipients.Email,
                                   'SubjectTitle': [email_group['subject_title']] * recipients.shape[0],
                                   'HtmlBody': [html] * recipients.shape[0]})

        schedule_df = pd.concat((schedule_df, eg_df), sort=False)

    schedule_df.reset_index(inplace=True, drop=True)

    # split into morning_and_noon and afternoon by class time
    today_at_noon = np.datetime64(str(np.datetime64('today')) + 'T12:00')
    morning_and_noon_classes = classes_today[classes_today <= today_at_noon].index.tolist()
    afternoon_classes = classes_today[classes_today > today_at_noon].index.tolist()

    morning_and_noon_scheduled_df = schedule_df[schedule_df.EmailGroup.isin(morning_and_noon_classes)]
    afternoon_scheduled_df = schedule_df[schedule_df.EmailGroup.isin(afternoon_classes)]

    morning_and_noon_scheduled_df.ClassTime = \
        morning_and_noon_scheduled_df.EmailGroup.apply(lambda x: classes_today[x]).dt.tz_localize('Etc/GMT+5')
    afternoon_scheduled_df.ClassTime = \
        afternoon_scheduled_df.EmailGroup.apply(lambda x: classes_today[x]).dt.tz_localize('Etc/GMT+5')

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

    return schedule_df


def schedule_subset_time(subset_df, start_datetime, batch_size, wait_time, logger):
    scheduled_emails_df = pd.DataFrame()
    current_datetime = start_datetime.astimezone(pytz.timezone('Etc/GMT+5'))

    # process each email group
    for email_group in subset_df.EmailGroup.unique():

        schedule_by_email_group = subset_df[subset_df.EmailGroup == email_group].copy()

        # don't send duplicate emails
        schedule_by_email_group.drop_duplicates(subset=['Recipient', 'EmailGroup'], inplace=True)

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
            logger.info('Preparing {} [{}:{}/{}] to send at {}'
                        .format(email_group, start, end, schedule_by_email_group.shape[0], current_datetime))

            batch_idx = schedule_by_email_group.iloc[start:end].index
            schedule_by_email_group.loc[batch_idx, 'ScheduledTime'] = [current_datetime] * size

            # move to next batch and datetime
            idx += batch_size
            current_datetime += wait_time

        scheduled_emails_df = pd.concat((scheduled_emails_df, schedule_by_email_group))

    # at least 30 minutes before class time (if already passed, then set to grace time of 1 to fail)
    for idx in scheduled_emails_df.index:
        ct = scheduled_emails_df.loc[idx, 'ClassTime'].to_pydatetime().astimezone(pytz.timezone('Etc/GMT+5'))
        st = scheduled_emails_df.loc[idx, 'ScheduledTime'].astimezone(pytz.timezone('Etc/GMT+5'))
        scheduled_emails_df.loc[idx, 'GraceTimeSeconds'] = max(1, int((ct - st).total_seconds()) - 30 * 60)

    return scheduled_emails_df


@pidfile()  # only run one instance of this script at a time
def main():
    # get command line args
    args = common.handle_argparse()

    # configure logging
    logging = common.setup_logging(__file__, args.logs_dirpath)

    # configure scheduler for daily CronJob
    config_scheduler = setup_scheduler(BackgroundScheduler, 'CronJob', logging, 'main')
    config_scheduler.start()
    config_scheduler.add_job(id='daily_job',
                             func=schedule_email_jobs,
                             args=[args.logs_dirpath, args.config_filepath],
                             jobstore='mongodb-CronJob',
                             executor='executor-CronJob',
                             name="Daily CronJob scheduler",
                             trigger='cron',
                             day_of_week='mon-sat', hour=5, minute=00,     # run at 5AM M-Sa
                             # day_of_week='mon-sun',  # DEBUG configuration
                             # hour=datetime.datetime.now(pytz.timezone('Etc/GMT+5')).hour,
                             # minute=datetime.datetime.now(pytz.timezone('Etc/GMT+5')).minute + 1,
                             timezone=pytz.timezone('Etc/GMT+5'),
                             misfire_grace_time=(24-5)*60*60,              # allow misfire up to midnight
                             coalesce=True,                                # if 5AM missed, run when possible
                             max_instances=1,
                             replace_existing=True)                        # replace if already exists in DB
    # schedule_email_jobs(args.logs_dirpath, args.config_filepath)  # DEBUG

    # configure scheduler for EmailJob, if they exist - allow processing of emails
    email_scheduler = setup_scheduler(BlockingScheduler, 'EmailJob', logging, 'main')
    email_scheduler.start()  # blocking call, will not exit


if __name__ == '__main__':
    main()
