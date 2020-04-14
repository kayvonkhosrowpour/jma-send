import pandas as pd
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
        # customers = customers[customers['Subscribed']]
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
        template_path = email_group['template_path']
        subject_title = email_group['subject_title']

        # validate fields
        valid_schedule_name = sch_name is not None and len(sch_name) > 0
        valid_recipients = recipients is not None and len(recipients) > 0

        valid_template = template_path is not None
        valid_template = valid_template and len(template_path) > 0
        valid_template = valid_template and os.path.exists(os.path.normpath(template_path))

        valid_sub = subject_title is not None and len(subject_title) > 0

        valid_html = False
        if valid_template:
            valid_html, errors = common.is_valid_html(template_path)
            if not valid_html:
                logger.error('Invalid HTML at {} with errors: {}'
                             .format(template_path, errors))

        if valid_schedule_name and valid_recipients and valid_template and valid_html and valid_sub:
            email_groups.append(email_group)

    return email_groups


def schedule_emails(config, email_groups, logger):
    email_log_df = pd.DataFrame(columns=['Recipient', 'EmailGroup', 'SubjectTitle',
                                         'TemplatePath', 'DateTimeSent'])

    for email_group in email_groups:
        # get all recipients for this email group (CASE-SENSITIVE)
        lower_recipients = [recip.lower() for recip in email_group['kicksite_recipients']]
        recipients = customers[customers.Program.str.lower().isin(lower_recipients)]

        # create new df for this group and concat
        eg_df = pd.DataFrame(data={'Recipient': recipients.Email})
        eg_df['EmailGroup'] = email_group['schedule_name']
        eg_df['SubjectTitle'] = email_group['subject_title']
        eg_df['TemplatePath'] = os.path.realpath(email_group['template_path'])
        email_log_df = pd.concat((email_log_df, eg_df))

    email_log_df.reset_index()

    logger.info('Prepared to send {} emails out today'.format(email_log_df.shape[0]))

    print(email_log_df)

    # TODO: break into morning_and_noon.csv, and afternoon.csv using `config`


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
    schedule_emails(config, email_groups, logging)
