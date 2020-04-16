import pandas as pd
import numpy as np
import os
import json
import tidylib
import smtplib
import logging
import sys
import validate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


CONFIG_FILEPATH = '../config.json'
LOGFILE = './logs/jma_send.log'
TRANSACTION_FILEPATH = './logs/transaction_log.csv'


def setup_logging(filename, level=logging.DEBUG):
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    file_handler = logging.FileHandler(os.path.normpath(LOGFILE))
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info('{} launched at {}'.format(filename, str(pd.Timestamp.today())))

    return logging


def get_todays_weekday():
    lookup = {i:weekday for i, weekday in enumerate(['M', 'T', 'W', 'Th', 'F', 'Sa', 'Su'])}
    
    return lookup[pd.Timestamp.today().weekday()]


def prepare_filepath(filepath, _raise=True):
    filepath = os.path.normpath(filepath)

    if not os.path.exists(filepath) and _raise:
        raise Exception('No file found at {}.'.format(filepath))

    return filepath


def norm_path_and_join(prefix, filepath):
    if filepath:
        filepath = os.path.normpath(filepath)
        filepath = os.path.join(prefix, filepath)

    return filepath


def read_config(logger):
    logger.info('Loading config file from {}'.format(CONFIG_FILEPATH))

    with open(prepare_filepath(CONFIG_FILEPATH)) as f:
        config = json.load(f)

    is_valid, errors = validate.validate_config(config)

    if not is_valid:
        logger.error('Loaded config contained the following errors: {}'.format(errors))
        return None

    prefix = os.path.dirname(CONFIG_FILEPATH)

    # set paths relative to config file
    config['customers_path'] = norm_path_and_join(prefix, config['customers_path'])
    config['schedule_path'] = norm_path_and_join(prefix, config['schedule_path'])

    for email_group in config['email_groups']:
        email_group['body_path'] = norm_path_and_join(prefix, email_group['body_path'])

    # set datetimes
    config['start_send_time_map']['morning_and_noon'] = \
        np.datetime64('{}T{}'.format(str(np.datetime64('today')), config['start_send_time_map']['morning_and_noon']))
    config['start_send_time_map']['afternoon'] = \
        np.datetime64('{}T{}'.format(str(np.datetime64('today')), config['start_send_time_map']['afternoon']))

    # set scheduling config
    config['batch_wait_time_sec'] = np.timedelta64(config['batch_wait_time_sec'], 's')

    return config


def explode_str(df, col, sep):
    s = df[col]
    i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
    df = df.iloc[i].assign(**{col: sep.join(s).split(sep)}).reset_index()
    df[col] = df[col].str.strip()

    return df


def read_frame(filepath, index_col=None):
    df = None

    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath, index_col=index_col)
    elif filepath.endswith('.xlsx'):
        df = pd.read_excel(filepath, index_col=index_col)

    return df


def is_valid_html(html_filepath):
    with open(os.path.normpath(html_filepath), 'r') as f:
        lines = [line.rstrip() for line in f]
    html = ''.join(lines)
    document, errors = tidylib.tidy_document(html)
    has_error = 'error' in errors.lower()

    return not has_error, errors


def send_email(to, _from, subject, html_body):
    # hardcode for safety
    to = ['fake_email@gmail.com']
    # create email
    msg = MIMEMultipart()
    msg['From'] = _from
    msg['To'] = ','.join(to)
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html'))

    # send the message via our SMTP server
    s = smtplib.SMTP('localhost:1025')  # TODO: replace with exchange
    s.sendmail(_from, to, msg.as_string())
    s.quit()


def save_append_dataframe(filepath, new_df):
    filepath = os.path.normpath(filepath)

    if os.path.exists(filepath) and filepath.endswith('csv'):
        df = pd.read_csv(filepath)
        new_df = pd.concat((df, new_df))

    new_df.to_csv(filepath, index=False)
