import pandas as pd
import numpy as np
import os
import json
import smtplib
import logging
import sys
import argparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from . import validate


DAYS = ['M', 'T', 'W', 'Th', 'F', 'Sa']
LOG_FILENAME = 'jma_sender.log'
SCHEDULED_EMAILS_FILENAME = 'scheduled.csv'


def handle_argparse():
    parser = argparse.ArgumentParser(description='Schedule emails to be sent today.')
    parser.add_argument('--config_filepath', type=str, help='/path/to/config.json')
    parser.add_argument('--logs_dirpath', type=str, help='/path/to/logs')

    return parser.parse_args()


def setup_logging(calling_filename, log_dirpath, level=logging.DEBUG):
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    file_handler = logging.FileHandler(os.path.normpath(os.path.join(log_dirpath,
                                                                     LOG_FILENAME)))
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info('{} launched at {}'.format(calling_filename, str(pd.Timestamp.today())))

    return logging


def prepare_filepath(filepath, _raise=True):
    filepath = os.path.normpath(filepath)

    if not os.path.exists(filepath) and _raise:
        raise FileNotFoundError('No file found at {}.'.format(filepath))

    return filepath


def read_config(config_filepath, logger):
    logger.info('Loading configuration {}'.format(config_filepath))
    config_prepared_filepath = prepare_filepath(config_filepath)

    if not config_prepared_filepath.endswith('.json'):
        logger.error('{} does not have a .json extension.'.format(config_prepared_filepath))
        return None

    with open(config_prepared_filepath) as f:
        try:
            config = json.load(f)
        except json.decoder.JSONDecodeError as e:
            logger.error('Error in "{}" decoding: "{}"'.format(config_filepath, e))
            return None

    is_valid, errors = validate.validate_config(config)

    if not is_valid:
        logger.error('Loaded config contained the following errors: {}'.format(errors))
        return None

    transform_config(config)

    return config


def transform_config(config):
    # set paths relative to config file
    config['customers_path'] = os.path.normpath(config['customers_path'])
    config['schedule_path'] = os.path.normpath(config['schedule_path'])

    for email_group in config['email_groups']:
        email_group['body_path'] = os.path.normpath(email_group['body_path'])

    # set datetimes
    config['start_send_time_map']['morning_and_noon'] = \
        np.datetime64('{}T{}'.format(str(np.datetime64('today')),
                                     config['start_send_time_map']['morning_and_noon']))
    config['start_send_time_map']['afternoon'] = \
        np.datetime64('{}T{}'.format(str(np.datetime64('today')),
                                     config['start_send_time_map']['afternoon']))

    # set scheduling config
    config['batch_wait_time_sec'] = np.timedelta64(config['batch_wait_time_sec'], 's')


def get_schedule_path_from_cache():

    root_dir = __file__
    for i in range(0, 3):
        root_dir = os.path.dirname(root_dir)

    return os.path.join(root_dir, 'cache', SCHEDULED_EMAILS_FILENAME)


def read_df(filepath, index_col=None):
    df = None

    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath, index_col=index_col)
    elif filepath.endswith('.xlsx'):
        df = pd.read_excel(filepath, index_col=index_col)

    return df


def save_df(df, filepath, logger, index=False, ext='csv'):
    filepath = os.path.normpath(filepath)

    if os.path.exists(filepath) and os.path.isfile(filepath):
        os.remove(filepath)

    logger.info('Saving dataframe of shape {} to {}'.format(df.shape, filepath))

    if ext == 'csv':
        df.to_csv(filepath, index=index)
    elif ext == 'xlsx':
        with pd.ExcelWriter(filepath) as writer:
            df.to_excel(writer, sheet_name='Schedule')
    else:
        raise ValueError('Unimplemented extension {}'.format(ext))


def explode_str(df, col, sep):
    s = df[col]
    i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
    df = df.iloc[i].assign(**{col: sep.join(s).split(sep)}).reset_index()
    df[col] = df[col].str.strip()

    return df


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

