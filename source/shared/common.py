import pandas as pd
import numpy as np
import os
import json
import smtplib
import logging
import sys
import argparse
import shutil
import pytz
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from . import validate


DAYS = ['M', 'T', 'W', 'Th', 'F', 'Sa']
LOG_FILENAME = 'jma_sender.log'
SCHEDULED_EMAILS_FILENAME = 'scheduled.csv'
CACHED_CONFIG_FILENAME = 'cached_config.json'


def handle_argparse():
    parser = argparse.ArgumentParser(description='Schedule emails to be sent today.')
    parser.add_argument('--config_filepath', type=str, help='/path/to/config.json')
    parser.add_argument('--logs_dirpath', type=str, help='/path/to/logs')

    return parser.parse_args()


def setup_logging(calling_filename, log_dirpath=None, level=logging.DEBUG):
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if log_dirpath is not None:
        file_handler = logging.FileHandler(os.path.normpath(os.path.join(log_dirpath,
                                                                         LOG_FILENAME)))
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info('{} launched at {}'.format(calling_filename, str(pd.Timestamp.now(pytz.timezone('Etc/GMT+5')))))

    return root_logger


def prepare_filepath(filepath, _raise=True):
    filepath = os.path.normpath(filepath)

    if not os.path.exists(filepath) and _raise:
        raise FileNotFoundError('No file found at {}.'.format(filepath))

    return filepath


def prepare_dirpath(filepath, _raise=True):
    filepath = os.path.normpath(filepath)
    dirpath = os.path.dirname(filepath)

    if not os.path.exists(dirpath) and _raise:
        raise NotADirectoryError('No directory found at {}.'.format(dirpath))

    return filepath


def log_and_add_error(msg, errors, logger):
    if errors is not None:
        errors.append(msg)
    logger.error(msg)


def read_config(config_filepath, logger, error_list=None):
    logger.info('Loading configuration {}'.format(config_filepath))
    config_prepared_filepath = prepare_filepath(config_filepath)

    if not config_prepared_filepath.endswith('.json'):
        log_and_add_error('{} does not have a .json extension.'.format(config_prepared_filepath), error_list, logger)
        return None

    with open(config_prepared_filepath) as f:
        try:
            config = json.load(f)
        except json.decoder.JSONDecodeError as e:
            log_and_add_error('Error in "{}" decoding: "{}"'.format(config_filepath, e), error_list, logger)
            return None

    is_valid, errors = validate.validate_config(config)

    if not is_valid:
        log_and_add_error('Loaded config contained the following errors: {}'.format(errors), error_list, logger)
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
    today = datetime.now(tz=pytz.timezone('Etc/GMT+5'))
    morn_and_noon_time = datetime.strptime(config['start_send_time_map']['morning_and_noon'], '%H:%M')
    afternoon_time = datetime.strptime(config['start_send_time_map']['afternoon'], '%H:%M')

    config['start_send_time_map']['morning_and_noon'] = datetime(year=today.year, month=today.month, day=today.day,
                                                                 hour=morn_and_noon_time.hour,
                                                                 minute=morn_and_noon_time.minute,
                                                                 tzinfo=pytz.timezone('Etc/GMT+5'))
    config['start_send_time_map']['afternoon'] = datetime(year=today.year, month=today.month, day=today.day,
                                                          hour=afternoon_time.hour,
                                                          minute=afternoon_time.minute,
                                                          tzinfo=pytz.timezone('Etc/GMT+5'))

    # set scheduling config
    config['batch_wait_time_sec'] = timedelta(seconds=config['batch_wait_time_sec'])


def get_cache_path():
    root_dir = __file__
    for i in range(0, 3):
        root_dir = os.path.dirname(root_dir)

    return root_dir


def get_cache_schedule_path():
    return os.path.join(get_cache_path(), 'cache', SCHEDULED_EMAILS_FILENAME)


def get_cache_config_path():
    return os.path.join(get_cache_path(), 'cache', CACHED_CONFIG_FILENAME)


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


def copyfile(src, dst, logger):
    src_prepared = prepare_filepath(src)
    dst_prepared = prepare_dirpath(dst)
    logger.info('Copying {} to {}'.format(src_prepared, dst_prepared))
    shutil.copyfile(src_prepared, dst_prepared)


def explode_str(df, col, sep):
    s = df[col]
    i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
    df = df.iloc[i].assign(**{col: sep.join(s).split(sep)}).reset_index()
    df[col] = df[col].str.strip()

    return df


def read_html(filepath):
    with open(os.path.normpath(filepath), 'r') as f:
        lines = [line.rstrip() for line in f]

    soup = BeautifulSoup(''.join(lines), "html.parser").find()

    return bool(soup), soup.prettify() if soup is not None else None


def get_seconds_from_epoch(dt):
    epoch = datetime.utcfromtimestamp(0).astimezone(pytz.timezone('Etc/GMT+5'))
    return (dt - epoch).total_seconds()


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

