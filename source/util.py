import pandas as pd
import numpy as np
import os
import json
import tidylib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def get_todays_weekday():
    lookup = {i:weekday for i, weekday in enumerate(['M', 'T', 'W', 'Th', 'F', 'Sa', 'Su'])}
    
    return lookup[pd.Timestamp.today().weekday()]


def prepare_filepath(filepath, _raise=True):
    filepath = os.path.normpath(filepath)

    if not os.path.exists(filepath) and _raise:
        raise Exception('No file found at {}.'.format(filepath))

    return filepath


def read_config(filepath):
    with open(prepare_filepath(filepath)) as f:
        data = json.load(f)

    return data


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
