import os
from cerberus import Validator
import common


def validate_filepath(field, value, error):
    if value is not None:
        if not os.path.exists(value):
            error(field, 'path not found - please enter the absolute path')


def validate_customers(field, value, error):
    validate_filepath(field, value, error)
    customers = common.read_frame(value)

    if not customers.shape[0] > 0:
        error(field, 'no customers')

    required_columns = set(['Emails', 'Programs', 'Subscribed'])
    if len(required_columns.intersection(set(customers.columns))) != len(required_columns):
        error(field, 'Spreadsheet meeting at least one of {} columns'.format(required_columns))


def validate_schedule(field, value, error):
    validate_filepath(field, value, error)
    schedule = common.read_frame(value, index_col=0)

    required_columns = set(['M', 'T', 'W', 'Th', 'F', 'Sa'])
    if len(required_columns.intersection(set(schedule.columns))) != len(required_columns):
        error(field, 'Spreadsheet missing at least one of {} columns'.format(required_columns))


def validate_email_groups():
    # TODO: ensure that all email_groups' `schedule_name` is in the schedule
    pass


def validate_config(config):

    valid_filepath = {
        'type': 'string',
        'check_with': validate_filepath
    }
    valid_customers = {
        'type': 'string',
        'check_with': validate_customers
    }
    valid_schedule = {
        'type': 'string',
        'check_with': validate_schedule
    }
    valid_24hr_time = {
        'type': 'string',
        'regex': r'([01][0-9]|2[0-3]):[0-5][0-9]'
    }
    valid_string = {'type': 'string'}
    valid_integer = {'type': 'integer'}
    valid_string_list = {
        'type': 'list',
        'schema': {
            'type': 'string'
        }
    }

    config_schema = {
        "customers_path": valid_customers,
        "schedule_path": valid_schedule,
        "start_send_time_map": {
          'type': 'dict',
          'schema': {
               "morning_and_noon": valid_24hr_time,
               "afternoon": valid_24hr_time
           }
        },
        "batch_wait_time_sec": valid_integer,
        "batch_size": valid_integer,
        "email_groups": {
            'type': 'list',
            "schema": {
                'type': 'dict',
                'schema': {
                    "schedule_name": valid_string,
                    "kicksite_recipients": valid_string_list,
                    "subject_title": valid_string,
                    "body_path": valid_filepath
                }
            }
        }
    }

    v = Validator(config_schema)
    result = v.validate(config)

    # TODO: validate email_groups here

    return result, v.errors
