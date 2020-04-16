import os
import re
from cerberus import Validator
import common


def validate_filepath(field, value, error):
    if value is not None:
        if not os.path.exists(value):
            error(field, '{} path not found - please use a valid, absolute path'.format(value))
            return False

    return True


def validate_24hr_time(field, value, error):
    if not re.match(r'^([01][0-9]|2[0-3]):[0-5][0-9]$', value):
        error(field, '{} does not match 24-HR time format (examples: 08:00, 14:30)'.format(value))


def validate_customers(field, value, error):
    if not validate_filepath(field, value, error):
        return

    customers = common.read_frame(value)

    if not customers.shape[0] > 0:
        error(field, 'no customers')

    required_columns = ['Emails', 'Programs', 'Subscribed']

    if len(set(required_columns).intersection(set(customers.columns))) != len(required_columns):
        error(field, 'Customers spreadsheet missing at least one of {} columns'
                     .format(required_columns))


def validate_schedule(field, value, error):
    if not validate_filepath(field, value, error):
        return

    schedule = common.read_frame(value, index_col=0).columns.tolist()

    days = ['M', 'T', 'W', 'Th', 'F', 'Sa']
    if len(days) != len(schedule) or len([x for x, y in zip(days, schedule) if x != y]) > 0:
        error(field, 'Customers spreadsheet columns {} do not match {}'.format(schedule, days))


def validate_email_groups(config):
    errors = []

    # make sure we have at least one email group
    if len(config['email_groups']) == 0:
        errors.append('Config email_groups contains no elements.')

    # make sure config contains valid schedule_name
    schedule = common.read_frame(config['schedule_path'], index_col=0).index
    schedule_names = [eg['schedule_name'] for eg in config['email_groups']]

    no_such_program = [sn for sn in schedule_names if sn not in schedule]
    if len(no_such_program) > 0:
        errors.append('Config file has email groups defined that are not in the schedule: {}'\
                      .format(no_such_program))

    # make sure config contains valid kicksite_recipients
    customers = common.read_frame(config['customers_path'])
    customers = customers[['Programs']]
    programs = common.explode_str(customers, 'Programs', ',').Programs.unique()

    recipients = set()
    for eg in config['email_groups']:
        recipients.update(eg['kicksite_recipients'])

    no_such_recipient = [r for r in recipients if r not in programs]
    if len(no_such_recipient) > 0:
        errors.append('Customers spreadsheet does not contain Kicksite program(s): {}'\
                      .format(no_such_recipient))

    return errors


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
        'check_with': validate_24hr_time
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

    is_valid_schema = v.validate(config)
    errors = v.errors

    if is_valid_schema:
        email_group_validation = validate_email_groups(config)
        if len(email_group_validation) > 0:
            errors['email_group_validation'] = email_group_validation

    return len(errors) == 0, errors
