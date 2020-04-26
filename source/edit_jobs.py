import logging
from apscheduler.schedulers.background import BackgroundScheduler
from shared import common


class JobStore:

    def __init__(self, logger):
        self.logger = logger
        self.jobstore = 'EmailJob'
        self.scheduler = common.setup_scheduler(BackgroundScheduler, self.jobstore, logger, 'JobStore')
        self.scheduler.start(paused=False)  # do not process, read only

    def get_jobs(self):
        return self.scheduler.get_jobs(pending=True)

    def delete_all_jobs(self):
        self.scheduler.remove_all_jobs()

    def delete_jobs(self, jobs):
        for j in jobs:
            self.scheduler.remove_job(j.id)



if __name__ == '__main__':
    # get arguments and setup logging
    args = common.handle_argparse(config_filepath=False)
    logger = common.setup_logging(__file__, args.logs_dirpath, level=logging.WARNING)

    jobstore = JobStore(logger)

    # process
    print('---------------------------- JMA SENDER JOB EDITOR -----------------------------')
    print('INFO: enter name of email group (case-sensitive) you want to delete and de-\n'
          'schedule for today or "*" for all.')
    print('--------------------------------------------------------------------------------')

    while True:
        # show all jobs
        all_jobs = jobstore.get_jobs()
        print('\nTOTAL # OF SCHEDULED JOBS:', len(all_jobs))
        if len(all_jobs) == 0:
            print('Exiting...')
            break

        # group by email_group
        email_groups = {}
        for j in all_jobs:
            email_group = j.name.split('::')[0]
            if email_group not in email_groups:
                email_groups[email_group] = []
            email_groups[email_group].append(j)

        for eg, jobs in email_groups.items():
            print('>', eg, ':', len(jobs))

        # get user input
        _input = input('Enter name of group to delete: ')
        msg = None

        # process input and jobs
        if _input in email_groups:
            confirm = input('CONFIRM (Y/N): ')
            if confirm == 'Y':
                jobstore.delete_jobs(email_groups[_input])
                msg = 'INFO: Deleted {}'.format(_input)
            else:
                msg = 'INFO: Deletion aborted'
        elif _input == '*':
            confirm = input('CONFIRM (Y/N): ')
            if confirm == 'Y':
                jobstore.delete_all_jobs()
                msg = 'INFO: Deleted {}'.format(_input)
            else:
                msg = 'INFO: Deletion aborted'
        else:
            msg = 'ERROR: "{}" not in {}'.format(_input, list(email_groups.keys()))

        print(msg)
        input()  # user acknowledgement
