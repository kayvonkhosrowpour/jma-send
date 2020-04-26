# jma-sender

**jma-sender** is a python app that automatically sends emails for [Journey Martial Arts](http://www.journeyma.com/), a martial arts company in Austin, TX. The app is integrated with the [Kicksite CRM](https://kicksite.com/) and a custom schedule that the company uses, but can be extended for other purposes.

The app uses a `config.json` file to determine email groups, recipients, and content of the emails. The `customers` csv/xlsx file contains the Kicksite customer data. The `schedule` csv/xlsx contains the schedule from which the email groups are driven.

### Tech

**jma-sender** uses the following

* python 3.8: although I expect it to work all 3.5+. See the dependencies in `jma_sender_requirements.txt` or `jma_sender_env.yaml`.
* [MongoDB 4.2.6](https://www.mongodb.com/download-center/community)

### Installation

Clone the repo. Install the dependencies using [pip](https://pip.pypa.io/en/stable/) or [anaconda](https://www.anaconda.com/) using `jma_sender_requirements.txt` or `jma_sender_env.yaml`.

### Usage

##### Schedule and process jobs
Run the automated email scheduler using the following (with your own paths to the config.json file and logs directory as appropriate):
```
python source/scheduler.py --config_filepath=config.json --logs_dirpath=logs
```

##### Validate data and config file
To validate your data and config file:
```
python validate_config.py
```

##### Delete currently scheduled jobs
To delete email groups:
```
python source/edit_jobs.py --logs_dirpath=logs
```

The console will guide the options that are to available to you, if there are emails that are currently being scheduled.