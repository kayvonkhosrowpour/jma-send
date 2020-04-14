import time
import util
from constants import CONFIG_FILEPATH

# read config
config = util.read_config(CONFIG_FILEPATH)
send_time_map = config['send_time_map']
before_or_at_noon = send_time_map['before_or_at_noon']
after_noon = send_time_map['after_noon']