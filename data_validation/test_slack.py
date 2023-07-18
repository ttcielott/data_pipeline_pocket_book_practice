# using this python file, check if sending message to slack using webhook url is working
import sys
import configparser
from  send_slack import *

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
webhook_url = parser.get('slack_notification', 'webhook_url')

test_result = sys.argv[1]
script_1 = sys.argv[2]
script_2 = sys.argv[3]
comp_operator = sys.argv[4]

if test_result == True: 
    exit(0)
else:
    send_slack_notification(
        webhook_url,
        script_1,
        script_2,
        comp_operator,
        test_result
    )
    exit(-1)
