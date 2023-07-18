import requests
import json

# test result should be True/False

def send_slack_notification(webhook_url,
        script_1,
        script_2,
        comp_operator,
        test_result):
    try:
        if test_result == True:
            message = (f"""Validation Test Passed!: 
                       {script_1} / {script_2} / {comp_operator}
                       """)
        else:
            message = (f"""Validation Test Failed!: 
                       {script_1} / {script_2} / {comp_operator}
                       """)
        
        slack_data = {'text': message}
        response = requests.post(webhook_url, 
                                 data = json.dumps(slack_data),
                                 headers = {'Content-Type': 'application/json'})
        
        if response.status_code != 200:
            print(response)
            return False

    except Exception as e:
        print('error sending slack notification')
        print(str(e))
        return False
    


