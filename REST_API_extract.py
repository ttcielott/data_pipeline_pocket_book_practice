import requests
import json
import configparser
import csv
import boto3

# use requests library to query API endpoint (Sunrise and Sunset)
# and get back the response 

# London's coordinates
lat = 51.5072
lng = 0.1276
coordinate_list = [("london", 51.5072, 0.1276), 
                   ("Milan", 45.4642, 9.1900), 
                   ("Seoul", 37.5519, 126.9918)]

sunrise_sunset_times = []
for city_name, lat, lng in coordinate_list:

    coordinate_param = {"lat": lat, "lng": lng}

    # sent an HTTP GET request to the API end point
    api_response = requests.get(
        "https://api.sunrise-sunset.org/json", 
        params = coordinate_param)

    # create a json object from the response content
    response_json = json.loads(api_response.content)

    # store the sunrise time and sunset time respectively
    sunrise = response_json['results']['sunrise']
    sunset = response_json['results']['sunset']

    sunrise_sunset_times.append([city_name, sunrise, sunset])

# write csv file
export_file = "export_file_sun.csv"

with open(export_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter = '|')
    csvw.writerows(sunrise_sunset_times)

# upload csv file to the s3 bucket

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key") 
secret_key = parser.get("aws_boto_credentials", "secret_key") 
bucket_name = parser.get("aws_boto_credentials", "bucket_name") 

s3 = boto3.client('s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key)

s3_file = export_file

s3.upload_file(export_file, bucket_name, s3_file)
