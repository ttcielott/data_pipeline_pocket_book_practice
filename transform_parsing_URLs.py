from urllib.parse import urlsplit, parse_qs
import csv

# example URL to parse
url = """https://www.mydomain.com/page-name?utm_con
         tent=textlink&utm_medium=social&utm_source=twit
    ter&utm_campaign=fallsale""".replace(" ", "")

# create an empty list to append parsed components
parsed_url = []
all_url = []

split_url = urlsplit(url)
print(f"Split Result: {type(split_url)} \n {split_url} \n")

params = parse_qs(split_url.query)
print(f"Params: {type(params)} \n {params} \n\n")

domain = split_url.netloc
print(f"domain: {domain} \n")
parsed_url.append(domain)

path = split_url.path
print(f"path: {path} \n")
parsed_url.append(path)

# utm parameters : Urchin Tracking Module (UTM) parameters are URL parameters
# that are used for tracking marketing and ad campaigns 
print(f"utm content: {params['utm_content'][0]} ")
parsed_url.append(params['utm_content'][0])
print(f"utm medium: {params['utm_medium'][0]} ")
parsed_url.append(params['utm_medium'][0])
print(f"utm source: {params['utm_source'][0]} ")
parsed_url.append(params['utm_source'][0])
print(f"utm campaign: {params['utm_campaign'][0]} ")
parsed_url.append(params['utm_campaign'][0])


all_url.append(parsed_url)

export_file = 'export_file_parsed_url.csv'

# write csv file
with open(export_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter = "|")
    csvw.writerows(all_url)

