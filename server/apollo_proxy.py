import re
import time
import random
import requests
import pandas as pd
from urllib import parse
from api_keys import API_KEYS
from multiprocessing.pool import ThreadPool

RECORDS_PER_PAGE = 200
API_ENDPOINT = "https://api.apollo.io/v1/mixed_people/search"
API_RETRIES = 15
MAX_THREADS_PER_REQUEST = 100

def get_api_key():
    return random.choice(API_KEYS)

def convert_camel_case_to_snake_case(word):
    words = re.findall(r'[A-Z][a-z]+|[A-Z]?[a-z]+|[A-Z]+', word)
    snake_case = '_'.join(words).lower()
    print(f"URL Parameter :  {word} " + " "*(50 - len(word)) + f" API Parameter : {snake_case}")
    return snake_case

def parse_input_url(url):
    url = url.replace("https://app.apollo.io/#/people?", "https://app.apollo.io/people?")
    parsed_url = parse.urlparse(url)
    url_parameters = parse.parse_qs(parsed_url.query)
    parameters = {}
    for key, value in url_parameters.items():
        if str(key[-2:]) != "[]":
            if isinstance(value, list): value = value[0]
        parameters[convert_camel_case_to_snake_case(key)] = value
    return parameters

def get_param_list(parameters, max_iterations):
    param_list = []
    for i in range(1, max_iterations):
        parameters["page"] = i + 1
        param_list.append(parameters.copy())
    return param_list

def parse_lead(lead):
    if "organization" in lead.keys():
        org = lead["organization"]
        for k,v in org.items():
            lead["org_" + str(k)] = v
    
    if "phone_numbers" in lead.keys():
        ph_no = lead["phone_numbers"]        
        if len(ph_no) > 0:
            for k,v in ph_no[0].items():
                lead["ph_no_" + str(k)] = v

    del_fields = ["employment_history", "organization", "phone_numbers", "account"]  
    for field in del_fields:
        if field in lead.keys():
            del lead[field]
    
    return lead

def parse_response(data):
    leads = data["people"]
    processed_leads = []
    for lead in leads:
        processed_lead = parse_lead(lead)
        processed_leads.append(processed_lead)
    return processed_leads


def make_api_call(parameters):
    endpoint = "https://api.apollo.io/v1/mixed_people/search"
    headers = {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json'
    }
    for _ in range(API_RETRIES):
        parameters["api_key"] = get_api_key()
        response = requests.post(endpoint, headers=headers, json=parameters)
        if response.status_code == 200:
            return parse_response(response.json())
        
    print(f"API Error. Response Status: {response.status_code} Parameters: {parameters}")
    print(response.url)
    return None
    
def make_unparsed_api_call(parameters, endpoint = "https://api.apollo.io/v1/mixed_people/search"):
    headers = {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json'
    }

    for _ in range(API_RETRIES*5):
        parameters["api_key"] = get_api_key()
        response = requests.post(endpoint, headers=headers, json=parameters)    
        if response.status_code == 200:
            return response.json()
        
    print(f"API Error. Response Status: {response.status_code} Parameters: {parameters}")
    print(response.url)
    return None
    
def fetch_entire_data(parameters, records_limit, records_per_page, endpoint = "https://api.apollo.io/v1/mixed_people/search"):
    
    df_list = []
    
    parameters["page"] = 1
    parameters["per_page"] = records_per_page
    parameters["api_key"] = get_api_key()
    response = make_unparsed_api_call(parameters, endpoint)
    
    if response is not None:
        records = parse_response(response)
        fetched_df = pd.DataFrame.from_records(records)
        df_list.append(fetched_df)
        
        total_entries = response["pagination"]["total_entries"]
        page_count = response["pagination"]["per_page"]
        
        max_iterations = int(min(1 + records_limit//records_per_page, 1 + total_entries//records_per_page, page_count))
        
        param_list = get_param_list(parameters, max_iterations)
        with ThreadPool(MAX_THREADS_PER_REQUEST) as pool:
            for records in pool.map(make_api_call, param_list):
                if records is not None:
                    fetched_df = pd.DataFrame.from_records(records)
                    df_list.append(fetched_df)

    if len (df_list) == 0:
        return None
    return pd.concat(df_list)

def get_csv_from_url(url, num_records):
    parameters = parse_input_url(url)
    df = fetch_entire_data(parameters, num_records, RECORDS_PER_PAGE, API_ENDPOINT)
    if df is not None:
        return df