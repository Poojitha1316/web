import re
import os
import time
import json
import pytz
import random
import warnings
import requests
import numpy as np
import pandas as pd

from config import Config
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, parse_qs

warnings.filterwarnings('ignore')

class Wrapper:
    def __init__(self):
        self.prev_job_ids = set()  # Initialize an empty set to store previous job IDs

    # Extract digits from the given text
    def extract_digits(self, text):
        digits = re.findall(r'\d', text)
        result = ''.join(digits)
        return result

    # Extract company and posted date from the URL
    def extract_data_from_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        company = query_params.get('company', [None])[0]
        posted_date_match = re.search(r'posted_time=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', url)
        posted_date = posted_date_match.group(1) if posted_date_match else None
        return company, posted_date

    # Process remote status from the remote dictionary
    def process_remote_status(self, remote_dict):
        if 'remote' in remote_dict and remote_dict['remote']:
            return 'remote'
        else:
            return ''

    # Extract job ID from the given URL
    def extract_job_id(self, url):
        if "jid=" in url:
            job_id_part = url.split("jid=")[1]
            job_id = job_id_part.split("&")[0]
            return job_id
        else:
            return None

    # Find and process job list data script
    def get_data(self, soup):
        ist_timezone = pytz.timezone('Asia/Kolkata')
        cst_timezone = pytz.timezone('America/Chicago')

        current_time_ist = datetime.now(ist_timezone)
        current_time_cst = current_time_ist.astimezone(cst_timezone)
        try:
            script = soup.find('script', id='js_variables')

            if not script or not script.string:
                print("Script content not found.")
                return None

            script_content = script.string
            json_data = json.loads(script_content)
            json_list = json_data.get('jobList', [])

            selected_fields = ['Title', 'City', 'FormattedSalaryShort', 'EmploymentType', 'EmploymentTags', 'JobURL', 'SaveJobURL']
            selected_data_list = []

            for json_data in json_list:
                try:
                    # Create a dictionary with only the selected fields for the current JSON
                    selected_data = {field: json_data.get(field, None) for field in selected_fields}

                    # Append the selected data to the list
                    selected_data_list.append(selected_data)
                except Exception as e:
                    print(f"Error in inner loop: {e}")
                    continue  # Skip this iteration if there's an error

            # Create a DataFrame from the selected data list
            dataframe = pd.DataFrame(selected_data_list)

            # Extract additional data from the 'SaveJobURL' field
            try:
                dataframe[['Company', 'Posted_date']] = dataframe['SaveJobURL'].apply(self.extract_data_from_url).apply(pd.Series)
            except Exception as e:
                print(f"Error extracting data from 'SaveJobURL': {e}")

            # Process 'EmploymentTags' to get 'RemoteStatus'
            try:
                dataframe['RemoteStatus'] = dataframe['EmploymentTags'].apply(self.process_remote_status)
            except Exception as e:
                print(f"Error processing 'EmploymentTags': {e}")

            # Extract 'JobID' from 'JobURL'
            try:
                dataframe['JobID'] = dataframe['JobURL'].apply(self.extract_job_id)
            except Exception as e:
                print(f"Error extracting 'JobID' from 'JobURL': {e}")

            # Add 'Current_Date_Time' 
            dataframe['Current date time (CST)'] = current_time_cst.strftime('%Y-%m-%d T%H:%M:%SZ')

            # Rename 'FormattedSalaryShort' column to 'Salary'
            dataframe.rename(columns={'FormattedSalaryShort': 'Salary'}, inplace=True)

            # Drop unnecessary columns
            dataframe.drop(['EmploymentTags', 'SaveJobURL'], axis=1, inplace=True)

            # Filter out the jobs that have been fetched previously
            new_job_ids = set(dataframe['JobID'])
            new_job_ids = new_job_ids - self.prev_job_ids  # Remove job IDs that are already in prev_job_ids
            dataframe = dataframe[dataframe['JobID'].isin(new_job_ids)]

            # Update prev_job_ids with the new job IDs
            self.prev_job_ids.update(new_job_ids)

            return dataframe

        except Exception as e:
            print(f"Error in outer try block: {e}")
            return None

    def run(self):
        proxy = Config.proxy
        proxies = {"http": proxy, "https": proxy}

        output_directory = Config.output_directory
        subdirectory = Config.subdirectory
        os.makedirs(os.path.join(output_directory, subdirectory), exist_ok=True)
        

        all_dataframes = []

        for keyword in Config.keywords:
            Config.keyword = keyword
            url = f'https://www.ziprecruiter.com/jobs-search?search={keyword}&location=&company=&refine_by_location_type=&radius=&days=&refine_by_salary=&refine_by_employment=employment_type%3Aemployment_type%3Acontract&'
            user_agent = random.choice(Config.USER_AGENT_LIST)
            headers = {'User-Agent': user_agent}
            response = requests.get(url, headers=headers, proxies=proxies, verify=False)

            if response.status_code == 200:
                print('Success!')
            else:
                # Print an error message
                print(f"Sorry, the website blocked your connection. Status Code: {response.status_code}")

            soup = BeautifulSoup(response.content, 'html.parser')
            a = BeautifulSoup(str(soup.find('div', class_='job_results_headline')), 'html.parser').find('h1').get_text(strip=True)
            result = int(self.extract_digits(a))
            dataframe1 = self.get_data(soup)
            all_dataframes.append(dataframe1)

            if 20 < result < 100:
                for j in range(2, 4):
                    url2 = f'https://www.ziprecruiter.com/jobs-search?search={keyword}&location=&company=&refine_by_location_type=&radius=&days=&refine_by_salary=&refine_by_employment=employment_type%3Aemployment_type%3Acontract&page={j}'

                    user_agent = random.choice(Config.USER_AGENT_LIST)
                    headers = {'User-Agent': user_agent}
                    res2 = requests.get(url2, headers=headers, proxies=proxies, verify=False)

                    if res2.status_code == 200:
                        print('Success!')
                    else:
                        # Print an error message
                        print(f"Sorry, the website blocked your connection. Status Code: {res2.status_code}")

                    soup2 = BeautifulSoup(res2.content, 'html.parser')
                    dataframe3 = self.get_data(soup2)
                    all_dataframes.append(dataframe3)
                    print('success for page ' + str(j))
            elif 100 < result:
                for i in range(2, 7):
                    url1 = f'https://www.ziprecruiter.com/jobs-search?search={keyword}&location=&company=&refine_by_location_type=&radius=&days=&refine_by_salary=&refine_by_employment=employment_type%3Aemployment_type%3Acontract&page={i}'

                    user_agent = random.choice(Config.USER_AGENT_LIST)
                    headers = {'User-Agent': user_agent}
                    res1 = requests.get(url1, headers=headers, proxies=proxies, verify=False)

                    if res1.status_code == 200:
                        print('Success!')
                    else:
                        # Print an error message
                        print(f"Sorry, the website blocked your connection. Status Code: {res1.status_code}")

                    soup1 = BeautifulSoup(res1.content, 'html.parser')
                    dataframe2 = self.get_data(soup1)
                    all_dataframes.append(dataframe2)
                    print('success for page ' + str(i))
            else:
                print('This keyword has only this data')

        final_dataframe = pd.concat(all_dataframes)
        final_dataframe = final_dataframe[final_dataframe['EmploymentType'] != 'Full-Time']
        final_dataframe.drop_duplicates(inplace=True)

        final_dataframe

        output_path = Config.output_csv_path1
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        final_dataframe.to_csv(os.path.join(output_path, Config.output_csv_zip), index=False)

if __name__ == "__main__":
    wrapper = Wrapper()
    wrapper.run()
