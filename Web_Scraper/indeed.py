import os
import re
import json
import pytz
import random
import warnings
import requests
import numpy as np
import pandas as pd

from config import Config
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

warnings.filterwarnings('ignore')

class IndeedScraper:
    def __init__(self):
        # Mapping for column names
        self.column_mapping = {
            'company': 'Company',
            'salary_text': 'Salary',
            'pub_date': 'Date Posted',
            'display_title': 'Title',
            'job_location': 'Location',
            'job_key': 'Job ID',
            'view_job_link': 'Job Link',
            'job_types': 'Job Type'
        }
        self.prev_job_ids = set()  # Set to store previous job IDs

    # Function to find job types from the list of attributes
    def find_job_types(self, attributes_list): 
        for attr in attributes_list:
            if 'job-types' in attr.get('label', ''):
                if 'attributes' in attr:
                    for sub_attr in attr['attributes']:
                        if 'label' in sub_attr:
                            return sub_attr['label']
                return None

    # Function to format salary range
    def format_salary_range(self, salary_range):
        if 'min' in salary_range and 'max' in salary_range and 'type' in salary_range:
            min_salary = '${:,.2f}'.format(salary_range['min']) if salary_range['min'] != salary_range['max'] else '$0.00'
            max_salary = '${:,.2f}'.format(salary_range['max'])
            salary_type = salary_range['type'].lower().capitalize()

            return f"{min_salary} - {max_salary} a {salary_type}"
        else:
            return None

    # Function to fill the location based on Job Location
    def fill_location(self, row):
        if row['job_location']:  # Modify 'Job Location' to 'job_location'
            return 'Remote'
        else:
            return 'Hybrid/On Site'

    # Function to extract data from the soup object
    def get_data(self, soup):
        script = soup.find('script', id='mosaic-data')
        script_content = str(script.string)
        pattern = re.compile(r'window\.mosaic\.providerData\["mosaic-provider-jobcards"\]\s*=\s*({.*?});', re.DOTALL)
        match = pattern.search(script_content)
        
        ist_timezone = pytz.timezone('Asia/Kolkata')
        cst_timezone = pytz.timezone('America/Chicago')

        current_time_ist = datetime.now(ist_timezone)
        current_time_cst = current_time_ist.astimezone(cst_timezone)

        if match:
            json_data = match.group(1)
            parsed_data = json.loads(json_data)
        else:
            print("No match found.")

        metadata = parsed_data['metaData']
        mosaic_provider_jobcards_model = metadata['mosaicProviderJobCardsModel']
        results = mosaic_provider_jobcards_model['results']

        all_inner_dataframes = []

        for result in results:
            fields_to_extract = [
                'company', 'formattedLocation', 'remoteLocation', 'estimatedSalary', 'extractedSalary',
                'jobkey', 'pubDate', 'taxonomyAttributes', 'viewJobLink', 'title'
            ]

            extracted_data = {field: result.get(field) for field in fields_to_extract}
            job_key = extracted_data.get('jobkey', '')

            if job_key in self.prev_job_ids:
                continue

            extracted_salary = extracted_data.get('extractedSalary')
            estimated_salary = extracted_data.get('estimatedSalary')

            if extracted_salary is not None:
                salary_text = self.format_salary_range(extracted_salary)
            elif estimated_salary is not None:
                salary_text = self.format_salary_range(estimated_salary)
            else:
                salary_text = None

            company = extracted_data['company']
            display_title = extracted_data.get('title', '')
            job_location = extracted_data.get('formattedLocation', '')
            view_job_link = extracted_data.get('viewJobLink', '')
            job_types = self.find_job_types(extracted_data['taxonomyAttributes'])
            remotelocation = extracted_data['remoteLocation']

            dataframe = pd.DataFrame({
                'company': [company],
                'salary_text': [salary_text],
                'pub_date': [current_time_cst.strftime('%Y-%m-%d T%H:%M:%SZ')],
                'display_title': [display_title],
                'job_location': [job_location],
                'job_key': [job_key],
                'view_job_link': [view_job_link],
                'job_types': [job_types]
            })
            dataframe['Current date time (CST)'] = current_time_cst.strftime('%Y-%m-%dT%H:%M:%SZ')
            dataframe['Remote / Hybrid'] = dataframe.apply(self.fill_location, axis=1)
            dataframe['view_job_link'] = 'https://www.indeed.com' + dataframe['view_job_link']
            dataframe.rename(columns=self.column_mapping, inplace=True)
            all_inner_dataframes.append(dataframe)

            # Update the set of previous job IDs
            self.prev_job_ids.add(job_key)

        if all_inner_dataframes:  # Check if there are dataframes to concatenate
            dataframes = pd.concat(all_inner_dataframes, ignore_index=True)
            return dataframes
        else:
            return None

    # Function to run the Indeed scraper
    def run(self):
        # Set up output directory
        output_directory = Config.output_directory
        subdirectory = Config.subdirectory
        os.makedirs(os.path.join(output_directory, subdirectory), exist_ok=True)

        all_outer_dataframes = []

        for keyword in Config.keywords:
            Config.keyword = keyword

            for i in range(0, 120, 10):
                url = Config.url_indeed.format(keyword=Config.keyword, page=i)
                user_agent = random.choice(Config.USER_AGENT_LIST)
                proxy = Config.proxy
                proxies = {"http": proxy, "https": proxy}
                headers = {'User-Agent': user_agent}

                try:
                    response = requests.get(url, headers=headers, proxies=proxies, verify=False)
                    response.raise_for_status()
                    print('Success!')

                    soup = BeautifulSoup(response.content, 'html.parser')
                    dataframe1 = self.get_data(soup)

                    if dataframe1 is not None:
                        all_outer_dataframes.append(dataframe1)
                        print(f'Success for page {i} - {Config.keyword}')
                    else:
                        print(f'Sorry, no data found for {Config.keyword} on page {i}. Either you entered the keyword wrong or connection aborted.')

                except requests.RequestException as e:
                    print(f"Error: {e}")
                    print(f"Sorry, the website blocked your connection or there was another error. Status Code: {response.status_code}")

        final_dataframe = pd.concat(all_outer_dataframes, ignore_index=True)
        final_dataframe = final_dataframe[final_dataframe['Job Type'] != 'Full-time']
        final_dataframe.drop_duplicates(inplace=True)

        # Set up output path
        output_path = Config.output_csv_path1
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        final_dataframe.to_csv(os.path.join(output_path, Config.output_csv_indeed), index=False)

if __name__ == "__main__":
    indeed_scraper = IndeedScraper()
    indeed_scraper.run()
