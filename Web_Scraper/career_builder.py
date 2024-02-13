import re
import os
import time
import json
import random
import warnings
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from config import Config
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

class CareerBuilderScraper:
    def __init__(self):
        # Initialize class variables
        self.user_agent = random.choice(Config.USER_AGENT_LIST)
        self.headers = {'User-Agent': self.user_agent}
        self.proxy = Config.proxy
        self.proxies = {"http": self.proxy, "https": self.proxy}
        self.dfs = []  # List to store dataframes
        self.soups = []  # List to store soup objects
        self.prev_job_ids = set()  # Initialize an empty set to store previous job IDs

    # Function to categorize work type based on title
    def categorize_work_type(self, title):
        if 'Onsite' in title:
            return 'On-site'
        elif 'Hybrid' in title:
            return 'Hybrid'
        elif 'Remote' in title:
            return 'Remote'
        else:
            return None

    # Function to convert relative dates to absolute dates
    def convert_relative_dates(self, relative_date):
        try:
            if 'today' in relative_date or 'Today' in relative_date:
                return datetime.now().date()
            elif 'yesterday' in relative_date or '1 day ago' in relative_date:
                return (datetime.now() - timedelta(days=1)).date()
            elif 'days ago' in relative_date:
                days_ago = int(relative_date.split()[0])
                return (datetime.now() - timedelta(days=days_ago)).date()
            else:
                return None
        except Exception as e:
            return None

    # Function to extract data from the soup object
    def get_data(self, soup):
        try:
            a = soup.find_all('div', class_='collapsed-activated')
            all_inner_dfs = []

            for i in a:
                b = BeautifulSoup(str(i), 'html.parser')
                c = b.find_all('li', class_='data-results-content-parent relative bg-shadow')

                all_innermost_dfs = []

                for j in c:
                    inner_job_listings = []
                    d = BeautifulSoup(str(j), 'html.parser')
                    job_data = {}
                    
                    try:
                        # Extracting data
                        job_data['publish_time'] = d.find('div', class_='data-results-publish-time').text.strip()
                        job_data['title'] = d.find('div', class_='data-results-title').text.strip()
                        job_data['company'] = d.find('div', class_='data-details').find('span').text.strip()
                        job_data['location'] = d.find('div', class_='data-details').find_all('span')[1].text.strip()
                        job_data['employment_type'] = d.find('div', class_='data-details').find_all('span')[2].text.strip()
                        job_url = j.find('a', class_='data-results-content')['href']
                        job_data['url'] = f"https://www.careerbuilder.com{job_url}"
                        result = d.select('div.block:not(.show-mobile)')
                        job_data['result'] = result[0].get_text(strip=True)
                        
                        inner_job_listings.append(job_data)
                        df = pd.DataFrame(inner_job_listings)
                        all_innermost_dfs.append(df)

                    except Exception as e:
                        continue  # Skip this iteration if there's an error

                try:
                    df2 = pd.concat(all_innermost_dfs, ignore_index=True)
                    all_inner_dfs.append(df2)
                except Exception as e:
                    continue  # Skip this iteration if there's an error

            final_df = pd.concat(all_inner_dfs, ignore_index=True)
            final_df['Work Location'] = final_df['location'].apply(self.categorize_work_type)
            final_df['Date Posted'] = final_df['publish_time'].apply(self.convert_relative_dates)
            final_df['Current Date'] = datetime.now().date()  # Add a new column with today's date
            columns_mapping = {
                'title': 'Title',
                'company': 'Company',
                'location': 'Location',
                'employment_type': 'Job_type',
                'url': 'Job_url',
                'result':'Salary'
            }
            final_df.rename(columns=columns_mapping, inplace=True)

            try:
                # Extract job IDs and create a new column
                final_df['Job_id'] = final_df['Job_url'].str.extract(r'/job/(.*)')
            except AttributeError:
                final_df['Job_id'] = None

            final_df.drop(columns=['publish_time'], inplace=True)
            
            # Filter out jobs that have been fetched previously
            new_job_ids = set(final_df['Job_id'])
            new_job_ids = new_job_ids - self.prev_job_ids  # Remove job IDs that are already in prev_job_ids
            final_df = final_df[final_df['Job_id'].isin(new_job_ids)]

            # Update prev_job_ids with the new job IDs
            self.prev_job_ids.update(new_job_ids)

            return final_df

        except Exception as e:
            return None

    # Function to run the scraper
    def run(self):
        try:
            for keyword in Config.keywords:
                keyword_lower = keyword.lower()

                for u in range(1, 20):
                    url = Config.url_career.format(keyword=keyword_lower.replace(" ", "%20"), page=u)
                    try:
                        response = requests.get(url, headers=self.headers, proxies=self.proxies, verify=False)
                        response.raise_for_status()

                        if response.status_code == 200:
                            print('Success!')
                        else:
                            print('Sorry, your connection is blocked by the website')
                            continue

                        soup = BeautifulSoup(response.content, 'html.parser')
                        self.soups.append(soup)
                        result_df = self.get_data(soup)

                        if result_df.empty:
                            print('Sorry, but the bot did not find proper data on this page')
                            continue

                        self.dfs.append(result_df)
                        print(f'Success for the page: {u}')

                    except requests.RequestException as e:
                        print(f'Request error for page {u}: {e}')

                    except Exception as e:
                        print(f'Error for page {u}: {e}')

                    time.sleep(5)

        except Exception as e:
            print(f'An unexpected error occurred: {e}')

        # Concatenate dataframes
        final_df = pd.concat(self.dfs, ignore_index=True)
        final_df.drop_duplicates(inplace=True)

        # Define the output file path based on the config
        output_directory = Config.output_directory
        output_subdirectory = Config.subdirectory
        output_filename = Config.output_csv_career
        output_path = os.path.join(output_directory, output_subdirectory)
        output_file_path = os.path.join(output_path, output_filename)

        # Save the data to the specified file path
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        final_df.to_csv(output_file_path, index=False)


# if __name__ == "__main__":
#     scraper = CareerBuilderScraper()
#     scraper.run()
