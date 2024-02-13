import os
import pytz
import requests
import pandas as pd

from config import Config
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

class Wrapper:
    # Initialize the class with configuration settings
    def __init__(self):
        self.config = Config()
        self.prev_job_ids = set()  # Initialize an empty set to store previous job IDs

    # Parse URL and extract relevant parameters
    def parse_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        q = query_params.get('q', [None])[0]
        location = query_params.get('location', [None])[0]
        latitude = query_params.get('latitude', [None])[0]
        longitude = query_params.get('longitude', [None])[0]

        return q, location, latitude, longitude

    # Fill the location based on Work type (remote/on-site)
    def fill_location(self, row):
        return 'Remote' if row['Work type(remote/on-site)'] else 'Hybrid/Onsite'

    # Create the output directory if it doesn't exist and fetches job data
    def run(self):
        output_path = os.path.join(self.config.output_csv_path2, self.config.output_csv_dice)
        os.makedirs(self.config.output_csv_path2, exist_ok=True)
        ist_timezone = pytz.timezone('Asia/Kolkata')
        cst_timezone = pytz.timezone('America/Chicago')

        current_time_ist = datetime.now(ist_timezone)
        current_time_cst = current_time_ist.astimezone(cst_timezone)

        for keyword in self.config.keywords:
            params = self.get_params(keyword)

            response = requests.get(
                self.config.url_dice,
                params=params,
                headers=self.config.HEADERS,
                timeout=30,
            ).json()

            try:
                data = response["data"]
            except Exception as e:
                print(f"Sorry could not get the data for {keyword}: {e}")
                continue

            if data:
                dataframe = pd.DataFrame(data)
                dataframe['jobLocation'] = dataframe['jobLocation'].apply(lambda x: x['displayName'] if isinstance(x, dict) and 'displayName' in x else None)
                dataframe1 = dataframe[['id', 'title', 'postedDate', 'detailsPageUrl', 'jobLocation', 'salary', 'companyName', 'employmentType',
                          'workFromHomeAvailability', 'isRemote', 'modifiedDate']]
                dataframe1.rename(columns=self.get_column_mapping(), inplace=True)
                dataframe1['Current date time (CST)'] = current_time_cst.strftime('%Y-%m-%dT%H:%M:%SZ')
                dataframe1['Work type(remote/on-site)'] = dataframe1.apply(self.fill_location, axis=1)
                dataframe1['Job Title'] = keyword  # Add a new column for the job title
                
                # Filter out jobs that have been fetched previously
                new_job_ids = set(dataframe1['Job_id'])
                new_job_ids = new_job_ids - self.prev_job_ids  # Remove job IDs that are already in prev_job_ids
                dataframe1 = dataframe1[dataframe1['Job_id'].isin(new_job_ids)]
                
                # Update prev_job_ids with the new job IDs
                self.prev_job_ids.update(new_job_ids)

                dataframe1.to_csv(output_path, mode='a', header=not os.path.exists(output_path), index=False)
                print(f'Successfully saved the data for {keyword}')
            else:
                print(f"Sorry we can't get the data for {keyword}. Please try again with correct url or keywords")

    # Get parameters based on search type
    def get_params(self, keyword):
        if self.config.search_type == '1':
            return self.get_keyword_params(keyword)
        elif self.config.search_type == '2':
            return self.get_url_params(keyword)
        else:
            print("Invalid search type")
            return {}

    # Get parameters for keyword-based search
    def get_keyword_params(self, keyword):
        return {
            "q": keyword,
            "countryCode2": "US",
            "radius": "30",
            "radiusUnit": "mi",
            "page": "1",
            "pageSize": "100",
            "facets": "employmentType|postedDate|workFromHomeAvailability|employerType|easyApply|isRemote",
            "fields": "id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug",
            "culture": "en",
            "recommendations": "true",
            "interactionId": "0",
            "fj": "true",
            "includeRemote": "true",
            "filters.employmentType": "CONTRACTS|PARTTIME"
        }

    # Get parameters for URL-based search
    def get_url_params(self, keyword):
        q, location, latitude, longitude = self.parse_url(keyword)
        return {
            "countryCode2": "US",
            "radius": "100",
            "radiusUnit": "mi",
            "page": "1",
            "q": q,
            "locationPrecision": 'city',
            "latitude": latitude,
            "longitude": longitude,
            "pageSize": "100",
            "facets": "employmentType|postedDate|workFromHomeAvailability|employerType|easyApply|isRemote",
            "fields": "id|jobId|guid|summary|title|postedDate|modifiedDate|jobLocation.displayName|detailsPageUrl|salary|companyName|employmentType|isHighlighted|score|easyApply|employerType|workFromHomeAvailability|isRemote|debug",
            "culture": "en",
            "recommendations": "true",
            "interactionId": "0",
            "fj": "true",
            "includeRemote": "true",
            "filters.employmentType": "CONTRACTS|PARTTIME"
        }

    # Define column mapping for output CSV
    def get_column_mapping(self):
        return {
            'id': 'Job_id',
            'companyName': 'Vendor company name',
            'title': 'Job title',
            'employmentType': 'Job type',
            'salary': 'Pay rate',
            'detailsPageUrl': 'Job posting url',
            'jobLocation': 'Job location',
            'postedDate': 'Job posting date',
            'isRemote': 'Work type(remote/on-site)',
            'workFromHomeAvailability': 'Work from availability',
            'modifiedDate': 'Modified Date'
        }

# Run the Wrapper class
if __name__ == "__main__":
    wrapper = Wrapper()
    wrapper.run()
