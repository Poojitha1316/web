# main.py
import os
import shutil
from config import Config
from indeed import IndeedScraper
from dice import Wrapper as DiceWrapper
from career_builder import CareerBuilderScraper
from zipRecruiter import Wrapper as ZipRecruiterWrapper

def main():
    # Run ZipRecruiter scraper
    print("Scraping Zip_Recruiter.....")
    zip_recruiter_wrapper = ZipRecruiterWrapper()
    zip_recruiter_wrapper.run()

    # # Run Indeed scraper
    print("Scraping Indeed.....")
    indeed_scraper = IndeedScraper()
    indeed_scraper.run()
    
    # Run Dice scraper
    print("Scraping Dice.....")
    dice_wrapper = DiceWrapper()
    dice_wrapper.run()

    # Run CareerBuilder scraper
    print("Scraping Career_Builder.....")
    career_builder_scraper = CareerBuilderScraper()
    career_builder_scraper.run()
    
    # Copy the output folder to the desktop
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    output_desktop_path = os.path.join(desktop_path, "output")
    
    # Copy the output folder to the desktop
    if not os.path.exists(output_desktop_path):
        shutil.copytree(Config.output_directory, output_desktop_path)

    print("Scraped successfully.....")

if __name__ == "__main__":
    main()
