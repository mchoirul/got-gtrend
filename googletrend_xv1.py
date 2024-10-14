
# extract google trend data from "Trending Now" section: https://trends.google.com/trending
# download csv & loaded into BigQuery
# author: choirul@gmail.com

# credit:
# https://hasdata.com/blog/how-to-scrape-google-trends
# https://ggiesa.wordpress.com/2018/05/15/scraping-google-trends-with-selenium-and-python/

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery

# assign configuration variables
class config_var:
    download_dir = '/home/pyproj/gtrend/download'  # Change to your desired folder
    download_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') #download timestamp
    downloaded_file_path = ''  #file path for downloaded csv
    table_id = 'yourproject-id.datasetname.table-name' # fully qualified table-id in BigQuery: projectid.datasetid.tablename
    projectid = 'yourproject-id' #GCP projectid

# Set up the chromedriver path
# make sure to install chromedriver first!
chrome_driver_path = '/usr/local/bin/chromedriver'
service = Service(chrome_driver_path)

# Initialize the WebDriver
# run in headless mode with no chrome browser window
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run Chrome in headless mode
options.add_argument("--no-sandbox")  #no-snadbox mode to to give permission accessing directory for saving downloaded csv
options.add_argument("--disable-dev-shm-usage") #use disk instead of shared memory

# specify default download directory to download automatically
# no prompt will be displayed
prefs = {
    "download.default_directory": config_var.download_dir,  # Set the default download location
    "download.prompt_for_download": False,  # Disable download prompt
    "download.directory_upgrade": True,  # Allow directory upgrade
    "safebrowsing.enabled": True  # Disable safe browsing to prevent blocking
}
options.add_experimental_option("prefs", prefs)

# Open Chrome with specified options
driver = webdriver.Chrome(service=service, options=options)


# method to click and download the CSV file from google trend website
def get_googletrend_trending(chrome_driver, gtrend_url):
    
    # Step 1: Open the Google Trends website
    chrome_driver.get(gtrend_url)

    time.sleep(10)  # waiting the page fully loaded

    # Step 2: Find the export button and click using JavaScript
    # click the Export button then download CSV
    # adjust the XPath or CSS selector if necessary
    export_button = chrome_driver.find_element(By.XPATH, "//body[@id='yDmH0d']/c-wiz/div/div[5]/div/c-wiz/div/div/div[3]/div[2]/div[2]/div/div/div/button/span[4]")

    # Use JavaScript to click the button
    chrome_driver.execute_script("arguments[0].click();", export_button)
    print("Clicked the Export button using JavaScript.")

    time.sleep(5)  # Wait for the next element to load

    # Step 3: Find the list item and click using JavaScript
    # click Download CSV item
    list_item = chrome_driver.find_element(By.XPATH, "//body[@id='yDmH0d']/c-wiz/div/div[5]/div/c-wiz/div/div/div[3]/div[2]/div[2]/div/div[2]/div/div/ul/li")
    chrome_driver.execute_script("arguments[0].click();", list_item)
    print("Clicked the Download CSV list item using JavaScript.")

    time.sleep(5)  # Wait for any further interactions or downloads


# method to check succesfull file download
def check_downloaded_file(download_dir_location):
    #declare timeout duration
    timeout = 20  # Timeout in seconds
    download_successful = False
    file_name = None

    # checking the download folder for the new file
    end_time = time.time() + timeout

    while time.time() < end_time:
        # Check if CSV file is added to the download directory
        files = os.listdir(download_dir_location)
        for file in files:
            if file.startswith('trending_') and file.endswith('.csv') :
                file_path = os.path.join(download_dir_location, file)
                # Ensure the file has finished downloading (by checking if it's still being written)
                if os.path.isfile(file_path) and not file.endswith(".crdownload"):
                    download_successful = True
                    file_name = file
                    break
        if download_successful:
            break
        time.sleep(1)  # Wait before checking again

    if download_successful:
        print(f"CSV File downloaded successfully: {file_name}")
        
        #assign to class variable to make it acessible
        config_var.downloaded_file_path = file_path
    else:
        print("File did not download within the expected time frame.")

#method to load google trend CSV file to BQ
def load_gtrend_csv_to_bq(gtrend_file_path, project_id, table_id):
    
    # Read the downloaded CSV data into a DataFrame
    df = pd.read_csv(gtrend_file_path, delimiter=',', quotechar='"')
    
    # Add two new columns to the DataFrame
    # adding additional data for easy tracking
    df['source_name'] = os.path.basename(gtrend_file_path)  # assign csv file name
    df['download_date'] = config_var.download_date  # assign download date and time
    
    # rename some columns in dataframe for easy querying in BigQuery
    df.rename(columns={'Search volume': 'Search_volume', 
                       'Trend breakdown': 'Trend_breakdown',
                       'Explore link': 'Explore_link'
                       }, inplace=True)
    
    #convert all column data type to string for easy loading
    #df=df.applymap(str)
    
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    try:
        job_config = bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",  # create table if not exist
            write_disposition="WRITE_APPEND",   # append rows 
        )

        #load dataframe to bq
        load_job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config ) # Make an API request.
          
        load_job.result()  # Wait for the job completed
        
        #get count of loaded rows
        gtrend_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(gtrend_table.num_rows))
        print(f"The following file has been loaded to BigQuery: {gtrend_file_path}")

        # delete csv file once uploaded 
        os.remove(gtrend_file_path)
        print(f"The following file has been deleted from local drive: {gtrend_file_path}")

    except Exception as e:
        print(e)
        
# navigate to google trend url then download the csv
try:
    # Open the Google Trends website
    # geo = indonesia, get the latest 4 hours trending
    # change the URL accordingly
    googletrend_url = 'https://trends.google.com/trending?geo=ID&hours=4'
    
    #download google trend - trending csv file
    get_googletrend_trending(driver, googletrend_url)

    #check if the CSV file already download successfuly
    check_downloaded_file(config_var.download_dir)

    # load to bigquery
    print('load to BQ')
    load_gtrend_csv_to_bq(config_var.downloaded_file_path,
                          config_var.projectid, 
                          config_var.table_id)

except Exception as e:
        print(e)

finally:
   # Close the WebDriver
    driver.quit()
