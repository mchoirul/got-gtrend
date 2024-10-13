# got-gtrend
python script to download latest trending from Google Trend website. Download CSV file and then load it up to BigQuery for easier analysis.

## Prerequisites
- Python: Install Python 3.8 or higher.
- Selenium: Install using ```pip install selenium```
- Pandas: Install using ```pip install pandas```
- Google Cloud SDK: Install and configure the Google Cloud SDK, enabling access to BigQuery.
- Chromedriver: Download the correct Chromedriver version for your Chrome browser and add it to your system's PATH.
- BigQuery project: Create a BigQuery project and table, noting the project ID, dataset ID, and table ID.

## Usage
open the googletrend_xv1.py file & edit to specify the url of Google Trend

Example:

Get latest 4 hours of trending topics for Indonesia's region

```googletrend_url = 'https://trends.google.com/trending?geo=ID&hours=4'```


Get latest 4 hours of trending topics for Indonesia's region

```googletrend_url = 'https://trends.google.com/trending?geo=SG&hours=24'```

Save the file and run in Python environment: ```python googletrend_xv1.py```
