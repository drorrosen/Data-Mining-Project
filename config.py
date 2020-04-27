import logging
import sys
from datetime import datetime
from selenium import webdriver

PATH_OF_CHROME_DRIVER = 'chromedriver_linux64/chromedriver'
JOBS_IN_ISRAEL = 'https://www.glassdoor.com/Job/israel-jobs-SRCH_IL.0,6_IN119.htm?fromAge=1&radius=25'
JOBS_IN_UK = 'https://www.glassdoor.com/Job/uk-jobs-SRCH_IL.0,2_IN2.htm?fromAge=1&radius=25'
DATA_SCIENTISTS_USA = 'https://www.glassdoor.com/Job/us-data-scientist-jobs-SRCH_IL.0,2_IN1_KO3,' \
                      '17.htm?fromAge=1&radius=25'
INITIAL_LINKS = [JOBS_IN_ISRAEL, DATA_SCIENTISTS_USA, JOBS_IN_UK]
# 'https://www.glassdoor.com/Job/india-jobs-SRCH_IL.0,5_IN115.htm?fromAge=1'
START_OF_LOCATION = 3
RELOAD_TRIALS = 3
MAX_SEARCH_PAGES = 30
API_URL = "https://devru-latitude-longitude-find-v1.p.rapidapi.com/latlon.php"
HEADERS = {
    'x-rapidapi-host': "devru-latitude-longitude-find-v1.p.rapidapi.com",
    'x-rapidapi-key': "151ca9c654msh1d0ca7a14cd32c0p1563b4jsnbdbf9c04a2ea"
    }
GEO_AGENT = "myGeocoder"
REST_COUNTRIES_A = 'https://restcountries.eu/rest/v2/name/'
REST_COUNTRIES_B = '?fullText=true'
HOST = "localhost"
USER = "root"
PASSWORD = "**"
DB = "GlassdoorDB"
COMMIT_ITER = 1000
CHROME_OPTIONS = webdriver.ChromeOptions()
# CHROME_OPTIONS.add_argument('--no-sandbox')
# CHROME_OPTIONS.add_argument('--headless')
# CHROME_OPTIONS.add_argument('--disable-dev-shm-usage')
CHROME_OPTIONS.add_argument("--incognito")
logger = logging.getLogger("glassdoor_scraper")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f'GDScraper_{datetime.now()}.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler(sys.stdout))

