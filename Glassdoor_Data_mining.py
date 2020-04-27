from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from sklearn.feature_extraction.text import TfidfVectorizer
import time
import random
import pandas as pd
import os
from datetime import datetime
import config as CFG
from collections import defaultdict
import click
import requests
import numpy as np
from geopy import Nominatim
from geopy.exc import GeocoderUnavailable
import mysql.connector

""""
In this program, we scrape Glassdoor site for job offers, using selenium and create a data frame with jobs data.

First each search link is loaded, then we gather all job posts links from the searches. 
After we have all job posts links, on each link we gather the data available.
Finally we create a data frame of positions with info of the role and company.
"""


class GDScraper:
    """
    Class for scraping glassdoor job posts out of search links
    Attributes:
        path: path to chrome driver
        search_links: Glassdoor search links for different job searches
        job_links: links to job posts collected from search links
    """

    def __init__(self, path, search_links=None):
        """
        This function initializes a GDScraper object with search links and creates
         a webdriver object to establish a connection to chrome.
        """
        if not os.path.exists(path):
            raise FileNotFoundError("'ChromeDriver' executable needs to be in path."
                                    "Please see https://sites.google.com/a/chromium.org/chromedriver/home")

        self._driver = webdriver.Chrome(executable_path=path, options=CFG.CHROME_OPTIONS)
        self.search_links = search_links
        self.job_links = []
        self.df = pd.DataFrame()

    def _close_popup(self):
        """This function closes pop-ups in search links, in they appear"""
        try:
            self._driver.find_element_by_id("prefix__icon-close-1").click()
        except NoSuchElementException:
            CFG.logger.info("No pop-up")

    def gather_job_links(self, limit_page_per_search=CFG.MAX_SEARCH_PAGES):
        """
        This function go over the instance's search links, for each search
         gathers all the links of job posts and returns them
        :limit_page_per_search: limit of pages to search per search link
        :return: list of links of all job posts
        """
        if limit_page_per_search is None:
            limit_page_per_search = CFG.MAX_SEARCH_PAGES
        links = []
        for search_link in self.search_links:
            self._driver.get(search_link)
            i = 0
            while True:
                job_headers = self._driver.find_elements_by_class_name('jobHeader')
                for job in job_headers:
                    links.append(job.find_element_by_css_selector('a').get_attribute('href'))
                i += 1
                print(f'Page {i} of {search_link} is done')
                if i == limit_page_per_search:
                    break
                try:
                    WebDriverWait(self._driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                                      "//li[@class='next']/a"))).click()
                    time.sleep(random.randint(2, 4))
                except NoSuchElementException:
                    CFG.logger.warning("Next page couldn't be clicked, last page assumed")
                    break
                except TimeoutException:
                    CFG.logger.warning("Next page couldn't be clicked, last page assumed")
                    break
                self._close_popup()
        CFG.logger.info(f'Total of {len(links)} links were gathered')
        self.job_links = links
        return links

    def gather_data_from_links(self, limit=None):
        """
        This function goes over all object's job links and creates a data frame out of the data pulled from each page
        :return: data frame with data collected from all the links
        """
        if self.job_links is None or len(self.job_links) < 1:
            CFG.logger.warning("No links passed to gather_data_from_links")
            return []
        glassdoor_jobs = pd.DataFrame(columns=['Job_ID', 'Title', 'Company', 'Location', 'Desc', 'Headquarters',
                                               'Size', 'Type', 'Revenue', 'Industry', 'Sector',
                                               'Company_Rating', 'Founded', 'Competitors', 'Scrape_Date'])

        if limit is not None:
            job_links = self.job_links[:limit]
        else:
            job_links = self.job_links
        num_of_links = len(job_links)
        for i, link in enumerate(job_links):
            CFG.logger.info(f'link {i + 1} out of {num_of_links}, {num_of_links - i - 1} left')
            job_post = JobPost(self._driver, link)
            job_post.go_to_page()
            time.sleep(random.randint(2, 4))
            self._close_popup()
            glassdoor_jobs.loc[i, 'Scrape_Date'] = datetime.now()
            glassdoor_jobs.loc[i, ['Job_ID', 'Title', 'Company', 'Location', 'Desc']] = job_post.get_main_tab()
            for col, val in job_post.get_company_tab().items():
                glassdoor_jobs.loc[i, col] = val
            glassdoor_jobs.loc[i, 'Company_Rating'] = job_post.get_rating()
            if glassdoor_jobs.loc[i, 'Location'] == 'Central' or glassdoor_jobs.loc[i, 'Location'] == 'Southern':
                glassdoor_jobs.loc[i, 'Location'] = glassdoor_jobs.loc[i, 'Headquarters']
        self.df = glassdoor_jobs
        glassdoor_jobs = self._enrich_df()
        self.df = glassdoor_jobs
        return glassdoor_jobs

    def _enrich_df(self):
        """
        This method deals with the object df, adding Country, HQ Country and replace missing values
        :return: df with added info and fixed missing values to be accepted by mysql
        """
        glassdoor_jobs = self.df
        glassdoor_jobs['Country'] = glassdoor_jobs['Location'].apply(find_country)
        glassdoor_jobs['HQ Country'] = glassdoor_jobs['Headquarters'].apply(find_country)

        glassdoor_jobs['Company_Rating'].fillna(-99, inplace=True)
        glassdoor_jobs.fillna("None", inplace=True)  # for mysql usage
        return glassdoor_jobs

    @staticmethod
    def long_lat_dict(dataframe):
        """
        Receives a dataframe and returns a dictionary with the unique latitude/longitude for each location
        :param dataframe: Glassdoor jobs data frame with locations and countries
        """

        locator = Nominatim(user_agent=CFG.GEO_AGENT)
        unique_locations = set(pd.concat([dataframe['Location'], dataframe['Country']]))
        coords_dict = {}
        for loc in unique_locations:
            if loc == np.nan or loc == 'None':
                coords_dict[loc] = ('', '')
            else:
                try:
                    location = locator.geocode(loc, timeout=20)
                except GeocoderUnavailable:
                    CFG.logger.warning(f'No response from Geocoder')
                    location = None
                if location is not None:
                    coords_dict[loc] = (location.longitude, location.latitude)
        return coords_dict

    @staticmethod
    def add_lon_lat(row, coords_dict):
        """For each row, adds its longitude and latitude
        :param row: row of Glassdoor data frame, Location and Country columns are mandatory
        :param coords_dict: dictionary of all locations in the object df and their coordinates
        :return: coordinates of the location in the row
        """
        if row['Location'] in coords_dict:
            longitude, latitude = coords_dict[row['Location']]
            return longitude, latitude
        else:
            longitude, latitude = coords_dict[row['Country']]
            return longitude, latitude

    @staticmethod
    def get_extra_country_info(country):
        """
        This method takes a country name and using restcountries api return countries population, capital city and
        region
        :param country: country name
        :return: A dictionary with population of the given country, capital of the given country
        and the region of the given country
        """
        response = requests.get(CFG.REST_COUNTRIES_A + country + CFG.REST_COUNTRIES_B)
        country_info = response.json()
        if isinstance(country_info, dict):
            population = None
            capital = None
            region = None
        else:
            population = country_info[0]['population']
            capital = country_info[0]['capital']
            region = country_info[0]['region']
        return {'Population': population, 'Capital': capital, 'Region': region}

    def _enrich_location(self):
        """This method enriches the data with coordinates, and country information and inserts it to location table in
        mysql db
        :return: df of the location with new country information
         """
        glassdoor_jobs = self.df
        df_location = pd.DataFrame()
        df_location['Location'] = pd.concat([glassdoor_jobs['Location'], glassdoor_jobs['Headquarters']])
        df_location['Country'] = pd.concat([glassdoor_jobs['Country'], glassdoor_jobs['HQ Country']])
        df_location.reset_index(drop=True, inplace=True)
        df_location['City'] = df_location.apply(lambda x: x['Location'].split(',')[0], axis=1)
        coords_dict = self.long_lat_dict(df_location)
        df_location['Longitude'], df_location['Latitude'] = df_location.apply(lambda x: self.add_lon_lat(
            x, coords_dict), axis=1).str
        df_location['Region'] = df_location['Country'].apply(lambda x:
                                                             self.get_extra_country_info(x)['Region'])
        df_location['Population'] = df_location['Country'].apply(lambda x:
                                                                 self.get_extra_country_info(x)['Population'])
        df_location['Capital'] = df_location['Country'].apply(lambda x:
                                                              self.get_extra_country_info(x)['Capital'])
        return df_location

    def location_to_mysql(self, mydb):
        """This method enriches the data with coordinates, and inserts it to location table in mysql db
         :param mydb: mysql db connection
         """
        my_cursor = mydb.cursor()
        df_location = self._enrich_location()
        df_location['Population'].fillna(-99, inplace=True)
        df_location.fillna('None', inplace=True)
        CFG.logger.info('insert into location table started')
        for i in range(len(df_location)):
            row = df_location.loc[i, :].tolist()
            row[-2] = int(row[-2])
            row = tuple(row)
            my_cursor.execute("""INSERT IGNORE INTO locations (
                                 location, country, city, longitude, latitude, region, population, capital)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", row)
            if i % CFG.COMMIT_ITER == 0:
                mydb.commit()
                CFG.logger.info('committed')
        mydb.commit()
        CFG.logger.info('committed')
        return

    def company_to_mysql(self, mydb):
        """
        This method inserts company data to company table in mysql db
        :param mydb: mysql db connection
        """
        glassdoor_jobs = self.df
        my_cursor = mydb.cursor()
        df_company = pd.DataFrame()
        df_company['Company_name'] = glassdoor_jobs['Company']
        df_company['country'] = glassdoor_jobs['HQ Country']
        df_company['city'] = glassdoor_jobs.apply(lambda x: x['Headquarters'].split(',')[0], axis=1)

        df_company['Size'] = glassdoor_jobs['Size']
        df_company['Founded'] = glassdoor_jobs['Founded']
        df_company['Type'] = glassdoor_jobs['Type']
        df_company['Industry'] = glassdoor_jobs['Industry']
        df_company['Sector'] = glassdoor_jobs['Sector']
        df_company['Revenue'] = glassdoor_jobs['Revenue']
        df_company['Rating'] = glassdoor_jobs['Company_Rating']

        CFG.logger.info('insert into company table started')
        for i in range(len(df_company)):
            row = df_company.loc[i, :]
            my_cursor.execute(f"""
                    SELECT id FROM locations WHERE country="{row['country']}" and city="{row['city']}" limit 1 
                    """)
            row['location_id'] = my_cursor.fetchone()[0]
            row = row[['Company_name', 'location_id', 'Size', 'Founded', 'Type', 'Industry', 'Sector', 'Revenue',
                       'Rating']]

            row = tuple(row.tolist())
            my_cursor.execute("""INSERT IGNORE INTO companies (
                                 company_name, location_id, size, founded, type, industry, sector, revenue, rating)
                                VALUES (%s, %s, %s, %s,  %s, %s, %s, %s, %s)""", row)
            if i % CFG.COMMIT_ITER == 0:
                mydb.commit()
                CFG.logger.info('committed')
        mydb.commit()
        CFG.logger.info('committed')
        return

    def jobs_to_mysql(self, mydb):
        """
        This method inserts jobs data to jobs table in mysql db
        :param mydb: mysql db connection
        """
        df_jobs = pd.DataFrame()
        glassdoor_jobs = self.df
        my_cursor = mydb.cursor()
        df_jobs['Job_Id'] = glassdoor_jobs['Job_ID']
        df_jobs['Title'] = glassdoor_jobs['Title']
        df_jobs['Company'] = glassdoor_jobs['Company']
        df_jobs['Desc'] = glassdoor_jobs['Desc']
        df_jobs['Scrape_Date'] = pd.to_datetime(glassdoor_jobs['Scrape_Date'])
        df_jobs['country'] = glassdoor_jobs['HQ Country']
        df_jobs['city'] = glassdoor_jobs.apply(lambda x: x['Headquarters'].split(',')[0], axis=1)

        CFG.logger.info('insert into jobs table started')
        for i in range(len(df_jobs)):
            row = df_jobs.loc[i, :]
            country = row['country']
            city = row['city']
            row.drop(['country', 'city'], inplace=True)
            my_cursor.execute(f"""
                               SELECT id FROM locations WHERE country="{country}" and city="{city}" limit 1 
                               """)
            row['location_id'] = my_cursor.fetchone()[0]
            row = row.tolist()
            if row[0] is None or row[0] == 'None':
                CFG.logger.info(f'row {i} skipped because non existing job id, missed info:\n{row}')
                continue
            row[0] = int(row[0])
            row = tuple(row)
            my_cursor.execute("""INSERT IGNORE INTO job_reqs (
                                 job_id, title, company, description, scrape_date, location_id)
                                VALUES (%s, %s, %s, %s, %s, %s)""", row)
            if i % CFG.COMMIT_ITER == 0:
                mydb.commit()
                CFG.logger.info('committed')
        mydb.commit()
        CFG.logger.info('committed')
        return

    def skills_to_mysql(self, mydb):
        """
        This method gathers and inserts skills data to skills and skills in jobs db tables
        :param mydb: mysql db connection

        """
        my_cursor = mydb.cursor()
        glassdoor_jobs = self.df
        df_skills = pd.DataFrame(columns=['Job_ID', 'Skill_ID'])
        bag_of_words = pd.read_sql_query("""SELECT * FROM skills""", mydb).set_index('skill_name')
        for index, row in glassdoor_jobs.iterrows():
            words = combinations(row['Desc'])
            for word in words:
                if word in bag_of_words.index:
                    skill_id = bag_of_words.loc[word]['skill_id']
                    df_skills = df_skills.append({'Job_ID': row['Job_ID'],
                                                  'Skill_ID': skill_id},
                                                 ignore_index=True)
        for i in range(len(df_skills)):
            row = tuple(df_skills.loc[i, :].tolist())
            my_cursor.execute("""INSERT IGNORE INTO skills_in_job  (
                                  job_id, skill_id)
                                VALUES (%s, %s)""", row)
            if i % 1000 == 0:
                mydb.commit()
        mydb.commit()


class JobPost:
    """
    Class for holding a job post and getting it's information
    Attributes:
        driver:  webdriver object
        job_link: links to job posts collected from search links
    """

    def __init__(self, driver, job_link):
        """Initialize a JobPost object with webdriver and link"""
        self.job_link = job_link
        self._driver = driver

    def go_to_page(self):
        """
        This function opens up the page of the link in the driver of the object
        """
        self._driver.get(self.job_link)

    def get_main_tab(self):
        """
        This function goes to the main tab of a job post links and returns title, company, location, desc
        :return: tuple with title, company, location, desc
        """
        jid = self._get_job_id()
        title = self._get_title()
        company = self._get_company()
        location = self._get_location()
        desc = self._get_desc()
        CFG.logger.debug("Main tab fetched")
        return jid, title, company, location, desc

    def _get_title(self):
        """
        This method gets the title of the JobPost within the main tab.
        :return: title of the job
        """
        title = None
        collected = False
        i = 0
        while not collected and i < CFG.RELOAD_TRIALS:
            try:
                # title = self._driver.find_element_by_class_name('mt-0.mb-xsm.strong').text
                title = self._driver.find_element_by_class_name('css-17x2pwl.e11nt52q5').text
                collected = True
            except NoSuchElementException:
                CFG.logger.warning(f'Title not collected on {i} trial')
                time.sleep(random.randint(6, 8))
                self.go_to_page()
                time.sleep(random.randint(2, 4))
            i += 1
        return title

    def _get_job_id(self):
        """
        This method gets the id of the JobPost from the main tab.
        :return: id of the job
        """
        jid = None
        collected = False
        i = 0
        while not collected and i < CFG.RELOAD_TRIALS:
            try:
                jid = self._driver.find_element_by_xpath("//div[@id='JobView']/div[@class='jobViewNodeContainer']"
                                                         ).get_attribute('id').split('_')[1]
                collected = True
            except NoSuchElementException:
                CFG.logger.warning(f'ID not collected on {i} trial')
                time.sleep(random.randint(6, 8))
                self.go_to_page()
                time.sleep(random.randint(2, 4))
            i += 1
        return jid

    def _get_company(self):
        """
        This method gets the company of the JobPost within main tab.
        :return: hiring company
        """
        try:
            # company = self._driver.find_element_by_class_name('strong.ib').text # css-16nw49e e11nt52q1
            company = self._driver.find_element_by_class_name('css-16nw49e.e11nt52q1').text.split()[0]
        except NoSuchElementException:
            company = None
            CFG.logger.warning("Company was not collected")
        except IndexError:
            company = None
            CFG.logger.warning("Company was not collected")
        return company

    def _get_location(self):
        """
        This method gets the location of the JobPost within main tab.
        :return: job location
        """
        try:
            # location = self._driver.find_element_by_class_name('subtle.ib').text[CFG.START_OF_LOCATION:] #
            location = self._driver.find_element_by_class_name('css-13et3b1.e11nt52q2').text  # [CFG.START_OF_LOCATION:]
        except NoSuchElementException:
            location = None
            CFG.logger.warning("Location was not collected")
        return location

    def _get_desc(self):
        """
        This method gets the description of the JobPost within main tab.
        :return: job description
        """
        try:
            desc = self._driver.find_element_by_class_name('desc.css-58vpdc.ecgq1xb3').text.replace('\n', ' ')
        except NoSuchElementException:
            desc = None
            CFG.logger.warning("Description was not collected")
        return desc

    def get_company_tab(self):
        """
        This method navigates to company tab and fetches information available
        :return: dictionary with al fields available in the company tab
        """
        # Headquarters, Size, Type, Revenue, Industry, Sector, Founded, Competitors
        data = defaultdict()
        try:
            self._driver.find_element_by_xpath("//span[@class='link' and text()='Company']").click()
            time.sleep(random.randint(2, 4))
            fields = self._driver.find_elements_by_xpath("//label[@for='InfoFields']")
            values = self._driver.find_elements_by_class_name("value")
            for field in zip(fields, values):
                field_name = field[0].text
                field_value = field[1].text
                data[field_name] = field_value
        except NoSuchElementException:
            CFG.logger.info('Partial data collected from company tab')
        CFG.logger.debug("Company tab fetched")
        return data

    def get_rating(self):
        """
        This method navigates to rating tab and gets the rating of the company
        :return: the rating of the company
        """
        try:
            self._driver.find_element_by_xpath("//span[@class='link' and text()='Rating']").click()
            time.sleep(random.randint(2, 4))
            rating = float(
                self._driver.find_element_by_class_name('mr-sm.css-16h0h8a.e1dyssh91').text)
        except NoSuchElementException:
            rating = None
            CFG.logger.warning("Rating was not found on page, and not collected")
        CFG.logger.debug("Rating tab fetched")
        return rating


def find_country(location):
    """
    This function finds the country of the location using an API
    :param location: the location to find it's country
    :return: The country of the given location if found, None otherwise
    """
    response = requests.request("GET", CFG.API_URL, headers=CFG.HEADERS, params={'location': location})
    if len(eval(response.text)['Results']) != 0 and eval(response.text)['Results'][0]['c'] == 'US':
        country = 'USA'
    elif len(eval(response.text)['Results']) == 0 and len(location) > 2:
        response = requests.request("GET", CFG.API_URL, headers=CFG.HEADERS, params={'location': location[:-2]})
        if len(eval(response.text)['Results']) == 0:
            if len(location.split(',')) > 1:
                country = location.split(',')[-1].strip()
            else:
                country = None
        else:
            # country = eval(response.text)['Results'][0]['c']
            country = eval(response.text)['Results'][0]['name'].split(',')[-1].strip()

    else:
        # country = eval(response.text)['Results'][0]['c']
        country = eval(response.text)['Results'][0]['name'].split(',')[-1].strip()
    CFG.logger.debug(country)
    return country


def combinations(description):
    """
    Receives a text and returns combinations of words(up to two words together)
    :param description: text to get word combinations from
    :return: word combinations gathered from the text
    """
    vectorized = TfidfVectorizer(stop_words=['english', 'make'], ngram_range=(1, 2))
    vectorized.fit_transform([description])
    combos = vectorized.get_feature_names()
    return combos


@click.command()
@click.option('--limit_search_pages', type=click.IntRange(1, CFG.MAX_SEARCH_PAGES), default=None,
              help=f'limit the number of pages in the search to gather job posts from 1-{CFG.MAX_SEARCH_PAGES}')
@click.option('--limit_job_posts', default=None, type=click.IntRange(1, 1000),
              help='limit the number of job posts to gather data from 1-1000')
@click.option('--IL', 'search_option', flag_value=0, help='searches to gather job posts IL - Israel')
@click.option('--DSUS', 'search_option', flag_value=1, help='searches to gather job posts DSUS - DATA_SCIENTISTS_USA')
@click.option('--UK', 'search_option', flag_value=2, help='searches to gather job posts UK - United Kingdom')
@click.option('--ALL', 'search_option', flag_value=3, default=3, help='searches to gather job posts: Israel,'
                                                                      'Data Scientists USA, UK')
def scrape_glassdoor(limit_search_pages, limit_job_posts, search_option):
    """
    Scraping Glassdoor site for job offers, and create a data frame with jobs data.
    Using search links chosen, and up to a limit of pages in the search links and a limit of total job offers.
    :param limit_search_pages: limit the number of pages in the search to gather job posts from
    :param limit_job_posts: limit the number of job posts to gather data from
    :param search_option: searches to gather job posts from, few options provided
    """
    print(limit_search_pages, limit_job_posts, search_option)
    if search_option == 3:
        search_links = CFG.INITIAL_LINKS
    else:
        search_links = [CFG.INITIAL_LINKS[search_option]]
    gd_scraper = GDScraper(CFG.PATH_OF_CHROME_DRIVER, search_links)
    gd_scraper.gather_job_links(limit_search_pages)
    glassdoor_jobs = gd_scraper.gather_data_from_links(limit_job_posts)
    glassdoor_jobs.to_csv(f"glassdoor_jobs{datetime.now()}.csv")
    mydb = mysql.connector.connect(host=CFG.HOST, user=CFG.USER, passwd=CFG.PASSWORD, database=CFG.DB)
    # gd_scraper.df = pd.read_csv('glassdoor_jobs2020-04-24 07:55:21.074133.csv')
    gd_scraper.location_to_mysql(mydb)
    gd_scraper.company_to_mysql(mydb)

    gd_scraper.jobs_to_mysql(mydb)
    gd_scraper.skills_to_mysql(mydb)
    return


def main():
    CFG.logger.info(f'Started at {datetime.now()}')
    scrape_glassdoor()
    CFG.logger.info(f'Ended at {datetime.now()}')
    return


if __name__ == '__main__':
    main()
