import mysql.connector
import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urlparse
from fake_useragent import UserAgent
import json
import logging
import tldextract

# Configure the logging module
logging.basicConfig(level=logging.INFO)

# Create a logger
logger = logging.getLogger(_name_)

def serialize(obj):
    if isinstance(obj, (list, dict, str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, BeautifulSoup):
        return str(obj)
    elif isinstance(obj, Tag):
        return obj.text.strip()
    else:
        return repr(obj)

def is_top_level_domain(url):
     ext = tldextract.extract(url)
     return not ext.subdomain
def get_random_user_agent():
    user_agent = UserAgent()
    return {'User-Agent': user_agent.random}

def scrape_about_us_page(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            about_us_tag = soup.find('a', string='About Us') or soup.find('a', string='About')
            if about_us_tag:
                about_us_url = urlparse(about_us_tag.get('href'), scheme=response.url.split('://')[0]).geturl()

                about_us_response = requests.get(about_us_url, headers=headers)
                
                if about_us_response.status_code == 200:
                    about_us_soup = BeautifulSoup(about_us_response.text, 'html.parser')
                    about_us_content = about_us_soup.get_text().strip()  # Modify this line based on your needs
                    return about_us_content

    except requests.RequestException as e:
        logger.exception(f"Error while fetching the 'About Us' page: {e}")

    return None

def scrape_website(url, headers=None, scrape_about_us=False):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # Validate the URL before making the request
        parsed_url = urlparse(url)
        if not parsed_url.netloc:
            logger.warning(f'Invalid URL: {url}')
            return None
            
        if not is_top_level_domain(url):
           logger.info(f'Skipping {url} as it is not a top-level domain')
           return None

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            page_dict = {
                'title': soup.title.text.strip() if soup.title else 'No Title',
                'paragraphs': [para.text.strip() for para in soup.find_all('p')],
                'links': [anchor['href'].strip() for anchor in soup.find_all('a', href=True)],
                'images': [img['src'].strip() for img in soup.find_all('img', src=True)],
                'headings': [heading.text.strip() for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])],
                'meta_tags': {meta['name'].strip(): meta['content'].strip() for meta in soup.find_all('meta', {'name': True, 'content': True})},
                'backlinks': [link['href'].strip() for link in soup.find_all('link', {'rel': 'stylesheet'})],
                'dom_elements': [serialize(element) for element in soup.find_all()],
                'forms': [form['action'].strip() for form in soup.find_all('form', action=True)],
                'tables': [table.text.strip() for table in soup.find_all('table')],
            }

            if scrape_about_us:
                about_us_content = scrape_about_us_page(url, headers)
                if about_us_content:
                    page_dict['about_us_content'] = about_us_content

            return page_dict

        else:
            logger.error(f'Error: Unable to fetch the content from {url}. Status code: {response.status_code}')
            return None

    except requests.RequestException as e:
        logger.exception(f"Error while fetching {url}: {e}")
        return None

def get_domain_name(url):
    return urlparse(url).netloc

# MySQL connectivity
host = 'localhost'
database = 'crawling'
user = 'root'
password = 'oracle'

try:
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        unix_socket="/var/run/mysqld/mysqld.sock",
        auth_plugin='mysql_native_password'
    )

    if connection.is_connected():
        print("Connected to MySQL database")
        
        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        # Specify the table where URLs are stored
        table_name = 'websites'

        # Specify the limit on how many URLs to scrape
        scrape_limit = 1

        # Retrieve URLs from the MySQL table with the specified limit, ordered by 'id'
        select_query = f"SELECT id, url FROM websites WHERE id >= 2 ORDER BY id LIMIT {scrape_limit}"
        cursor.execute(select_query)
        rows = cursor.fetchall()

        for row in rows:
            website_id, url = row
            # Call the scrape_website_with_retry function
            scraped_data = scrape_website(url, scrape_about_us=True)

            # Do something with the extracted data (replace this with your own logic)
            if scraped_data:
                # Escape single quotes in the description
                escaped_description = json.dumps(scraped_data).replace("'", r"\'")

                # Check if the URL already exists in the table
                check_query = f"SELECT id FROM websites WHERE url = '{url}'"
                cursor.execute(check_query)
                existing_entry = cursor.fetchone()

                if existing_entry:
                    # If the URL already exists, update the existing entry
                    update_query = f"""
                        UPDATE websites
                        SET description = '{escaped_description}'
                        WHERE id = {website_id}
                    """
                    cursor.execute(update_query)
                    print(f'Successfully updated data for {url} in MySQL table websites')
                else:
                    # If the URL doesn't exist, insert a new entry
                    insert_query = f"""
                        INSERT INTO websites (url, description)
                        VALUES ('{url}', '{escaped_description}')
                    """
                    cursor.execute(insert_query)
                    print(f'Successfully inserted data for {url} into MySQL table websites')

                # Commit the changes to the database
                connection.commit()

            else:
                logger.warning(f'Scraping failed or was not allowed for {url}')
        
except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the connection in the finally block to ensure it's always closed
    if 'connection' in locals():
        connection.close()
        print("Connection closed")
