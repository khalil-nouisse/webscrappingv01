import csv
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.common.exceptions import TimeoutException
import time

# Setup Edge options
options = webdriver.EdgeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Initialize Edge driver with automatic driver management
driver = webdriver.Edge(
    service=Service(r"c:\Users\asus\Desktop\IT\X-GATE\msedgedriver.exe"),
    options=options
)
driver.set_page_load_timeout(30)  # 30 seconds timeout
# Function to scrape a single property page
def scrape_page(url):
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Initialize variables to avoid UnboundLocalError
    surface = pieces = chambres = salles_de_bains = ''
    try:
        longitude = soup.find('div', class_='blockProp mapBlockProp').find('div', class_='prop-map-holder').attrs["lon"]
    except:
        longitude = ''
    try:
        latitude = soup.find('div', class_='blockProp mapBlockProp').find('div', class_='prop-map-holder').attrs["lat"]
    except:
        latitude = ''
    try:
        title = soup.find('div', class_='mainInfoProp').h1.text.strip()
    except AttributeError:
        title = ''
    try:
        minDesc = ''
    except AttributeError:
        minDesc = ''
    try:
        city = soup.find('div', class_='mainInfoProp').find('h3', class_='greyTit').text.replace('\n','').replace('\t','')
    except AttributeError:
        city = ''
    try:
        details = soup.find_all('div', class_='adDetailFeature')

        for detail in details:
            try:
                if 'm²' in detail.text.lower():
                    surface = detail.span.text.strip()
            except AttributeError:
                surface = ''
            try:
                if 'pièces' in detail.text.lower():
                    pieces = detail.span.text.strip()
            except AttributeError:
                pieces = ''
            try:
                if 'chambres' in detail.text.lower():
                    chambres = detail.span.text.strip()
            except AttributeError:
                chambres = ''
            try:
                if 'salles de bains' in detail.text.lower():
                    salles_de_bains = detail.span.text.strip()
            except AttributeError:
                salles_de_bains = ''
    except AttributeError:
        details = []
    try:
        desc = soup.find('div', class_='blockProp').p.text.strip()  # corrected find_all -> find
    except AttributeError:
        desc = ''
    try:
        price = soup.find('div', class_='mainInfoProp').find('h3', class_='orangeTit').text.replace('\n','').replace('\t','')
    except AttributeError:
        price = ''

    # Tags
    tagProp = []
    try:
        for item in soup.find('div', class_='mainInfoProp').find_all('span', class_='tagProp'):
            tagProp.append(item.text.replace('\n','').replace('\t','').strip())
    except AttributeError:
        tagProp = []

    # Characteristics
    caracterestics = []
    try:
        for item in soup.find('div', class_='row rowIcons adFeatures inBlock w100').find_all('span', class_='characIconText centered'):
            caracterestics.append(item.text.replace('\n','').replace('\t','').strip())
    except AttributeError:
        caracterestics = []

    return (title, minDesc, city, surface, pieces, chambres, salles_de_bains, desc, tagProp, caracterestics, price, longitude, latitude, url)

# Generate listing page URL
def get_url(page_number):
    template = 'https://www.mubawab.ma/fr/sc/appartements-a-vendre:p:{}'
    return template.format(page_number)

# Extract all property links from a listing page
def get_links(url):
    try:
        driver.get(url)
    except TimeoutException:
        print(f"Timeout loading {url}")
        return []
    import time; time.sleep(3)  # Wait for JS to load
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    links = []
    for item in soup.find_all('div', class_='listingBox sPremium feat'):
        a_tag = item.find('a', href=True)
        if a_tag:
            links.append(a_tag['href'])
    print(f"Found {len(links)} links on {url}")
    return links

# Main scraping function
def main(nb_pages):
    all_links = []
    for i in range(201, 200 + nb_pages + 1):
        all_links.append(get_url(i))

    # Collect all property links
    data_links = set()
    for page in all_links:
        data_links.update(get_links(page))

    # Scrape each property
    data_list = []
    import time
    for link in data_links:
        try:
            data_list.append(list(scrape_page(link)))
            time.sleep(1)  # Add delay
        except Exception as e:
            print(f"Failed to scrape {link}: {e}")

    # Close the driver
    driver.quit()

    # Save to CSV
    df = pd.DataFrame(
        data_list, 
        columns=['title', 'minDesc', 'city','surface','pieces','chambres','salles_de_bains', 'desc', 'tagProp', 'caracterestics', 'price', 'longitude', 'latitude', 'link']
    )
    df.to_csv('MubawabOutput4.csv', index=False)
    print("Scraping finished. Data saved to MubawabOutput4.csv")

# Example: scrape first 2 pages
if __name__ == "__main__":
    main(170)
