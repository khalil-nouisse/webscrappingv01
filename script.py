import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse, urljoin

# Define the base URL
baseurl = 'https://www.mubawab.ma/'

# Set to store unique product links
product_links = set()

num_pages = 3
# Loop through multiple pages of apartment listings
for page_num in range(1, num_pages + 1):
    # Make a GET request to the page
    response = requests.get(f'https://www.mubawab.ma/fr/sc/appartements-a-vendre:p:{page_num}')

    #parse the html
    soup = BeautifulSoup(response.content, 'html.parser')  #.content Returns the raw response body in byte with no decoding

   # Extract listings from the page
    listings = soup.find_all(class_='listingBox w100')

    # Loop through each listing and extract product links
    for listing in listings:
        links = listing.find_all('a', href=True)
        for link in links:
            product_link = link['href']

            # Check if the URL is valid
            if not urlparse(product_link).scheme:
                product_link = urljoin(baseurl, product_link)
                product_links.add(product_link)  # Add link to the set

# Initialize list to store scraped data
scraped_data = []

# Loop through each product link and scrape data
for link in product_links:
    try:
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        # Extract relevant information from the page
        # (Code snippet continued from previous section...)
        title = soup.find('h1').get_text(strip=True) if soup.find('h1') else ''
        price = soup.find(class_='price').get_text(strip=True) if soup.find(class_='price') else ''
        location = soup.find(class_='location').get_text(strip=True) if soup.find(class_='location') else ''
        # Add more fields as needed

        scraped_data.append({
            'URL': link,
            'Title': title,
            'Price': price,
            'Location': location,
            # Add more fields here
        })
    except Exception as e:
        print('Error processing page:', link)
        print(e)
        continue

# Convert scraped data to DataFrame and save to Excel
df = pd.DataFrame(scraped_data)
df.to_excel('scraped_data.xlsx', index=False)
