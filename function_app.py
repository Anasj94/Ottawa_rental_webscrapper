import logging
import azure.functions as func
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql

app = func.FunctionApp()

@app.schedule(schedule="0 0 0/12 1/1 * ? *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

@app.timer_trigger(schedule="0 0 0/12 1/1 * ? *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')



timeout=10

def get_title(soup):
    try:
        title = new_soup.find("span", attrs={'itemprop': 'address'})
        title_value = title.text
        title_string = title_value.strip()
    except AttributeError:
        title_string = ""
    return title_string

def get_price(soup):
    try:
        price = soup.find("span",attrs={'class': 'currentPrice-2842943473'})
        price_value = price.text
    except AttributeError:
        price_value = ""
    return price_value

def get_category(soup):
    try:
        category_elements = soup.find_all("span", attrs={'class': 'noLabelValue-3861810455'})
        if category_elements:
            category_value = category_elements[0].text
        else:
            category_value = ""
    except AttributeError:
        category_value = ""
    return category_value

def get_bedroom(soup):
    try:
        bedroom_elements = soup.find_all("span", attrs={'class': 'noLabelValue-3861810455'})
        if bedroom_elements:
            bedroom_value = bedroom_elements[1].text
        else:
            bedroom_value = ""
    except AttributeError:
        bedroom_value = ""
    return bedroom_value

def get_bathroom(soup):
    try:
        bathroom_elements = soup.find_all("span", attrs={'class': 'noLabelValue-3861810455'})
        if bathroom_elements:
            bathroom_value = bathroom_elements[2].text
        else:
            bathroom_value = ""
    except AttributeError:
        bathroom_value = ""
    return bathroom_value


url = "https://www.kijiji.ca/b-for-rent/ottawa-gatineau-area/c30349001l1700184"

# Header for request
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5'
}

webpage = requests.get(url, headers=HEADERS)
soup = BeautifulSoup(webpage.content, "html.parser")

# Find the links
links = soup.find_all("a",attrs={'data-testid':'listing-link','class':'sc-2e07e5ea-0 gpxjNR'})

d = {"Title": [],"Price":[],"Category":[],"Bedroom":[],"Bathroom":[],"URL":[]}

for link in links:
    new_url = "https://www.kijiji.ca" + link.get('href')
    new_webpage = requests.get(new_url, headers=HEADERS)
    new_soup = BeautifulSoup(new_webpage.content, "html.parser")
    
    d['Title'].append(get_title(new_soup))
    d['Price'].append(get_price(new_soup))
    d['Category'].append(get_category(new_soup))
    d['Bedroom'].append(get_bedroom(new_soup))
    d['Bathroom'].append(get_bathroom(new_soup))
    d['URL'].append(new_url)


#df = pd.DataFrame.from_dict(d)
#df.to_csv('data.csv',header=True,index=False)
#print(df)

# Your MySQL connection details
connection = pymysql.connect(
    charset="utf8mb4",
    connect_timeout=timeout,
    cursorclass=pymysql.cursors.DictCursor,
    db="KIJIJI",
    host="mysql-15fecc2b-anasj1801937.a.aivencloud.com",
    password="AVNS_f0kprHoNWg4LRI67ClR",
    read_timeout=timeout,
    port=22387,
    user="avnadmin",
    write_timeout=timeout,
)

# Create a cursor for executing SQL queries
cursor = connection.cursor()

# Your dictionary d contains the data you want to insert into the MySQL table

# Loop through the dictionary and insert the data into the MySQL table
for index, row in df.iterrows():
    title = row["Title"]
    price = row["Price"]
    category = row["Category"]
    bedroom = row["Bedroom"]
    bathroom = row["Bathroom"]
    url = row["URL"]

    # SQL query to insert data into the table
    insert_query = "INSERT INTO kijiji (Title, Price, Category, Bedroom, Bathroom, URL) VALUES (%s, %s, %s, %s, %s, %s)"
    values = (title, price, category, bedroom, bathroom, url)

    try:
        cursor.execute(insert_query, values)
        connection.commit()  # Commit the transaction
        print(f"Inserted data for URL: {url}")
    except pymysql.Error as e:
        print(f"Error inserting data for URL: {url}")
        print(e)

# Close the cursor and the database connection
cursor.close()
connection.close()