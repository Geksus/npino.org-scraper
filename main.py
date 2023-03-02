from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from lxml import etree

from bs4 import BeautifulSoup
import requests

BASE_URL = "https://www.npino.org"

session = requests.Session()
state_codes = []
types_and_states = []
list_of_facilities = []

headers = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/54.0 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; AS; rv:11.0) like Gecko"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; AS; rv:11.0) like Gecko"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; AS; rv:11.0) like Gecko"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063"
    },
]


def types_of_facilities():
    """Return a list of all types of facilities."""
    soup = BeautifulSoup(
        session.get(
            "https://www.npino.org/organization/nursing-custodial-care-facilities",
            headers=random.choice(headers),
        ).text,
        "html.parser",
    )
    types = soup.find("div", class_="taxonomies-section").find_all("h3")
    return [BASE_URL + t.find("a")["href"] for t in types]


def get_states_by_type(url):
    """Return a list of all states."""
    soup = BeautifulSoup(
        session.get(url, headers=random.choice(headers)).text, "html.parser"
    )
    states = soup.find("div", id="state-filter-container").find_all("a")
    if len(state_codes) == 0:
        state_codes.extend([c.text for c in soup.find_all("span", class_="code")])
    types_and_states.extend([BASE_URL + s["href"] for s in states])


def get_pagination(url):
    """Return pagination."""
    soup = BeautifulSoup(
        session.get(url, headers=random.choice(headers)).text, "html.parser"
    )
    return (
        int(soup.find("li", class_="last").find("a")["href"][-1])
        if soup.find("ul", class_="pagination")
        else 1
    )


def get_facilities_urls(url):
    """Return a list of all facilities on a given page."""
    soup = BeautifulSoup(
        session.get(url, headers=random.choice(headers)).text, "html.parser"
    )
    facilities = soup.find(
        "table", class_="npi-record-list table table-hover"
    ).find_all("a")
    return [BASE_URL + f["href"] for f in facilities]


def scrape_data(url):
    response = session.get(url, headers=random.choice(headers))
    soup = BeautifulSoup(response.text, "html.parser")
    dom = etree.HTML(str(soup))
    data = {"Company Name": soup.find("h1").text.strip(), "Health care specialties": [
        ", ".join(
            s.text
            for s in soup.find("div", class_="panel panel-info npi-specialty").find_all(
                "td"
            )
        )
        for _ in soup.find("div", class_="panel panel-info npi-specialty").find_all(
            "tr"
        )
    ], "Description": "", "Address": soup.find('span', class_='address'), "City": "", "State": "", "Zip": "", "Phone": "", "Website": "",
            "Contact Name": "", "Contact phone": "", "NPI Number": "", "LBN": "", "DBA": "", "Entity Type": "",
            "Enumeration Date": "", "Last Update Date": "", "Organization Subpart": "",
            "Identifiers": {"State": "", "Type": "", "Number": "", "Issuer": ""}}
    for _ in dom.xpath('//*[@id="content"]/div/div[1]/text()'):
        if len(_) > 50:
            data["Description"] = _.strip()
    city_state_zip = soup.find("span", class_="citystate").text.split(" ")
    data["City"] = ' '.join(city_state_zip[:-2])
    data["State"] = city_state_zip[-2]
    data["Zip"] = city_state_zip[-1]
    data["Phone"] = soup.find("i", class_="fa fa-phone").text
    data["Website"] = soup.find("i", class_="fa fa-globe").text
    data["Contact Name"] = dom.xpath('//*[@id="content"]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[2]')[0].text[6:].strip()
    data["Contact phone"] = dom.xpath('//*[@id="content"]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[3]/a')[0].text
    additional_data = soup.find('div', class_='panel panel-info npi-other-info').find_all('td')
    data["NPI Number"] = additional_data[1]
    data["LBN"] = additional_data[2]
    data["DBA"] = additional_data[3]
    data["Entity Type"] = additional_data[4]
    data["Enumeration Date"] = additional_data[6]
    data["Last Update Date"] = additional_data[7]
    data["Organization Subpart"] = additional_data[5]




for url in types_of_facilities():
    get_states_by_type(url)

# Fetch data from multiple pages in parallel
start = 0
while start < len(types_and_states):
    try:
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for url in types_and_states:
                # sleep(random.randint(1, 10))
                print(url)
                for i in range(get_pagination(url)):
                    futures.append(executor.submit(session.get, f"{url}&page={i}"))
                start += 1
            for future in as_completed(futures):
                response = future.result()
                soup = BeautifulSoup(response.text, "html.parser")
                if soup.find("div", class_="error-code"):
                    break
                else:
                    list_of_facilities.extend(get_facilities_urls(response.url))
                    print(len(list_of_facilities))
    except:
        types_and_states = types_and_states[start:]
