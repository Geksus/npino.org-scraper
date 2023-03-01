from bs4 import BeautifulSoup
import requests

BASE_URL = 'https://www.npino.org/organization/nursing-custodial-care-facilities'

session = requests.Session()
state_codes = []


def types_of_facilities():
    """Return a list of all types of facilities."""
    soup = BeautifulSoup(session.get(BASE_URL).text, 'html.parser')
    types = soup.find('div', class_='taxonomies-section').find_all('h3')
    return [BASE_URL + t.find('a')['href'] for t in types]


def get_states():
    """Return a list of all states."""
    soup = BeautifulSoup(session.get('https://www.npino.org/organization/nursing-custodial-care-facilities/alzheimer-center-dementia-center-311500000X').text, 'html.parser')
    states = soup.find('div', id='state-filter-container').find_all('a')
    state_codes.extend([c.text for c in soup.find_all('span', class_='code')])
    return [BASE_URL + s['href'] for s in states]


