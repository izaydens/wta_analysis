import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import concurrent.futures
import time

def extract_html(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html5lib')
    time.sleep(0.25)
    return soup, soup.prettify()

def extract_links(html):
    hike_link_pattern = r'class="listitem-title" href="([\w:\/.-]*)"'
    hike_link_matcher = re.compile(hike_link_pattern)
    hike_links = hike_link_matcher.findall(html)
    return hike_links

def scroll_through_links(soup):
    next_page = soup.find("li", {"class": "next"})
    if next_page:
        next_url = next_page.find("a", href=True)
        if next_url:
            url = next_url['href']
            return url
    return

def collect_links(url, max_pages = None):
    if max_pages:
        assert isinstance(max_pages, int), 'max_pages needs to be an integer'
        assert max_pages >= 1, 'max_pages needs to be >= 1'
    
    links = []
    current_page = 1
    while url:
        print('Collecting URLs from {}'.format(url))
        soup, html = extract_html(url)
        links.extend(extract_links(html))
        
        current_page += 1
        if not max_pages or (current_page <= max_pages):
            url = scroll_through_links(soup)
        else:
            break
        
    return list(set(links))

def regex_finder(html, pattern):
    matcher = re.compile(pattern)
    matches = matcher.findall(html)
    if matches:
        return matches[0]
    else:
        return

def record_features(soup):
    feature_list = []
    features = soup.findAll("div", {"class": "feature"})
    if features:
        feature_list = [f['data-title'] for f in features]
        return feature_list
    else:
        return

def collect_hiking_details(url):
    soup, html = extract_html(url)
    name = regex_finder(html, r'documentFirstHeading".\n\s*([-\w\s.\':]*)\n')
    location = regex_finder(html, r'Location[\\n\s]*<\/h4>[\\n\s]*<div>[\\n\s]*([-\w\s.\/\']*)\n')
    distance = regex_finder(html, r'distance["<>\s\\nspan]*([\d.]*)')
    hike_type = regex_finder(html, r'distance["<>\s\\nspan]*[\d.]*\smiles,\s([-\w]*)\n')
    if hike_type not in ['one-way', 'roundtrip']:
        hike_type = regex_finder(html, r'distance["<>\s\\nspan]*[\d.]*\smiles\s([-\w\s]*)\n')
    gain = regex_finder(html, r'Gain:[\\n\s*<span>]*([\d.]*)\n')
    highest_point = regex_finder(html, r'Point:[\\n\s<>span]*([\d.]*)')
    current_rating = regex_finder(html, r'current-rating["\s\w=:.%>]*\n\s*([\d.]*)\sout')
    rating_count = regex_finder(html, r'rating-count[">\\n\s(]*(\d*)')
    parking_pass_entry_fee = regex_finder(html, r'Entry\sFee\n\s*<\/h4>\n\s*<[\w\s=":\/.-]*>\n\s*([-\w\s]*)\n')
    permit = regex_finder(html, r'Permits\sRequired\n\s*<\/h4>\n\s*<[-\w="\s:\/.?]*>\n\s*([\w\s.()]*)\n')
    latlong = regex_finder(html, r'Co-ordinates:[\\n\s*<span>]*([\d.]*)[\\n<>\s\/\w,]*([-\d.]*)')
    if not latlong:
        latlong = [None, None]

    hike_dict = {
        'name': name,
        'link': url,
        'location': location,
        'distance': distance, 
        'hike_type': hike_type,
        'gain': gain,
        'highest_point': highest_point,
        'current_rating': current_rating,
        'rating_count': rating_count, 
        'parking_pass/entry_fee': parking_pass_entry_fee,
        'permit': permit,
        'latitude': latlong[0],
        'longitude': latlong[1],
        'features': record_features(soup)
    }
    
    return hike_dict

def create_hiking_csv(hiking_links):
    # assert isinstance(hiking_links, list), "Argument must be a list of hiking links"
    hiking_collection = []
    counter = 1
    for url in hiking_links:
        if counter%10 == 0:
            print('Working on {} of {}'.format(counter, len(hiking_links)))
        hiking_collection.append(collect_hiking_details(url))
        counter += 1
    pd.DataFrame(hiking_collection).to_csv('WTA_Hiking_1.csv')
    return hiking_collection

def create_hiking_csv_2(hiking_links):
    MAX_THREADS = 100
    # assert isinstance(hiking_links, list), "Argument must be a list of hiking links"
    # hiking_collection = []
    counter = 1
    threads = min(MAX_THREADS, len(hiking_links))
    print(threads)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        hiking_collection = executor.map(collect_hiking_details, hiking_links)
    # print(list(hiking_collection))
    df = pd.DataFrame(list(hiking_collection))
    print(df)
    df.to_csv('WTA_Hiking_1.csv')
    return hiking_collection