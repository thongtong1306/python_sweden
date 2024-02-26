import requests
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime
import json
import urllib.parse
from dateutil import parser
import asyncio
import aiohttp  # pip install aiohttp
import aiofile  # pip install aiofile


def download_files_from_report(url_lst):
    sema = asyncio.BoundedSemaphore(5)

    async def fetch_file(session, url_item):
        # fname = url_item["url"].split("/")[-1] #get filename from url
        async with sema:
            async with session.get(url_item["url"]) as resp:
                assert resp.status == 200
                data = await resp.read()

        async with aiofile.async_open(url_item["path"], "wb") as outfile:
            await outfile.write(data)

    async def async_main():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_file(session, url) for url in url_lst]
            await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main())
    loop.close()

def mapping_index(div, index):
    temp_index = {}
    filing_date = 1900
    class_year = ""
    yrs = []
    yr_flag = False
    # for main_div in div.find_all("div", {"class": "ra__main"}):
    for span in div.find("span", {"class": "date-display-single"}):
        print(span.text)
        filing_date = parser.parse(span.text)
        temp_index["filingDate"] = span.text
    for a in div.find_all("a", href=True):
        print(a)
        print(a["href"])
        temp_index["tittle"] = a.text
        for yr in range(filing_date.year - 5, filing_date.year + 5):
            if str(yr) in temp_index["tittle"]:
                class_year = yr
                yr_flag = True
                break
        if not yr_flag:
            class_year = filing_date.year
        temp_index["source"] = urllib.parse.urljoin("https://vp292.alertir.com", a["href"])

    if not class_year:
        print("Cannot classify resource:", temp_index)
        return index

    qtr = div.find("span", {"class": "ra__quarter"})
    if qtr:
        temp_index["quarter"] = qtr.get_text()
        if temp_index["quarter"] in temp_index["tittle"][:5]:
            temp_index["tittle"] = temp_index["tittle"][len(temp_index["quarter"]):]
        if class_year in index:
            if "quarter" in index[class_year]:
                if temp_index["quarter"] in index[class_year]["quarter"]:
                    index[class_year]["quarter"][temp_index["quarter"]].append(temp_index)
                else:
                    index[class_year]["quarter"][temp_index["quarter"]] = [temp_index]
            else:
                index[class_year]["quarter"] = {temp_index["quarter"]: [temp_index]}
        else:
            index[class_year] = {"quarter": {temp_index["quarter"]: [temp_index]}}
    else:
        if class_year in index:
            if "annual" in index[class_year]:
                index[class_year]["annual"].append(temp_index)
            else:
                index[class_year]["annual"] = [temp_index]
        else:
            index[class_year] = {"annual": [temp_index]}

    return index

def main():
    headers = {
        'authority': 'www.handelsbanken.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': 'shb-consent-cookies=true; _shbga=GA1.2.257574858.1708623086; adobeujs-optin=%7B%22aam%22%3Atrue%2C%22adcloud%22%3Atrue%2C%22aa%22%3Atrue%2C%22campaign%22%3Atrue%2C%22ecid%22%3Atrue%2C%22livefyre%22%3Atrue%2C%22target%22%3Atrue%2C%22mediaaa%22%3Atrue%7D; AMCVS_3899365F62CF041A0A495E92%40AdobeOrg=1; AMCV_3899365F62CF041A0A495E92%40AdobeOrg=179643557%7CMCIDTS%7C19778%7CMCMID%7C73399669258219117511365951714646210938%7CMCAAMLH-1709393698%7C3%7CMCAAMB-1709393698%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1708796098s%7CNONE%7CvVersion%7C5.5.0; _shbga_gid=GA1.2.674203811.1708788962; _gat=1; kndctr_3899365F62CF041A0A495E92_AdobeOrg_identity=CiY3MzM5OTY2OTI1ODIxOTExNzUxMTM2NTk1MTcxNDY0NjIxMDkzOFIRCNmI%5FY7dMRgBKgRTR1AzMAPwAeWqid7dMQ%3D%3D; kndctr_3899365F62CF041A0A495E92_AdobeOrg_cluster=sgp3',
        'referer': 'https://www.google.com/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36',
    }

    root_path = "./mnt"
    if not os.path.exists(root_path):
        os.makedirs(root_path)

    root_path = os.path.join(root_path, "data") #"./mnt/data"
    if not os.path.exists(root_path):
        os.makedirs(root_path)

    root_path = os.path.join(root_path, "swedenReport") #"./mnt/data/swedenReport"
    if not os.path.exists(root_path):
        os.makedirs(root_path)

    cookie_file = os.path.join(root_path, "cookies.json")
    index_file = os.path.join(root_path, "index.json")
    index_path_file = os.path.join(root_path, "index_path.json")
    files_path = os.path.join(root_path, "files")
    home_url = 'https://www.handelsbanken.com/en/investor-relations/reports-and-presentations'
    if not os.path.exists(cookie_file):
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(home_url, allow_redirects=True)
        cookies = session.cookies.get_dict()
        print(cookies, session.headers)
        json_object = json.dumps(cookies, indent=4)
        with open(cookie_file, "w") as outfile:
            outfile.write(json_object)
    else:
        with open(cookie_file, "r") as io:
            cookies = json.load(io)
        response = requests.get(home_url, cookies=cookies, headers=headers)

    if response.status_code != 200: # Reset session cookies if fail
        session = requests.Session()
        response = session.get(home_url)
        cookies = session.cookies.get_dict()
        json_object = json.dumps(cookies, indent=4)
        with open(cookie_file, "w") as outfile:
            outfile.write(json_object)

    # print(response.text)
    url_lst = []
    index = {} # "annual": {}, "quarter": {}
    soup = BeautifulSoup(response.text, "lxml")
    for iframe in soup.find_all("iframe"):
        src = iframe.attrs['src']
        while True:
            res = requests.get(src, headers=headers, cookies=cookies)
            iframe_soup = BeautifulSoup(res.text, "lxml")
            # print(iframe_soup.text)
            for div in iframe_soup.find_all("div", {"class": "ra__main"}):
                index = mapping_index(div, index)
            next_page = iframe_soup.find("li", {"class": "pager-next"})
            if next_page:
                src = urllib.parse.urljoin("https://vp292.alertir.com", next_page.find("a", href=True)['href'])
            else:
                break
    json_index = json.dumps(index, indent=4)
    with open(index_file, "w") as outfile:
        outfile.write(json_index)

    # TODO: download pdf and scan
    for key1 in index:
        key1_path = os.path.join(files_path, str(key1))
        os.makedirs(key1_path, exist_ok=True)
        if "annual" in index[key1]:
            temp_path = os.path.join(key1_path, "annual")
            os.makedirs(temp_path, exist_ok=True)
            for i in range(len(index[key1]["annual"])):
                index[key1]["annual"][i]["path"] = os.path.join(temp_path, index[key1]["annual"][i]["source"].split("/")[-1])
                url_lst.append({"url": index[key1]["annual"][i]["source"], "path": index[key1]["annual"][i]["path"]})
        if "quarter" in index[key1]:
            temp_path = os.path.join(key1_path, "quarter")
            os.makedirs(temp_path, exist_ok=True)
            for key2 in index[key1]["quarter"]:
                key2_path = os.path.join(temp_path, key2)
                os.makedirs(key2_path, exist_ok=True)
                for i in range(len(index[key1]["quarter"][key2])):
                    index[key1]["quarter"][key2][i]["path"] = os.path.join(key2_path,index[key1]["quarter"][key2][i]["source"].split("/")[-1])
                    url_lst.append({"url": index[key1]["quarter"][key2][i]["source"], "path": index[key1]["quarter"][key2][i]["path"]})
    json_index = json.dumps(index, indent=4)
    with open(index_path_file, "w") as outfile:
        outfile.write(json_index)

    download_files_from_report(url_lst)

    return

if __name__ == '__main__':
    tic = time.perf_counter()
    main()
    print("Run on {}. Elapsed time is {} seconds.".format(datetime.utcnow().strftime("%Y%m%d %H:%M:%S"),
                                                          time.perf_counter() - tic))