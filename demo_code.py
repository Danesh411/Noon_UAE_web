import os
import json
import time
import hashlib
import pandas as pd
from pathlib import Path
from parsel import Selector
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor

# Global variables
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Noon_UAE_feasiblity"
COLLECTION_INPUT = "sitemaps_inputs"
COLLECTION_OUTPUT = "sitemaps_outputs"
PAGESAVE_PATH = Path("D:/Sharma Danesh/Pagesave/Noon_UAE_feasiblity/product_page_data/21_07_2025")
PAGESAVE_PATH.mkdir(parents=True, exist_ok=True)
results_list = []

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'cookie': 'visitor_id=668cc970-0baa-4ea5-8470-304c0532181a; nloc=en-ae; visitorId=12c027bc-403d-4b18-8aca-0a1e6fd100f7; ZLD887450000000002180avuid=69fdb243-3025-470d-a32c-ffdbbf28d0fc; x-whoami-headers=eyJ4LWxhdCI6IjI1MTk5ODQ5NSIsIngtbG5nIjoiNTUyNzE1OTg1IiwieC1hYnkiOiJ7XCJwb192Mi5lbmFibGVkXCI6MSxcImlwbF9lbnRyeXBvaW50LmVuYWJsZWRcIjoxLFwid2ViX3BscF9wZHBfcmV2YW1wLmVuYWJsZWRcIjoxLFwiY2F0ZWdvcnlfYmVzdF9zZWxsZXIuZW5hYmxlZFwiOjF9IiwieC1lY29tLXpvbmVjb2RlIjoiQUVfRFhCLVM1IiwieC1hYi10ZXN0IjpbNjEsOTAxLDk0MSw5NjEsMTAzMSwxMDkwLDExMDEsMTIxMSwxMjUwLDEzMDAsMTMzMSwxMzQyLDEzNzEsMTQxMywxNDIwLDE0NzAsMTUwMiwxNTQxLDE1ODAsMTYyMSwxNjUwLDE2ODFdLCJ4LXJvY2tldC16b25lY29kZSI6IlcwMDA2ODc2NUEiLCJ4LXJvY2tldC1lbmFibGVkIjp0cnVlLCJ4LWJvcmRlci1lbmFibGVkIjp0cnVlfQ%3D%3D; _gcl_au=1.1.330145660.1752672992; _uetsid=e320d670624911f09c581bb6872f63c4; _uetvid=e320eee0624911f0b2b81bd0bf06f6af; _fbp=fb.1.1752672993755.971521475232449673; _tt_enable_cookie=1; _ttp=01K09Q721G54Q8YJ8MHQN1GAR7_.tt.1; _pin_unauth=dWlkPU5UUTJZVEV4TTJVdFpqSm1ZUzAwWmpVMUxUbGxNVFV0TVRRME56azVaVGt4TldFNA; nguestv2=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJraWQiOiJhNjczMTdhZjM0OGE0Zjk0ODYzZTU4MTU1MjQwNTYwYyIsImlhdCI6MTc1MjcyOTk2NSwiZXhwIjoxNzUyNzMwMjY1fQ.cjZph3XziPtsa1nzZnxkzpHCpGvHqZWNOmlO_s6qkrw; AKA_A2=A; _etc=ozSGPBN4GBWBXPcH; nguestv2=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJraWQiOiJjYWZiOTQ3ZGZiNzc0NjUyYjhiY2IyNGI4NDFiN2Y0NSIsImlhdCI6MTc1MjcyOTk2OCwiZXhwIjoxNzUyNzMwMjY4fQ.e7I3FSRCTStkds4qlLAtIG6PtGjZpfyEurI-T6FIyzs; ak_bmsc=40E224BD368E6788C3C6EC90A5F66DBE~000000000000000000000000000000~YAAQC4R7XEc4LhSYAQAA2eHYFhzcRdzeJtSpPSiHDObR+Ak9NFwRA0OXdPOaWpo3T93SMREgp1a45bqwrMg2CK2klTR7KsVR/ZW1yCEqPFvd9/nNAgAEJyF836sTYlyI4oqsaIcqpaGQIQEDnAVhDXRG1QhDNXwfBOpOMQmD5gmdYWDscuOr3HNUt+arXINBHGG49cot0LL0ZCdcdoPCCleUwauf04ip2dkG5LHRwCe74rEHpwyMd6F3zJK7KCJ+/hGHaFk/sq/k24IUQ7KOEibZycL/KIeviD8rKZcofYJdRpGskCI6Iz9ckHlNEZ9p1ijKCZopqQrO0WQobMRXB/+Y9U1hwn1u43QdM9FvdJL2oxzXIU7GBu6xbcTaC4JxgVXjBuH8bLIzXwTL0XAjekfJ/ubQAcpCaq/VavSXkGpyvTLT5aN/0ejGTYhXFMCxolAINILj3RvQ3g==; bm_sv=20629C43B2D7670213D918F0A868F802~YAAQC4R7XFg4LhSYAQAAQOLYFhx3bpMR5oeemgSeY0hrJJunBhdBuYgUXJNGz3zl+9mu4ihv0h9sVv/C6S8zLr/gAvyEn/Nu1PfYoZjrixMy2FPOz2jeaG+MKqX+gPg0RQsplScNvdtSXpUmImRB/6IpTWAvvExiQXGNbG5IdgsFl3BuIVcr3ceYq2GD6Sb4TmXqTkcUnDJ6H99LpkdD+sDD1GgSVVVtULkhWc3mn7RfoMwAkQ5dPely61et6A==~1; _scid=bCO-z0yS-pKdmAyW73S9SEc4ek9vqCJU; _scid_r=bCO-z0yS-pKdmAyW73S9SEc4ek9vqCJU; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22oiaVDcBgjYZ6mfhdhwPS%22%2C%22expiryDate%22%3A%222026-07-17T05%3A26%3A10.126Z%22%7D; _ym_uid=1752729970999757519; _ym_d=1752729970; _ScCbts=%5B%22626%3Bchrome.2%3A2%3A5%22%5D; _clck=1y044rt%7C2%7Cfxo%7C0%7C2023; ttcsid_CFED02JC77U7HEM9PC8G=1752729969995::VXdv-m5DYxUkT1QGuwGw.2.1752730015740; ttcsid=1752729969996::lEAuvrJgHlhE8DVfg7vh.2.1752730015740; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3Anull%2C%22expiryDate%22%3A%222026-07-17T05%3A26%3A55.861Z%22%7D; _clsk=utc9ss%7C1752730016601%7C2%7C0%7Cl.clarity.ms%2Fcollect; noonengr-_zldp=dbw5UOFoeCyNLdmkfp51cZotnya0NgRNiFBrtjtE8RRqDZa%252ByjYyd5lOiDEkvBCmmKwM1K1ctjo%253D; ZLDsiq663f5c2580454f7ed6b7bbe12e575c5570eb9b21832ce32b902ca6cbca6ffc2bavuid=69fdb243-3025-470d-a32c-ffdbbf28d0fc; ZLDsiq3b3ce696144e42ab351af48092266ce3dda2b3c7b2ad6e09ba5d18504de03180tabowner=undefined; RT="z=1&dm=noon.com&si=qy91nidj7ei&ss=md6y5zn5&sl=1&tt=hbu&ld=hbx&nu=2400f1297a27ce661c35940f7ba1d940&cl=121n&ul=2jwd"',
}


def fetch_or_load(url):
    hashed_id = hashlib.sha256(url.encode()).hexdigest()
    file_path = PAGESAVE_PATH / f"{hashed_id}.html"

    if file_path.exists():
        return file_path.read_text(encoding='utf-8')

    for _ in range(3):
        try:
            response = requests.get(url, headers=headers, impersonate="chrome120")
            if response.status_code == 200 and "ProductTitle_title" in response.text:
                file_path.write_text(response.text, encoding="utf-8")
                return response.text
        except Exception as e:
            print(f"Fetch error: {e}")
        time.sleep(2)
    return ""


def extract_group_urls(html):
    try:
        selector = Selector(text=html)

        common_json_contain = selector.xpath('//script[(contains(text(),"7:[[["))]/text()').get()
        common_json_contain1 = common_json_contain.encode('utf-8').decode('unicode_escape')
        final_common_json_contain_test = common_json_contain.split('push(')[-1].strip('()')
        final_common_json_contain = final_common_json_contain_test.split("[1,")[-1].strip("[]")
        json_loading_content_check = json.loads(final_common_json_contain)
        av = (str(json_loading_content_check)).replace("7:", "")
        json_loading_content_check = json.loads(av)
        groups = json_loading_content_check[1][3]['data']['catalogData']['catalog']['product'].get('groups', [])
        group_urls = []

        for group in groups:
            for opt in group.get("options", []):
                sku = opt.get("sku")
                offer = opt.get("offer_code")
                slug = opt.get("url")
                group_urls.append(f"https://www.noon.com/uae-en/{slug}/{sku}/p/?o={offer}")
        return group_urls
    except Exception as e:
        print(f"Group URL extraction error: {e}")
        return []


def extract_variant_urls(url, html):
    try:
        selector = Selector(text=html)

        common_json_contain = selector.xpath('//script[(contains(text(),"7:[[["))]/text()').get()
        common_json_contain1 = common_json_contain.encode('utf-8').decode('unicode_escape')
        final_common_json_contain_test = common_json_contain.split('push(')[-1].strip('()')
        final_common_json_contain = final_common_json_contain_test.split("[1,")[-1].strip("[]")
        json_loading_content_check = json.loads(final_common_json_contain)
        av = (str(json_loading_content_check)).replace("7:", "")
        json_loading_content_check = json.loads(av)
        variants = json_loading_content_check[1][3]['data']['catalogData']['catalog']['product'].get('variants', [])

        variant_urls = []
        for variant in variants:
            sku = variant.get("sku")
            spliting_urls_v = url.split("?o=")[0]
            variant_urls.append(f"{spliting_urls_v}?o={sku}")
        return variant_urls
    except Exception as e:
        print(f"Variant extraction error: {e}")
        return []


def extract_product_data(url, parent_sku):
    html = fetch_or_load(url)
    if not html:
        return

    selector = Selector(text=html)
    item = {}

    item['product_url'] = url
    item['product_sku'] = url.split("?o=")[-1].split("&")[0]
    item['product_id'] = url.split("/p/")[0].split("/")[-1]
    item['Parent'] = "Yes" if parent_sku.lower() == item['product_sku'].lower() else "No"

    try:
        item['product_name'] = selector.xpath('//span[contains(@class,"ProductTitle_title")]//text()').get(default="N/A").strip()
        item['product_image_url'] = "|".join(selector.xpath('//div[contains(@class,"GalleryV2_outerThumbnailsWrapper")]//img/@src').getall())
        item['description'] = selector.xpath('//*[contains(@class,"OverviewDescription_overviewDesc")]//text()').get(default="N/A").strip()
        item['price'] = selector.xpath('//span[contains(@class,"PriceOfferV2_priceNowText")]/text()').get(default="N/A")
        item['discounted_price'] = selector.xpath('//div[contains(@class,"PriceOfferV2_priceWasText")]/span/following-sibling::span/text()').get(default="N/A")
        item['discount_percentage'] = selector.xpath('//span[contains(@class,"PriceOfferV2_profit")]/text()').get(default="N/A")
        item['variation_id'] = item['product_id']
    except Exception as e:
        print(f"Data extraction error for {url}: {e}")

    results_list.append(item)


def process_group_variant(url, parent_sku):
    html = fetch_or_load(url)
    if not html:
        return

    variant_urls = extract_variant_urls(url, html)
    if variant_urls:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for variant_url in set(variant_urls):
                executor.submit(extract_product_data, variant_url, parent_sku)
    else:
        extract_product_data(url, parent_sku)


def process_task(main_url):
    parent_sku = main_url.split("?o=")[-1].split("&")[0]
    html = fetch_or_load(main_url)
    if not html:
        return

    group_urls = extract_group_urls(html)
    if group_urls:
        with ThreadPoolExecutor(max_workers=5) as executor:
            for group_url in set(group_urls):
                executor.submit(process_group_variant, group_url, parent_sku)
    else:
        extract_product_data(main_url, parent_sku)


def main():
    urls = [
        "https://www.noon.com/uae-en/elegant-button-detail-dress-green-work-casual-dress-cocktail-swing-dress/Z16410431289BFC481314Z/p/?o=z16410431289bfc481314z-2"
    ]

    with ThreadPoolExecutor(max_workers=len(urls)) as executor:
        executor.map(process_task, urls)

    print("All tasks completed.")

    df = pd.DataFrame(results_list)
    output_path = "Noon_Uae_sample_21_07_2025.xlsx"
    df.to_excel(output_path, index=False)
    print(f"Excel file saved to {output_path}")


if __name__ == '__main__':
    main()
