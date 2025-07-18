import pandas as pd
import os, json, pymongo, threading, time, hashlib
import datetime
from curl_cffi import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from parsel import Selector
import re

results_list = []

#TODO:: Mongo Connection string
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "Noon_UAE_feasiblity"
COLLECTION_INPUT = "sitemaps_inputs"
COLLECTION_OUTPUT = "sitemaps_outputs"

#TODO:: Pagesave conection path
try:
    PAGESAVE_PATH = Path("D:/Sharma Danesh/Pagesave/Noon_UAE_feasiblity/product_page_data/17_07_2025")
    PAGESAVE_PATH.mkdir(parents=True, exist_ok=True)
except Exception as e:
    print(e)


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


def pagesave_portion(join_path, fetch_Product_URL):
    try:
        attempts = 0
        max_attempts = 3
        my_selector = ''

        while attempts < max_attempts and not my_selector:
            try:
                url = fetch_Product_URL
                # token = ""
                # scrape_do_url = f'http://api.scrape.do?token={token}&url={urllib.parse.quote(url)}&customHeaders=true&super=true'
                response = requests.get(url=url,
                                        headers=headers,
                                        impersonate="chrome120"
                                        )

                if response.status_code == 200 and ("ProductTitle_title" in response.text and "PriceOfferV2_priceNowText" in response.text):
                    try:
                        with open(join_path, "w", encoding="utf-8") as file:
                            file.write(response.text)
                        my_selector = response.text
                    except Exception as e:
                        print(f"File write error: {e}")
            except Exception as e:
                print(f"Request error: {e}")
            attempts += 1
            if not my_selector:
                time.sleep(2)
        return my_selector
    except Exception as e:
        return ""



def process_item(url):
    Product_hashed_id = hashlib.sha256(url.encode()).hexdigest()

    page_name = f"{Product_hashed_id}.html"
    join_path = PAGESAVE_PATH / page_name

    if os.path.exists(join_path):
        my_selector = open(join_path, "r", encoding="utf-8").read()
    else:
        my_selector = pagesave_portion(join_path, url)

    if my_selector:
        my_content = Selector(text=my_selector)

        item = {}
        split_id_url = url.split("/p/")[0].split("/")[-1]
        item['product_id'] = split_id_url
        item['product_url'] = url



        # Product Name
        try:
            product_name_check = my_content.xpath('//span[contains(@class,"ProductTitle_title")]//text()').get()
            item['product_name'] = product_name_check.strip() if product_name_check else "N/A"
        except:
            ...

        # Product Image url
        try:
            all_img_contain = my_content.xpath(
                '//div[contains(@class,"GalleryV2_outerThumbnailsWrapper")]//button[contains(@id,"thumbnail-slide")]//img/@src').getall()
            item['product_image_url'] = "|".join(all_img_contain) if all_img_contain else "N/A"
        except:
            ...

        try:
            description = my_content.xpath('//*[contains(@class,"OverviewDescription_overviewDesc")]//text()').get()
            item['description'] = description.strip() if description else "N/A"
        except:
            ...

        # Price // # Discounted Price
        try:
            Price_1 = my_content.xpath('//span[contains(@class,"PriceOfferV2_priceNowText")]//text()').get()
            Price_2 = my_content.xpath(
                '//div[contains(@class,"PriceOfferV2_priceWasText")]/span/following-sibling::span/text()').get()

            if Price_1 and Price_2:
                if Price_1 < Price_2:
                    item['price'] = Price_1
                    item['discounted_price'] = Price_2
                elif Price_1 < Price_2:
                    item['price'] = Price_2
                    item['discounted_price'] = Price_1
                elif Price_1 == Price_2:
                    item['price'] = Price_1
                    item['discounted_price'] = "N/A"
            elif Price_1:
                item['price'] = Price_1
                item['discounted_price'] = "N/A"
            elif Price_2:
                item['price'] = Price_2
                item['discounted_price'] = "N/A"
            else:
                item['price'] = "N/A"
                item['discounted_price'] = "N/A"
        except:
            ...

        # Discount Percentage
        try:
            discount_percentage = my_content.xpath('//span[contains(@class,"PriceOfferV2_profit")]//text()').get()
            item['discount_percentage'] = discount_percentage if discount_percentage else "N/A"
        except:
            ...

        # Variation ID
        try:
            variation_id = split_id_url
            item['variation_id'] = variation_id
        except:
            ...

        results_list.append(item)
        print("Appended ......")


def process_task(url):
    fetch_Product_URL = url
    # fetch_Product_URL =

    split_id_url = fetch_Product_URL.split("/p/")[0].split("/")[-1]

    Product_hashed_id = hashlib.sha256(fetch_Product_URL.encode()).hexdigest()

    page_name = f"{Product_hashed_id}.html"
    join_path = PAGESAVE_PATH / page_name

    if os.path.exists(join_path):
        my_selector = open(join_path, "r", encoding="utf-8").read()
    else:
        my_selector = pagesave_portion(join_path, fetch_Product_URL)

    if my_selector:
        my_content = Selector(text=my_selector)
        item ={}


        common_json_contain = my_content.xpath('//script[(contains(text(),"7:[[["))]/text()').get()
        common_json_contain1 = common_json_contain.encode('utf-8').decode('unicode_escape')
        final_common_json_contain_test = common_json_contain.split('push(')[-1].strip('()')
        final_common_json_contain = final_common_json_contain_test.split("[1,")[-1].strip("[]")
        json_loading_content_check = json.loads(final_common_json_contain)
        av = (str(json_loading_content_check)).replace("7:", "")
        json_loading_content_check = json.loads(av)

        variantion_urls = []

        checking_groups = json_loading_content_check[1][3]['data']['catalogData']['catalog']['product']['groups']
        for sub_options in checking_groups:
            sub_option = sub_options.get("options")
            for checking_groups_list in sub_option:
                content_sku_insert_check = checking_groups_list.get("sku")
                content_urls_insert_check = checking_groups_list.get("url")
                generated_url = f"https://www.noon.com/uae-en/{content_urls_insert_check}/{content_sku_insert_check}/p/"
                variantion_urls.append(generated_url)

        if not variantion_urls:
            item['product_id'] = split_id_url
            item['product_url'] = fetch_Product_URL

            # Product Name
            try:
                product_name_check = my_content.xpath('//span[contains(@class,"ProductTitle_title")]//text()').get()
                item['product_name'] = product_name_check.strip() if product_name_check else "N/A"
            except:...

            # Product Image url
            try:
                all_img_contain = my_content.xpath('//div[contains(@class,"GalleryV2_outerThumbnailsWrapper")]//button[contains(@id,"thumbnail-slide")]//img/@src').getall()
                item['product_image_url'] = "|".join(all_img_contain) if all_img_contain else ""
            except:...

            try:
                description = my_content.xpath('//*[contains(@class,"OverviewDescription_overviewDesc")]//text()').get()
                item['description'] = description.strip() if description else "N/A"
            except:...

            # Price // # Discounted Price
            try:
                Price_1 = my_content.xpath('//span[contains(@class,"PriceOfferV2_priceNowText")]//text()').get()
                Price_2 = my_content.xpath('//div[contains(@class,"PriceOfferV2_priceWasText")]/span/following-sibling::span/text()').get()

                if Price_1 and Price_2:
                    if Price_1 < Price_2:
                        item['price'] = Price_1
                        item['discounted_price'] =Price_2
                    elif Price_1 < Price_2:
                        item['price'] = Price_2
                        item['discounted_price'] = Price_1
                    elif Price_1 == Price_2:
                        item['price'] = Price_1
                        item['discounted_price'] = "N/A"
                elif Price_1:
                    item['price'] = Price_1
                    item['discounted_price'] = "N/A"
                elif Price_2:
                    item['price'] = Price_2
                    item['discounted_price'] = "N/A"
                else:
                    item['price'] = "N/A"
                    item['discounted_price'] = "N/A"
            except:...

            # Discount Percentage
            try:
                discount_percentage = my_content.xpath('//span[contains(@class,"PriceOfferV2_profit")]//text()').get()
                item['discount_percentage'] = discount_percentage if discount_percentage else "N/A"
            except:...

            # Variation ID
            try:
                variation_id = split_id_url
                item['variation_id'] = variation_id
            except:...

            results_list.append(item)
            print("Appended ......")
        else:
            with ThreadPoolExecutor(max_workers=len(set(variantion_urls))) as executor:
                results = list(executor.map(process_item, set(variantion_urls)))




MAX_THREADS = 1
def main():
    urls = ["https://www.noon.com/uae-en/matilde-vicenzi-vicenzovo-ladyfingers/Z1176F025FF4CBC1D4259Z/p/",
"https://www.noon.com/uae-en/tiffany-delights-chocolate-chip-cookies-4x90g/ZE466A973417E619CEB96Z/p/",
"https://www.noon.com/uae-en/dates-with-pistachio-200grams/N32146846A/p/",
"https://www.noon.com/uae-en/biscuits-250grams/N12279716A/p/",
"https://www.noon.com/uae-en/dates-with-pistachio-400g/Z870874D9EB80A433B52AZ/p/",
"https://www.noon.com/uae-en/dates-with-almond-gift-box-275grams/N32146836A/p/",
"https://www.noon.com/uae-en/pumpkin-seeds-kernel-200grams/N12278713A/p/",
"https://www.noon.com/uae-en/sukkary-dates-500grams/N12278833A/p/",
"https://www.noon.com/uae-en/raw-almonds-1kg/N42469318A/p/",
"https://www.noon.com/uae-en/organic-khidri-dates-700grams/N32146833A/p/",
"https://www.noon.com/uae-en/dry-roast-mix-nuts-450grams/N27682110A/p/",
"https://www.noon.com/uae-en/crispy-seaweed-snacks-5grams-pack-of-3/N27679911A/p/",
"https://www.noon.com/uae-en/salad-chips-15grams-pack-of-25/N25260140A/p/",
"https://www.noon.com/uae-en/flamin-hot-flavored-potato-chips-184-2g/ZFB4150B0A168BC33FFA3Z/p/",
"https://www.noon.com/uae-en/american-style-cream-and-onion-flavour-48grams/N29010575A/p/",
"https://www.noon.com/uae-en/potato-flavored-chips-200grams/N24645199A/p/",
"https://www.noon.com/uae-en/chips-15grams-pack-of-25/N12280280A/p/",
"https://www.noon.com/uae-en/mixed-vegetable-chips-75grams/N13599837A/p/",
"https://www.noon.com/uae-en/jumbo-almonds-400grams/N12278772A/p/",
"https://www.noon.com/uae-en/premium-ajwa-al-madina-dates-400g/ZA01DFBEB80FE09AE2F24Z/p/",
"https://www.noon.com/uae-en/tiffany-cookie-monsta-mini-chocolate-chip-cookies-rainbow/ZCF60979CD8ECC730A7DDZ/p/",
"https://www.noon.com/uae-en/sweet-chili-chips-48grams/N12279172A/p/",
"https://www.noon.com/uae-en/crunchy-flaming-hot-chips-210grams/N12279009A/p/",
"https://www.noon.com/uae-en/american-style-cream-and-onion-chips-indian-import-82grams/N52093662A/p/",
"https://www.noon.com/uae-en/snacks-oriental-super-range-m-60grams/N13346921A/p/",
"https://www.noon.com/uae-en/original-36-8g-12-pack-of-12/N25260125A/p/",
"https://www.noon.com/uae-en/salad-chips-75grams/N25259719A/p/",
"https://www.noon.com/uae-en/mini-bites-quinoa-chia-crackers-250g/Z126FC88166656A839A18Z/p/",
"https://www.noon.com/uae-en/sunflower-salt-and-vinegar-150g/Z0FBC1D3AFF64F84116DFZ/p/",
"https://www.noon.com/uae-en/magic-masala-chips-indian-import-special-south-flavour-82grams/N52093661A/p/",
"https://www.noon.com/uae-en/cookies-n-chocolate-12-76grams/N36131038A/p/",
"https://www.noon.com/uae-en/sokari-dates-700g/Z85134BAF465FC98DE2FFZ/p/",
"https://www.noon.com/uae-en/masala-munch-78grams/N29010568A/p/",
"https://www.noon.com/uae-en/walnut-500grams/N42469321A/p/",
"https://www.noon.com/uae-en/salted-pistachios-150grams/N27682087A/p/",
"https://www.noon.com/uae-en/extra-mix-300grams/N39891270A/p/",
"https://www.noon.com/uae-en/glucose-biscuit-pack-of-27/Z4D172332C90D81F82637Z/p/",
"https://www.noon.com/uae-en/milk-and-chocolate-36-8g-12-pack-of-12/N25264002A/p/",
"https://www.noon.com/uae-en/walnut-jumbo-400grams/N12278817A/p/",
"https://www.noon.com/uae-en/spanish-tomato-tango-potato-chips-indian-import-48grams/N45061619A/p/",
"https://www.noon.com/uae-en/peanuts-500grams/N38698957A/p/",
"https://www.noon.com/uae-en/raw-cashew-500grams/N42469324A/p/",
"https://www.noon.com/uae-en/mild-cheddar-cheese-dip-255g/Z31005C3F208641E3F1E1Z/p/",
"https://www.noon.com/uae-en/honey-barbeque-twists-283g/Z15C3658B7AE0B667F171Z/p/",
"https://www.noon.com/uae-en/sweet-corn-rings/Z2576C54232463407405DZ/p/",
"https://www.noon.com/uae-en/khidri-dates-pouch-400g/Z512295C47F52EA1A4233Z/p/",
"https://www.noon.com/uae-en/french-cheese-potato-chips-45grams/N13422023A/p/",
"https://www.noon.com/uae-en/pistachio-kernels-jumbo-200grams/N12278757A/p/",
"https://www.noon.com/uae-en/chocolate-brownie-protein-bar/Z35365E8E812429EA1C4DZ/p/",
"https://www.noon.com/uae-en/hot-chili-baked-potato-chips-150g/Z65DC82349E397358169CZ/p/",
"https://www.noon.com/uae-en/salted-pistachios-300grams/N12278719A/p/",
"https://www.noon.com/uae-en/pure-butter-shortbread-fingers/Z77F8EAE6263CDC5F5DB8Z/p/",
"https://www.noon.com/uae-en/original-chips-70grams/N13599920A/p/",
"https://www.noon.com/uae-en/premium-chocolate-marble-pecan-dragees-165g/ZB2385D80982366882D7BZ/p/",
"https://www.noon.com/uae-en/salted-stick-cracker-30grams/N12280659A/p/",
"https://www.noon.com/uae-en/freeze-fresh-dried-fruit-mix/Z3F96181C20B324993972Z/p/",
"https://www.noon.com/uae-en/red-velvet-whey-protein-bar-70g-20g-protein/ZB582DED67E824E8CD4DEZ/p/",
"https://www.noon.com/uae-en/glucose-biscuit-pack-of-24/Z8E19D0BC4EB57706EB85Z/p/",
"https://www.noon.com/uae-en/dip-cheddar-jalapeno-255g/ZAA14634F0F22C3CE1E22Z/p/",
"https://www.noon.com/uae-en/potato-chips-original-165g/Z8388975AB48E4E2849EBZ/p/",
"https://www.noon.com/uae-en/pro-puffs-chili-lemon-50g-13g-protein/Z9C1CB2095312CA561356Z/p/",
"https://www.noon.com/uae-en/cashew-biscuits-400grams/N29171929A/p/",
"https://www.noon.com/uae-en/qrakers-cheese-25g/ZFAD212524C351FBE67B4Z/p/",
"https://www.noon.com/uae-en/flax-seed-400grams/N25264146A/p/",
"https://www.noon.com/uae-en/digestive-original-500grams/N13421966A/p/",
"https://www.noon.com/uae-en/chili-lime-protein-puffs-40g/Z06666D6BA2452DCD3762Z/p/",
"https://www.noon.com/uae-en/delights-butter-cookies-tin-405grams/N25259333A/p/",
"https://www.noon.com/uae-en/india-s-magic-masala-indian-import-48grams/N29010578A/p/",
"https://www.noon.com/uae-en/honey-mustard-baked-potato-chips-150g/Z1CC151C6743F628B9977Z/p/",
"https://www.noon.com/uae-en/freeze-fresh-dried-strawberry/Z55CE00869F54FA9964D7Z/p/",
"https://www.noon.com/uae-en/square-crisps-habanero-25g/ZEAB4B36EEE0AA41013DCZ/p/",
"https://www.noon.com/uae-en/brazil-nuts-shelled-200grams/N27683155A/p/",
"https://www.noon.com/uae-en/flamin-hot-163g/ZC4938057E6CF4F9090B8Z/p/",
"https://www.noon.com/uae-en/nachips-185grams-single/N51051929A/p/",
"https://www.noon.com/uae-en/salted-classic-mixed-nuts-300grams/N27682082A/p/",
"https://www.noon.com/uae-en/jumbo-pistachios-400grams/N12278776A/p/",
"https://www.noon.com/uae-en/french-cheese-baked-potato-chips-150g/Z2F6AA25BF77D6F155712Z/p/",
"https://www.noon.com/uae-en/peanut-peeled-400grams/N38982861A/p/",
"https://www.noon.com/uae-en/oven-baked-chips-25-x-12g/ZA8BF180E4E43C48B0987Z/p/",
"https://www.noon.com/uae-en/olive-and-oregano-bread-bites-23grams-pack-of-12/N12280556A/p/",
"https://www.noon.com/uae-en/american-harvest-chia-seeds-whole-grain-gluten-free-superfood-1kg-jar/ZC9C54407F95D976BB3D1Z/p/",
"https://www.noon.com/uae-en/classic-mix-300grams/N39891269A/p/",
"https://www.noon.com/uae-en/beef-original-jerky-40grams/N49498929A/p/",
"https://www.noon.com/uae-en/long-masala-banana-chips-170grams/N27679749A/p/",
"https://www.noon.com/uae-en/apricots-dried-jumbo-400grams/N12278700A/p/",
"https://www.noon.com/uae-en/cream-onion-makhana-90g/Z50BC21C782DF7241E519Z/p/",
"https://www.noon.com/uae-en/leibniz-zoo-original-biscuits-100grams/N12278625A/p/",
"https://www.noon.com/uae-en/salted-cashews-300grams/N27682076A/p/",
"https://www.noon.com/uae-en/cheese-onion-square-crisps-25g/ZDADF7C1D70171CAD10F3Z/p/",
"https://www.noon.com/uae-en/deglet-nour-dates-250grams/N25264147A/p/",
"https://www.noon.com/uae-en/flamin-hot-sriracha-chips-ridged-potato-crisps-45g/Z62B46C3481850C6B24E7Z/p/",
"https://www.noon.com/uae-en/organic-high-protein-gluten-free-pumpkin-seeds-340grams/N33479266A/p/",
"https://www.noon.com/uae-en/crunchy-flamin-hot-us-import-99g/Z40DB199091465406B613Z/p/",
"https://www.noon.com/uae-en/bikaji-potato-chips-cream-n-onion-40g/ZCEC62E220F5788927493Z/p/",
"https://www.noon.com/uae-en/pistachio-protein-balls-60g/ZBA024EBFE3DA1A818FABZ/p/",
"https://www.noon.com/uae-en/hazelnut-spread-with-cocoa/Z140654BAE2BA90FECA52Z/p/",
"https://www.noon.com/uae-en/caramelized-biscuits-156grams/N12279714A/p/",
"https://www.noon.com/uae-en/salted-peanuts-can-550grams/N27682069A/p/",
"https://www.noon.com/uae-en/hazelnut-fudge-whey-protein-bar-70g-21g-protein/ZA103AF10B5FCDA15983AZ/p/",
"https://www.noon.com/uae-en/kholas-dates/Z6D00C702D70E15709A60Z/p/",
]

    with ThreadPoolExecutor(max_workers=len(set(urls))) as executor:
        results = list(executor.map(process_task, set(urls)))

    # url = 'https://www.noon.com/uae-en/iphone-16-pro-max-256gb-desert-titanium-5g-with-facetime-international-version/N70106183V/p/?shareId=1b22f732-38a8-4d8b-8475-e366597e0f20'
    # client = pymongo.MongoClient(MONGO_URI)
    # db = client[DB_NAME]
    # collection_ip = db[COLLECTION_INPUT]
    # pending_tasks = list(collection_ip.find({"status": "Pending"}))
    #
    # if not pending_tasks:
    #     print("No pending tasks found.")
    #     return
    #
    # with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    #     futures = []
    #     for task in pending_tasks:
    #         url = task.get("url", "")
    #         future = executor.submit(process_task, url)
    #         futures.append(future)
    #
    #     for future in as_completed(futures):
    #         try:
    #             future.result()
    #         except Exception as e:
    #             print(f"Error processing task: {e}")

    print("All tasks completed.")

if __name__ == '__main__':
    main()

print(results_list)
df = pd.DataFrame(results_list)

# Export to Excel
output_path = "Noon_Uae_sample.xlsx"
df.to_excel(output_path, index=False)

print(f"Excel file saved to {output_path}")
