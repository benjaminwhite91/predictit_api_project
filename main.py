from scraper_functions import predictit_scraper,json_parser,json_to_csv
base_url = 'https://www.predictit.org/api/marketdata/all/'

raw_data = predictit_scraper(base_url)
data_for_upload = json_parser(raw_data)
json_to_csv(data_for_upload)