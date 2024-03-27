from scraper_functions import predictit_scraper,json_parser,json_to_csv,s3_upload

base_url = 'https://www.predictit.org/api/marketdata/all/'

try:
    raw_data = predictit_scraper(base_url)
    data_for_upload = json_parser(raw_data)
    predictit_data = json_to_csv(data_for_upload)
    resp = s3_upload(predictit_data,'raw-prediction-market-data')
except:
    print('Something Broke')
