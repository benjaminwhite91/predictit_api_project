import requests
import boto3
import polars as pl 

def predictit_scraper(url):
    print('start running')
    response = requests.request("GET",url)
    json_data = response.json()
    return json_data

def dictionary_collapser(data_dict,columns_to_include):
    reduced_dict = {field:data_dict[field] for field in columns_to_include}
    return reduced_dict

def json_parser(json_data):
    markets = json_data['markets']
    market_level_columns = ['market_id','shortName','url','timeStamp']
    contract_level_columns = ['contract_id','name','lastTradePrice','lastTradePrice','bestBuyYesCost','bestBuyNoCost','bestSellYesCost','bestSellNoCost','lastClosePrice']
    dictionary_list = []
    for market in markets:
        market['market_id'] = market['id']
        market_dict = dictionary_collapser(market,market_level_columns)
        
        for contract in market['contracts']:
            contract['contract_id'] = contract['id']
            contract_dict = dictionary_collapser(contract,contract_level_columns)
            all_data_dict = {**market_dict,**contract_dict}
            dictionary_list.append(all_data_dict)
    return dictionary_list

def json_to_csv(parsed_json):
    data_frame = pl.DataFrame(parsed_json)
    data_frame.write_csv('STORAGE LOCATION HERE',separator=',')


def s3_upload(file_name,bucket):
    s3 = boto3.client('s3')
    s3.upload_file(file_name,bucket,f"predictit/{file_name}")
    print('file uploaded')


