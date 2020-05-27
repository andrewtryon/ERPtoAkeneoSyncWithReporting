from datetime import date
import json
import os
import logging
import logzero
import gc
import numpy as np
import pandas as pd
import pyodbc
import subprocess
import pysftp
import json
import paramiko
import csv
import urllib
import requests
import array as arr 
import codecs, json 
from base64 import decodebytes
from pandas.io.json import json_normalize

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "**********", folderid = "**************"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': "bearer "
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer '
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response     

def get_google_link (row):
    try:
        if 'pricelist' in row['ProductUrl'] and row['IsNewStyle']:
            return r'http://www.XXXXXXXXXXXX.com/products.htm?item=' +  row['ItemCode'] + r'?ref=gbase'
        elif row['ProductUrl'] != '':
            return 'http://' + row['ProductUrl']  + r'?ref=gbase'
        else:
            return ''
    except:
        return ''

def get_google_product_type (row):
    if row['google_product_category'] is None or row['google_product_category']==np.nan:
        return np.nan     
    else:
        return row['google_product_category'].split(' >', 1)[0]

def yesno_to_truefalse (row, column_name):
    try:
        if row[column_name] == 'Y':
            return True
        elif row[column_name] == 'N':
            return False
    finally:
        return np.nan   

def make_json_association_data_nest(row, column_name):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan' or str(row[column_name]) == '':
        row[column_name] = {"products":[]}
    elif type(row[column_name]) != dict:
        if not isinstance(row[column_name], str):
            d = str(row[column_name]).split(",")
        else:
            d = row[column_name].split(",")
        row[column_name]  = {"products":d}       
    return row

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan' or str(row[column_name]) == '':
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
            #d = str(row[column_name]).encode().decode('unicode-escape')
            #d = str(row[column_name]).encode().decode('raw_unicode_escape')
        else:
            d = row[column_name].encode().decode()
            #d = row[column_name].encode().decode('unicode-escape')
            #d = row[column_name].encode().decode('raw_unicode_escape')
        if unit is not None and currency is None:
            d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row

if __name__ == '__main__':

    pd.options.display.max_colwidth = 9999

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    #Stored in .env
    AAKENEOKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL") 

    #Establish akeneo API client
    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)    

    #sku							
    attributeCols = [
        'VendorAlias',
        'Catalog',
        'UPC',
        'GTIN',
        'VendorPriceDate',
        'COO',
        'Brand',
        'ProductType',
        'Replacement',
        'freeship_excluded',
        'SageCreatedDate',
        'GsaPriceDate',
        'GsaTempPriceDate',                        
        'OnAmazonVendor',
        'OnNewEgg',
        'OnGlobal',
        'OnJet',
        'OnWalmart',
        'AmazonSKU',
        'AmazonASIN',
        'AmazonVendorDiscount',   
        'AmazonVendorPriceDate',
        'Title70',
        'Title100',
        'Title150',
        'Header',
        'webCategory1',        
        'webCategory2',
        'webCategory3',
        'TextOnlyFeatures',
        'TextOnlyDescription',
        'ImageUrl',
        'ProductUrl',
        'DatasheetUrl',
        'ManualUrl',
        'QuickstartUrl',
        'BrochureUrl',
        'VideoUrl',
        'Keywords',
        'MetaDescription',
        'MetaKeywords',
        'InformationSource',
        'TextOnlyComponents',
        'MainOrAccessory',
        'MainUnits',
        'Accessories',
        'RelatedProducts',
        'AdditionalImages',
        'Specs',    
        'DisplayName',
        'Condition',
        'ProductFamily',
        'PriceListDescription',
        'GoogleId',
        'google_description',   
        'google_product_type',
        'google_product_category',
        'google_link',
        'google_mpn'                                                    
    ]
    unitCols = [
        'product_weight',
        'ProductLength',
        'ProductWidth',
        'ProductHeight',
        'ShippingLength',      
        'ShippingWidth',
        'ShippingHeight'                                            
    ]
    currencyCols = [
        'Cost',
        'SalePrice',
        'MSRP',
        'MAP',
        'GsaTempPrice',
        'AmazonVendorPrice'                                              
    ] 
    associationCols = [
        'related',
        'accessory',
        'mainUnit'                          
    ]        
    enabledCols = [
        'enabled'                       
    ]
    idCols = [
        'identifier'                       
    ]       

    jsonCols = idCols + enabledCols + attributeCols + currencyCols + unitCols + associationCols

    logzero.loglevel(logging.WARN)

    #Connection String
    sage_conn_str = (
        r'DSN=SOTAMAS90;'
        r'UID=at;'
        r'PWD=Try93;'
        r'Directory=\\sage100\sage 100 Advanced\2018\MAS90;'
        r'Prefix=\MAS90\SY\, \\sage100\Sage 100 Advanced\2018\MAS90\==\;'
        r'ViewDLL=\\sage100\Sage 100 Advanced\2018\MAS90\HOME;'
        r'Company=FOT;'
        r'LogFile=\PVXODBC.LOG;'
        r'CacheSize=4;'
        r'DirtyReads=1;'
        r'BurstMode=1;'
        r'StripTrailingSpaces=1;'
        r'SERVER=NotTheServer;'
        )

    #Establish sage connection
    sage_cnxn = pyodbc.connect(sage_conn_str, autocommit=True)
    #SQL Sage data into dataframe
    SageSQLquery = """SELECT 
                            CI_Item.ItemCode, CI_Item.InactiveItem, IM_ItemVendor.VendorAliasItemNo, CI_Item.UDF_CATALOG_NO, CI_Item.UDF_UPC, CI_Item.UDF_GTIN14, CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER, 
                            CI_Item.StandardUnitCost, CI_Item.StandardUnitPrice, CI_Item.SuggestedRetailPrice, CI_Item.Category4,
                            CI_Item.UDF_MAP_PRICE, CI_Item.UDF_VENDOR_PRICE_DATE, CI_Item.ShipWeight, CI_Item.UDF_COUNTRY_OF_ORIGIN_TEMP, CI_Item.UDF_ECCN, 
                            CI_Item.UDF_SCHEDULE_B_NUMBER, CI_Item.UDF_PACK_QUANTITY, CI_Item.ProductLine, CI_Item.ProductType, CI_Item.UDF_REPLACEMENT_ITEM, 
                            CI_Item.UDF_ON_CLEARANCE, CI_Item.UDF_DRP_SHP_ONLY, CI_Item.UDF_SHIPPING_EXCLUSION, IM_ItemWarehouse.MinimumOrderQty, 
                            CI_Item.PrimaryVendorNo, IM_ItemWarehouse.ReorderPointQty, CI_Item.UDF_REVIEW_REQUIRED, CI_Item.UDF_RFQ, CI_Item.UDF_DISCONTINUED_STATUS, CI_Item.DateCreated, 
                            CI_Item.UDF_LOWEST_PRICE, CI_Item.UDF_GSA_PRICE, CI_Item.UDF_GSA_PRICE_DATE, CI_Item.UDF_GSA_TEMP_PRICE, CI_Item.UDF_GSA_TEMP_PRICE_DATE, CI_Item.UDF_SPECIALORDER, 
                            CI_Item.UDF_ISAMAZ002, CI_Item.UDF_ISAMAZ009, CI_Item.UDF_ISEBAY, CI_Item.UDF_ISNEWEGG, CI_Item.UDF_ISGLOBAL, CI_Item.UDF_ISJET, CI_Item.UDF_ISWALMART, 
                            CI_Item.UDF_AMAZON_SKU, CI_Item.UDF_AMAZON_ASIN, CI_Item.UDF_AMAZON_VENDOR_DISCOUNT, CI_Item.UDF_AMAZON_VENDOR_PRICE, CI_Item.UDF_AMAZON_VENDOR_PRICE_DATE 
                      FROM 
                            CI_Item CI_Item, IM_ItemVendor IM_ItemVendor, IM_ItemWarehouse IM_ItemWarehouse
                      WHERE 
                            CI_Item.ItemCode = IM_ItemVendor.ItemCode AND 
                            CI_Item.PrimaryVendorNo = IM_ItemVendor.VendorNo AND 
                            IM_ItemWarehouse.ItemCode = CI_Item.ItemCode AND 
                            IM_ItemVendor.ItemCode = IM_ItemWarehouse.ItemCode AND 
                            IM_ItemWarehouse.WarehouseCode='000'"""
                                                        
                            #AND          
                            #((CI_Item.InactiveItem<>'Y' AND CI_Item.Category2='NEW') OR (CI_Item.ProductType<>'D' AND CI_Item.Category2='USED'))"""                            
                            #CI_Item.Category4,

                        #"WHERE CI_Item.ItemCode = IM_ItemVendor.ItemCode AND " & _
                        #    "(CI_Item.PrimaryVendorNo = IM_ItemVendor.VendorNo OR CI_Item.PrimaryVendorNo is Null) AND " & _
                        #    "IM_ItemWarehouse.ItemCode = CI_Item.ItemCode AND " & _
                        #    "(IM_ItemVendor.ItemCode = IM_ItemWarehouse.ItemCode OR IM_ItemVendor.ItemCode is Null) AND " & _
                        #    "IM_ProductLine.ProductLine = CI_Item.ProductLine AND " & _
                        #    "IM_ItemWarehouse.WarehouseCode='000' AND " & _
                        #    "((CI_Item.InactiveItem<>'Y' AND CI_Item.Category2='NEW') OR (CI_Item.ProductType<>'D' AND CI_Item.Category2='USED'))"

    #Execute SQL
    print('Retrieving Sage data1')
    sageDF = pd.read_sql(SageSQLquery,sage_cnxn) 

    #SQL Sage data into dataframe
    SageSQLquery = """SELECT 
                            CI_Item.ItemCode, CI_Item.InactiveItem, CI_Item.UDF_CATALOG_NO, CI_Item.UDF_UPC, CI_Item.UDF_GTIN14, CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER, 
                            CI_Item.StandardUnitCost, CI_Item.StandardUnitPrice, CI_Item.SuggestedRetailPrice, CI_Item.Category4,
                            CI_Item.UDF_MAP_PRICE, CI_Item.UDF_VENDOR_PRICE_DATE, CI_Item.ShipWeight, CI_Item.UDF_COUNTRY_OF_ORIGIN_TEMP, CI_Item.UDF_ECCN, 
                            CI_Item.UDF_SCHEDULE_B_NUMBER, CI_Item.UDF_PACK_QUANTITY, CI_Item.ProductLine, CI_Item.ProductType, CI_Item.UDF_REPLACEMENT_ITEM, 
                            CI_Item.UDF_ON_CLEARANCE, CI_Item.UDF_DRP_SHP_ONLY, CI_Item.UDF_SHIPPING_EXCLUSION, IM_ItemWarehouse.MinimumOrderQty, 
                            CI_Item.PrimaryVendorNo, IM_ItemWarehouse.ReorderPointQty, CI_Item.UDF_REVIEW_REQUIRED, CI_Item.UDF_RFQ, CI_Item.UDF_DISCONTINUED_STATUS, CI_Item.DateCreated, 
                            CI_Item.UDF_LOWEST_PRICE, CI_Item.UDF_GSA_PRICE, CI_Item.UDF_GSA_PRICE_DATE, CI_Item.UDF_GSA_TEMP_PRICE, CI_Item.UDF_GSA_TEMP_PRICE_DATE, CI_Item.UDF_SPECIALORDER, 
                            CI_Item.UDF_ISAMAZ002, CI_Item.UDF_ISAMAZ009, CI_Item.UDF_ISEBAY, CI_Item.UDF_ISNEWEGG, CI_Item.UDF_ISGLOBAL, CI_Item.UDF_ISJET, CI_Item.UDF_ISWALMART, 
                            CI_Item.UDF_AMAZON_SKU, CI_Item.UDF_AMAZON_ASIN, CI_Item.UDF_AMAZON_VENDOR_DISCOUNT, CI_Item.UDF_AMAZON_VENDOR_PRICE, CI_Item.UDF_AMAZON_VENDOR_PRICE_DATE 
                      FROM 
                            CI_Item CI_Item, IM_ItemWarehouse IM_ItemWarehouse
                      WHERE 
                            CI_Item.PrimaryVendorNo is Null AND 
                            IM_ItemWarehouse.ItemCode = CI_Item.ItemCode AND 
                            CI_Item.ItemCode = IM_ItemWarehouse.ItemCode AND 
                            IM_ItemWarehouse.WarehouseCode='000'"""

    #Execute SQL
    print('Retrieving Sage data2')
    sageDF2 = pd.read_sql(SageSQLquery,sage_cnxn)    
    #print(sageDF) 
    #print(sageDF2) 
    sageDF = pd.concat([sageDF.set_index('ItemCode'),sageDF2.set_index('ItemCode')],sort=False)
    #sageDF = sageDF.merge(sageDF2, how='outer', left_on='ItemCode', right_on='ItemCode')
    print(sageDF) 
    #exit()

    qarl_conn_str = (
        r'DSN=QARL_64;'
        r'CharSet=utf8;'
        )        

    qarl_cnxn = pyodbc.connect(qarl_conn_str)
    qarl_sql = """SELECT 
                    ProductInfo.ItemCode, ProductInfo.Title70, ProductInfo.Title100, ProductInfo.Title150,  ProductInfo.Header, ProductInfo.Category1, ProductInfo.Category2, 
                    ProductInfo.Category3, ProductInfo.Features , ProductInfo.Description, ProductInfo.ImageUrl, ProductInfo.ProductUrl, ProductInfo.DatasheetUrl, 
                    ProductInfo.ManualUrl, ProductInfo.QuickstartUrl, ProductInfo.BrochureUrl, ProductInfo.VideoUrl, ProductInfo.Keywords, ProductInfo.MetaDescription, ProductInfo.MetaKeywords, ProductInfo.InformationSource, 
                    ProductInfo.PersonUpdated, ProductInfo.Components, ProductInfo.DateUpdated, ProductInfo.MainOrAccessory, ProductInfo.MainUnits, ProductInfo.Accessories, 
                    ProductInfo.RelatedProducts, ProductInfo.AdditionalImages, ProductInfo.Specs, General.MagentoId, General.Condition, General.ProductFamily, General.CategoryId, General.Length, General.Width, General.Height, 
                    General.ShipLength, General.ShipWidth, General.ShipHeight, General.PriceListDescription, General.ClearanceCategory, General.ClearanceFlag, General.RFQEnabled, ProductLine.IsNewStyle, 
                    Google.GoogleId, Google.GoogleProductCategory, Google.GoogleProductType
                FROM 
                    ((dbo.General LEFT JOIN dbo.ProductLine ON dbo.General.ProductLine = dbo.ProductLine.ProductLine) 
                    LEFT JOIN dbo.Google ON dbo.General.ItemCode = dbo.Google.ItemCode) 
                    LEFT JOIN dbo.ProductInfo ON dbo.General.ItemCode = dbo.ProductInfo.ItemCode
    """
    #, Google.GoogleProductCategoryId
    qarlDF = pd.read_sql(qarl_sql,qarl_cnxn)   
    todayakeneoDF = sageDF.merge(qarlDF, how='left', left_on='ItemCode', right_on='ItemCode')
    columnstouse = list(todayakeneoDF)
    lastakeneoDF = pd.read_pickle('LastAkeneoLyzer')

    #todayakeneoDF.to_pickle('LastAkeneoLyzer')
    #exit()

    akeneoDF = pd.concat([todayakeneoDF.set_index('ItemCode'),lastakeneoDF.set_index('ItemCode')],sort=False)
    akeneoDF = akeneoDF.drop_duplicates(keep=False).reset_index()
    akeneoDF = akeneoDF.drop_duplicates(subset='ItemCode')
    akeneoDF = akeneoDF.filter(columnstouse)
    #akeneoDF.to_excel('./akeneoDF.xlsx')
    
    #akeneoDF = akeneoDF.query("InactiveItem!='Y'")
    #akeneoDF = akeneoDF.query("ProductType=='D'")

    if akeneoDF.shape[0] > 0:
        print('there are things to sync...')
        print(akeneoDF)
        #akeneoDF = akeneoDF.applymap(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)
        akeneoDF = akeneoDF.applymap(lambda x: x.encode().decode('utf-8') if isinstance(x, str) else x)
        
        akeneoDF['product_weight'] = akeneoDF['ShipWeight']  
        akeneoColMap = {'InactiveItem': 'enabled',
                        'ItemCode':'identifier', 
                        'VendorAliasItemNo': 'VendorAlias',
                        'UDF_CATALOG_NO': 'Catalog',
                        'UDF_WEB_DISPLAY_MODEL_NUMBER': 'DisplayName',
                        'UDF_UPC': 'UPC',
                        'UDF_GTIN14': 'GTIN',
                        'StandardUnitCost': 'Cost',
                        'StandardUnitPrice': 'SalePrice',
                        'SuggestedRetailPrice': 'MSRP',
                        'UDF_MAP_PRICE': 'MAP',
                        'UDF_COUNTRY_OF_ORIGIN_TEMP': 'COO',
                        'UDF_REPLACEMENT_ITEM': 'Replacement',
                        'DateCreated': 'SageCreatedDate',
                        'UDF_ISAMAZ009': 'OnAmazonVendor',
                        'UDF_AMAZON_ASIN': 'AmazonASIN',
                        'UDF_AMAZON_SKU': 'AmazonSKU',
                        'UDF_VENDOR_PRICE_DATE': 'VendorPriceDate',
                        'UDF_SHIPPING_EXCLUSION': 'freeship_excluded',
                        'UDF_AMAZON_VENDOR_DISCOUNT': 'AmazonVendorDiscount',
                        'UDF_AMAZON_VENDOR_PRICE': 'AmazonVendorPrice',
                        'UDF_AMAZON_VENDOR_PRICE_DATE': 'AmazonVendorPriceDate',
                        'UDF_ISNEWEGG': 'OnNewEgg',
                        'UDF_ISGLOBAL': 'OnGlobal',
                        'UDF_ISJET': 'OnJet',
                        'UDF_ISWALMART': 'OnWalmart',
                        'UDF_GSA_PRICE_DATE': 'GsaPriceDate',
                        'UDF_GSA_TEMP_PRICE': 'GsaTempPrice',
                        'UDF_GSA_TEMP_PRICE_DATE': 'GsaTempPriceDate',
                        'Category1': 'webCategory1',
                        'Category2': 'webCategory2',
                        'Category3': 'webCategory3',
                        'Components': 'TextOnlyComponents',
                        'Description': 'TextOnlyDescription',
                        'Features': 'TextOnlyFeatures',
                        'GoogleProductCategory': 'google_product_category',
                        'GoogleProductType': 'google_product_type',
                        'Height': 'ProductHeight',
                        'Length': 'ProductLength',
                        'ProductLine': 'Brand',
                        'ShipHeight': 'ShippingHeight',
                        'ShipLength': 'ShippingLength',
                        'ShipWeight': 'ShippingWeight',
                        'ShipWidth': 'ShippingWidth',
                        'Width': 'ProductWidth'}         
        akeneoDF = akeneoDF.rename(columns=akeneoColMap)

        #Top Level Index
        akeneoDF.loc[akeneoDF['enabled'] == 'Y','enabled'] = False
        akeneoDF.loc[akeneoDF['enabled'] != False,'enabled'] = True

        #Akeneo Atts
        akeneoDF['google_mpn'] = akeneoDF['VendorAlias']
        akeneoDF['google_description'] = akeneoDF['Title150']

        akeneoDF['google_link'] = akeneoDF.apply(get_google_link, axis=1)

        akeneoDF['freeship_excluded'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'freeship_excluded', axis=1)
        akeneoDF['Clearance'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'Clearance', axis=1)
        akeneoDF['DropShipOnly'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'DropShipOnly', axis=1)
        akeneoDF['ReviewReq'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'ReviewReq', axis=1)
        akeneoDF['SpecialOrder'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'SpecialOrder', axis=1)
        akeneoDF['OnAmazonSeller'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnAmazonSeller', axis=1)
        akeneoDF['OnAmazonVendor'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnAmazonVendor', axis=1)
        akeneoDF['OnEbay'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnEbay', axis=1)
        akeneoDF['OnNewEgg'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnNewEgg', axis=1)
        akeneoDF['OnGlobal'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnGlobal', axis=1)
        akeneoDF['OnJet'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnJet', axis=1)
        akeneoDF['OnWalmart'] = akeneoDF.apply(yesno_to_truefalse, column_name = 'OnWalmart', axis=1)

        akeneoDF['accessory'] = akeneoDF['Accessories'].str.replace('|',',')
        akeneoDF['related'] = akeneoDF['RelatedProducts'].str.replace('|',',')
        akeneoDF['mainUnit'] = akeneoDF['MainUnits'].str.replace('|',',')        

        akeneoDF.loc[akeneoDF['product_weight'].notnull(),'product_weight-unit'] = 'POUND'
        akeneoDF.loc[akeneoDF['ProductHeight'].notnull(),'ProductHeight-unit'] = 'INCH'
        akeneoDF.loc[akeneoDF['ProductLength'].notnull(),'ProductLength-unit'] = 'INCH'
        akeneoDF.loc[akeneoDF['ProductWidth'].notnull(),'ProductWidth-unit'] = 'INCH'
        akeneoDF.loc[akeneoDF['ShippingHeight'].notnull(),'ShippingHeight-unit'] = 'INCH'
        akeneoDF.loc[akeneoDF['ShippingLength'].notnull(),'ShippingLength-unit'] = 'INCH'
        akeneoDF.loc[akeneoDF['ShippingWidth'].notnull(),'ShippingWidth-unit'] = 'INCH'

        for cols in attributeCols:
            akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = None, axis = 1)          
        for cols in currencyCols:
            akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = 'USD', unit = None, axis = 1)     
        for cols in unitCols:
            if 'weight' in cols or 'Weight' in cols:
                akeneoUnit = 'POUND'
            else:
                akeneoUnit = 'INCH'
            akeneoDF = akeneoDF.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = akeneoUnit, axis = 1)
        for cols in associationCols:
            akeneoDF = akeneoDF.apply(make_json_association_data_nest, column_name = cols, axis = 1)  
        
        valuesCols = attributeCols + currencyCols + unitCols
        akeneoDF = akeneoDF.loc[:, jsonCols].reindex()

        jsonDF = (akeneoDF.groupby(['identifier','enabled'], as_index=False)
                    .apply(lambda x: x[valuesCols].dropna(axis=1).to_dict('records'))
                    .reset_index()
                    .rename(columns={0:'values'}))

        jsonDF['values'] = jsonDF['values'].str[0]

        jsonDF = (jsonDF.merge(akeneoDF.groupby(['identifier','enabled'], as_index=False)
                    .apply(lambda x: x[associationCols].dropna(axis=1).to_dict('records'))
                    .reset_index()
                    .rename(columns={0:'associations'})))  
        
        jsonDF['associations'] = jsonDF['associations'].str[0]
        jsonDF.to_excel('./jsonDF.xlsx', encoding = 'utf8')
        jsonout = open("itemjson.json", "w")

        load_failure = False
        api_errors_file = open("Akeneo_Sync_Data_Errors.csv", "w") 
        #******** - Andrew 
        try:
            #values_for_json = jsonDF.loc[:, ['identifier','enabled','values']].to_json(orient='records')
            values_for_json = jsonDF.loc[:, ['identifier','enabled','values']].to_dict(orient='records')
            #jsonout.write(str(values_for_json))         
            #results = akeneo.products.update_create_list(j_load)      
            data_results = akeneo.products.update_create_list(values_for_json)                  
            print(data_results)   
        except requests.exceptions.RequestException as api_error:
            load_failure = True
            api_errors_file.write(str(api_error))    
        api_errors_file.close()                      

        if load_failure:
            #assignees = '[********]' #Doug
            assignees = '[*******]' #Andrew
            #folderid = '************' #Web Requests **************
            folderid = '**************' #Data Requests 
            description = "Attached List of items failed while syncing to Akeneo due to Akeneo's Validation.\n\nThere should be a description of the error from the Akeneo API, along with the ItemCode."
            #response = makeWrikeTask(title = "Akeneo Sync Product Data Failure (" + str(fail_count) + ") - " + date.today().strftime('%y/%m/%d'), description = description, assignees = assignees, folderid = folderid)
            response = makeWrikeTask(title = "Akeneo Sync Product Data Failure - " + date.today().strftime('%y/%m/%d'), description = description, assignees = assignees, folderid = folderid)
            response_dict = json.loads(response.text)
            taskid = response_dict['data'][0]['id']
            filetoattachpath = "Akeneo_Sync_Data_Errors.csv"
            print('Attaching file')
            attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
            print('File attached!')

        load_failure = False
        api_errors_file = open("Akeneo_Sync_Associations_Errors.csv", "w") 
        try:
            values_for_json = jsonDF.loc[:, ['identifier','associations']].to_dict(orient='records')
            #jsonout.write(str(values_for_json))   
            associations_results = akeneo.products.update_create_list(values_for_json)                  
            print(associations_results)   
        except requests.exceptions.RequestException as api_error:
            load_failure = True
            api_errors_file.write(str(api_error))              
        api_errors_file.close()

        if load_failure:
            assignees = '[************]' #Andrew
            #folderid = '*************' #Web Requests **************
            folderid = 'IEAAJKV3I4JBAOZD' #Data Requests 
            description = "Attached List of items failed while syncing to Akeneo due to Akeneo's Validation.\n\nThere should be a description of the error from the Akeneo API, along with the ItemCode."
            response = makeWrikeTask(title = "Akeneo Sync Product Associations Failure - " + date.today().strftime('%y/%m/%d'), description = description, assignees = assignees, folderid = folderid)
            response_dict = json.loads(response.text)
            taskid = response_dict['data'][0]['id']
            filetoattachpath = "Akeneo_Sync_Associations_Errors.csv"
            print('Attaching file')
            attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
            print('File attached!')     

        data_reponse_df = pd.DataFrame.from_dict(data_results)  
        data_reponse_df = data_reponse_df.loc[(data_reponse_df["status_code"] > 299) | (data_reponse_df["status_code"] < 200)]
        print(data_reponse_df)     
        associations_reponse_df = pd.DataFrame.from_dict(associations_results)
        associations_reponse_df = associations_reponse_df.loc[(associations_reponse_df["status_code"] > 299) | (associations_reponse_df["status_code"] < 200)]
        print(associations_reponse_df)   

        errordf = data_reponse_df.merge(associations_reponse_df, how='outer', left_on='identifier', right_on='identifier', suffixes=('_data_sync_error', '_associations_sync_error'))

        if errordf.shape[0] > 0:
            errordf.to_excel('errordf.xlsx')
            print('some items did not pass akeneo validation')
            assignees = '[************,************]' #
            folderid = '**************' #Web Requests **************
            wrikedescription = "Attached List of items failed while syncing to Akeneo due to Akeneo's Data Validation.\n\nThere should be a description of the error drom the Akeneo API, along with the ItemCode."
            wriketitle = "Akeneo Sync Product Data/Assocations Validations errors - " + date.today().strftime('%y/%m/%d') + "(" + str(errordf.shape[0]) + ")"
            response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
            response_dict = json.loads(response.text)
            taskid = response_dict['data'][0]['id']
            filetoattachpath = 'errordf.xlsx'
            print('Attaching file')
            attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
            print('File attached!')   
        else:
            print('no api data errors....Yay!')
       
    else:
        print('nothing to sync ... :D')

    todayakeneoDF.to_pickle('LastAkeneoLyzer')

    
        #for dfindex, dfrow in jsonDF.iterrows():
        #    try:    
        #        values_for_json =  dfrow.to_json(orient='columns')
        #        jsonout.write(values_for_json + '\n')
        #        j_load = json.loads(values_for_json)                  
        #        results = akeneo.products.update_create_item(j_load)      
        #        print(results)      
        #    except requests.exceptions.RequestException as api_error:
        #        fail_count = fail_count + 1
        #        load_failure = True
        #        api_errors_file.write(dfrow['identifier'] + ',' + str(api_error) + '\n')
        #        #values_for_json =  dfrow.to_json(orient='columns')
        #        #jsonout.write(values_for_json + '\n')
        #        #j_load = json.loads(values_for_json)  
        #        #print('oppsy..' + str(api_error))
        #api_errors_file.close()     