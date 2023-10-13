# -*- coding: utf-8 -*-
"""
Created on Fri Oct 13 01:08:39 2023

@author: Gary

download data from https://agr.afa.gov.tw/afa/afa_frame.jsp
only for taiwan ip, vpn is needed if located outside taiwan
"""

import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import polars as pl
import json

class AGRAFA:
    def __init__(self):
        self.year = range(86,112)
        self.item = {'00':'裡作','01':'一期作','02':'二期作','03':'全年作'}
        self.crop_type = {'01':'雜糧類','02':'蔬菜類','03':'果品類','04':'牧草類',
                           '05':'特用作物類','06':'花卉','07':'綠肥類','08':'其他類'}
        self.crops = self.LoadAllCrop(self.crop_type)
        self.city = {'0001':'新北市','0002':'宜蘭縣','0003':'桃園市','0004':'新竹縣',
                     '0005':'苗栗縣','0006':'臺中市','0007':'彰化縣','0008':'南投縣',
                     '0009':'雲林縣','0010':'嘉義縣','0011':'臺南市','0012':'高雄市',
                     '0013':'屏東縣','0014':'臺東縣','0015':'花蓮縣','0016':'澎湖縣',
                     '0017':'基隆市','0018':'新竹市','0020':'嘉義市','0063':'臺北市',
                     '0065':'金門縣','0066':'連江縣'}
    
    def LoadAllCrop(self,crop_type:dict):
        crop = {}
        for key in crop_type.keys():
            crop[key] = self.LoadCrop(key)
        return crop
    
    def LoadCrop(self,crop_type:str):
        url = f'https://agr.afa.gov.tw/afa/ajax/jsonAfaCode1.jsp?corn001={crop_type}&input803='
        response = requests.post(url)
        temp_crop = json.loads(response.text)
        crop = {}
        for c in temp_crop:
            crop[c['code']] = c['name']
        return crop
        
    
    def Post(self,year,item,crop_type,crop,city):
        """
        Arguments:
            year: 年度, 3 digits
            item: 期作別
            crop_type: 作物類別
            crop: 作物代號
            city: 縣市代號
        
        Returns:
            The post response text in html
        """
        
        url = 'https://agr.afa.gov.tw/afa/pgcroptown.jsp'
        body = {
            'accountingyear': year,
            'item': item,
            'corn001': crop_type,
            'input803': '', 
            'crop': crop,
            'city': city,
            'btnSend': '送　出'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=body, headers=headers)
        return response

    def ToDataFrame(self,response_text):
        """
        Arguments:
            reponse_text: response.text
            
        Returns:
            The polar dataframe of table in response text
        """
        # Parse HTML Response Text
        soup = BeautifulSoup(response_text, 'html.parser')
        tables = soup.body.find_all('table')
        data_table = tables[2]
        rows = data_table.find_all('tr')

        # Get Column Names and Units
        col_names = []
        for col in rows[0].find_all('td'):
            col_names.append(col.text)
        for c in range(len(rows[1].find_all('td'))):
            col_names[c+1] += '(' + rows[1].find_all('td')[c].text +')'

        # Get Data
        data_row = []
        for r in range(2,len(rows)-1):
            row = []
            for col in rows[r].find_all('td'):
                if (col != rows[r].find_all('td')[0]):
                    row.append(float(col.text.replace(',','')))
                else:
                    row.append(col.text)
            data_row.append(row)

        # Create a DataFrame based on Column Names and Data
        df = pl.DataFrame(data_row, schema=col_names)
        return df
    
    #%% 檢查所有的檔頭，輸出txt
    def ExportHeaderAndFirstRow(self, response_text, sw):
        # Parse HTML Response Text
        soup = BeautifulSoup(response_text, 'html.parser')
        tables = soup.body.find_all('table')
        data_table = tables[2]
        rows = data_table.find_all('tr')

        # Get Column Names and Units
        col_names = []
        for col in rows[0].find_all('td'):
            col_names.append(col.text)
        for c in range(len(rows[1].find_all('td'))):
            col_names[c+1] += '(' + rows[1].find_all('td')[c].text +')'
        
        for c in col_names:
            sw.write(c + '\t')
        sw.writelines('\n')
        
        # Get Data
        for r in range(2,len(rows)-1):
            row = []
            for col in rows[r].find_all('td'):
                row.append(col.text)
            for r in row:
                sw.write(r + '\t')
            sw.writelines('\n')
            break

    def ExportAllCropTypesHeader(self, year, item_key, city):
        sw = open('header.txt','a')
        for crop_type_key in self.crop_type.keys():
            crop_type = self.crop_type[crop_type_key]
            sw.writelines(f'{crop_type_key}:{crop_type}\n')
            for crop_key in self.crops[crop_type_key].keys():
                crop = self.crops[crop_type_key][crop_key]
                sw.writelines(f'{crop_key}:{crop}\n')
                response = afa.Post(year,item_key, crop_type_key, crop_key, city)
                afa.ExportHeaderAndFirstRow(response.text,sw)
        sw.close()

    #%%
    def GetAllCropData(self,city_name, crop_name):
        #|0年|1期作|2縣市|3鄉鎮|4種植面積(公頃)|5收穫面積(公頃)|6每公頃收量(公斤)|7收量(公斤)|
        item_key = self.item.keys()
        city_keys = list(self.city.keys())
        city_values = list(self.city.values())
        city_key = city_keys[city_values.index(city_name)]
        if (city_key == -1):
            raise "city not found."
        
        crop_key = ''
        for value in self.crops.values():
            crop_keys = list(value.keys())
            crop_values = list(value.values())
            if (crop_name in crop_values):
                crop_key = crop_keys[crop_values.index(crop_name)]
                break
        if (crop_key == ''):
            raise "crop not found."
        
        sw = open(f'{city_name}_{crop_name}.txt','w')
        column_header = ['年','期作','縣市','鄉鎮']
        for year in self.year:
            year = "{:03d}".format(year)
            for item in item_key:
                response = self.Post(year, item, '', crop_key, city_key)
                df = self.ToDataFrame(response.text)
                for row in range(len(df)):
                    # append column header
                    if (len(column_header) == 4):
                        for col in df.columns[1:]:
                            column_header.append(col)
                        line = ''
                        for col in column_header:
                            line += col + ','
                        line = line[:-1]
                        sw.writelines(line + '\n')

                    area_name = df[row].get_column(df.columns[0])[0][3:]
                    line = f'{year},{item},{city_name},{area_name},'
                    for col in df.columns[1:]:
                        line += str(df[row].get_column(col)[0]) + ','
                    line = line[:-1]
                    sw.writelines(line + '\n')
        sw.close()

# Example
afa = AFA()

# Get DataFrame of query data
year = '111'         # must be 3 digit, with leading zero
item_key = '03'      # 全年作
crop_type_key = '02' # 蔬菜類
crop_key = '203'     # 馬鈴薯
city_key = '0011'    # 臺南市
response = self.Post(year, item_key, crop_type_key, crop_key, city_key)
df = self.ToDataFrame(response.text)

# Export specific crop data to txt
afa.GetAllCropData('臺南市','馬鈴薯')
