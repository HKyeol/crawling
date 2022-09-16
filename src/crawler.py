from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
import cx_Oracle
import os
import json

class Crawler:
    '''
     This class aims to scrap the next week's dividends calendar in the website of investing.com and finviz
     It consists of four steps:
     1) Initialization
     2) Scraping stock data from investing
     3) Scraping stock data from finviz
     4) Store the data into Oracle
     '''
        
    def __init__(self, driver_path=None):
            env = dict()
            # driver_path can be initialized in ../environment.txt
            if driver_path is None:
                try:
                    with open("./environment.txt", "r", encoding="UTF-8") as input_file:
                        for line in input_file.readlines():
                            key, val = [e.strip() for e in line.split("=")]
                            env.update({key: val})
                    driver_path = env["driver_path"]
                    print(env)
                except:
                    #print("Failed to load environment.txt, please check the usage in readme.md")
                    raise Exception("Failed to load environment.txt, please check the usage in readme.md")
         
            self.news_list = None
            self.news_data = None
            self.BASE_URL = "https://kr.investing.com/dividends-calendar/"
            self.connect = cx_Oracle.connect(env["oracle_id"], env["oracle_pw"],  env["oracle_host"])
            self.cursor = self.connect.cursor()
    
    def create_table(self):
        '''
        Log in to Oracle and create a table.
        '''
        env = dict()
        with open("./environment.txt", "r", encoding="UTF-8") as input_file:
            for line in input_file.readlines():
                key, val = [e.strip() for e in line.split("=")]
                env.update({key: val})
                    
        connect = cx_Oracle.connect(env["oracle_id"], env["oracle_pw"],  env["oracle_host"])
        cursor = connect.cursor()
        # drop table
        cursor.execute("drop table dividends_calendar")
        
        # create table
        cursor.execute("create table dividends_calendar \
                       (ename varchar2(10), \
                        ex_dividend date, \
                        dividend number(10), \
                        dividend_rate number(10))")
        connect.commit()
        return 
        
    def crawl_investing(self):
        '''
        This function collects dividends data from investing.com
        Use selenium because we need a click to change the filter
        Data is returned in dict type
        '''
        driver = webdriver.Chrome('C:/a/chromedriver')
        
        driver.implicitly_wait(2)
        driver.get('https://kr.investing.com/dividends-calendar/')
        driver.implicitly_wait(2)
        # Click next week data filter
        driver.find_element_by_xpath('//*[@id="timeFrame_nextWeek"]').click()
        driver.implicitly_wait(3)
        # Click filter
        driver.find_element_by_xpath('//*[@id="filterStateAnchor"]').click()
        driver.implicitly_wait(3)
        # Uncheck Singapore, UK, China, Hong Kong 
        driver.find_element_by_xpath('//*[@id="country36"]').click()
        driver.implicitly_wait(1)
        driver.find_element_by_xpath('//*[@id="country4"]').click()
        driver.implicitly_wait(1)
        driver.find_element_by_xpath('//*[@id="country37"]').click()
        driver.implicitly_wait(1)
        driver.find_element_by_xpath('//*[@id="country35"]').click()
        driver.implicitly_wait(1)
        driver.find_element_by_xpath('//*[@id="country39"]').click()
        driver.implicitly_wait(2)
        driver.find_element_by_xpath('//*[@id="ecSubmitButton"]').click()
        driver.implicitly_wait(10)
        # Sort by Dividend Rate
        element = driver.find_element_by_xpath('//*[@id="dividendsCalendarData"]/thead[1]/tr/th[7]')
        driver.execute_script("arguments[0].click();", element)
        driver.implicitly_wait(2)
        driver.execute_script("arguments[0].click();", element)
    
        df = pd.DataFrame(columns=['ename', 'ex_dividend', 'dividend', 'dividend_rate'])
        
        self.data_to_insert = {}
        # insert data 
        for i in range(2,200):
            try:
                data_to_insert = {'ename': driver.find_element_by_css_selector(f"#dividendsCalendarData > tbody > tr:nth-child({i}) > td.left.noWrap > a").text,  \
                                  'ex_dividend': driver.find_element_by_css_selector(f"#dividendsCalendarData > tbody > tr:nth-child({i}) > td:nth-child(3)").text, \
                                  'dividend': driver.find_element_by_css_selector(f"#dividendsCalendarData > tbody > tr:nth-child({i}) > td:nth-child(4)").text, \
                                  'dividend_rate' :(driver.find_element_by_css_selector(f"#dividendsCalendarData > tbody > tr:nth-child({i}) > td:nth-child(7)").text)[:len(driver.find_element_by_css_selector(f"#dividendsCalendarData > tbody > tr:nth-child({2}) > td:nth-child(7)").text)-1]}
                df = df.append(data_to_insert, ignore_index=True)
            except :
                pass
        return df

    def save_sql(self, save_path="./data/stock_data.json"):
        '''
        Data received from investing is stored in SQL
        '''          
        df = self.crawl_investing()
        for j in range(len(df)):
            df['ex_dividend'][j] = df['ex_dividend'][j].replace('년 ', '-').replace('월 ','-').replace('일','')
            df['dividend_rate'][j]=df['dividend_rate'][j][:-1]


        env = dict()
        with open("./environment.txt", "r", encoding="UTF-8") as input_file:
            for line in input_file.readlines():
                key, val = [e.strip() for e in line.split("=")]
                env.update({key: val})
        connect = cx_Oracle.connect(env["oracle_id"], env["oracle_pw"],  env["oracle_host"])
        cursor = connect.cursor()
        
        rows = [tuple(x) for x in df.to_records(index=False)]

        cursor.executemany("""INSERT INTO dividends_calendar (ename, ex_dividend, dividend, dividend_rate) VALUES (:1,:2,:3,:4)""", rows)
        connect.commit()        
    
    def crawl_finviz(self,symbol):
        '''
        This function collects additional data from finviz.com
        '''        
        try:
            url = r'http://finviz.com/quote.ashx?t={}'.format(symbol.lower())
            req = Request(url,headers={'User-Agent': 'Chrome/105.0.5195.102'})
            html = urlopen(req).read()
            soup = bs(html, 'lxml')
            pb = soup.find(text="Change")
            pb1 = pb.find_next(class_='snapshot-td2').text
            pb = soup.find(text="Perf Week")
            pb2 = pb.find_next(class_='snapshot-td2').text
            pb = soup.find(text="Perf Month")
            pb3 = pb.find_next(class_='snapshot-td2').text
            pb = soup.find(text="P/E")
            pb4 = pb.find_next(class_='snapshot-td2').text
            pb = soup.find(text="Target Price")
            pb5 = pb.find_next(class_='snapshot-td2').text
            pb = soup.find(text="Recom")
            pb6 = pb.find_next(class_='snapshot-td2').text
            
            return pb1, pb2, pb3, pb4, pb5, pb6
        except Exception as e:
            print(e)
    
  
    def getjson(self,symbol):
        '''
        The data received from the 'crawl_finviz' function is changed to json form and returned
        '''               
        psr = self.crawl_finviz(symbol)
        json_object = {
            "ename": symbol,
            "change": psr[0],
            "perf_week": psr[1],
            "perf_month": psr[2],
            "pe": psr[3],
            "target_price": psr[4],
            "recom": psr[5]
        }
    
        json_string = json.dumps(json_object)
        return json_string



    def save_json(self):    
        
        env = dict()
        with open("./environment.txt", "r", encoding="UTF-8") as input_file:
            for line in input_file.readlines():
                key, val = [e.strip() for e in line.split("=")]
                env.update({key: val})
                    
        connect = cx_Oracle.connect(env["oracle_id"], env["oracle_pw"],  env["oracle_host"])
        cursor = connect.cursor()
        json_data = {}

        cursor.execute("""select ename from dividends_calendar""")
        row = cursor.fetchall()
        df2 = pd.DataFrame(row)
        df2.columns = ['ename']

        json_data['stock_data']=[]
        for i in range(1,9):
            json_data['stock_data'].append(self.getjson(df2['ename'][i]))
        
        with open('output.json', 'w') as f:
            json.dump(json_data, f, indent=2)   
