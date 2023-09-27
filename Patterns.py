import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
import time
import schedule 
import streamlit as st

conn = st.experimental_connection('mysql', type='sql',ttl=0)


url = "https://chartink.com/screener/process"

condition =  {"scan_clause": "( {cash} ( [0] 5 minute close > 1 day ago high and [0] 5 minute close > 2 days ago high and [0] 5 minute close > 3 days ago high and [0] 5 minute close > 4 days ago high and latest volume > 1 day ago volume and latest volume > 2 days ago volume and latest volume > 3 days ago volume and latest volume > 4 days ago volume ) )"}

runEvery = 1  # Ten or less than 10 but greater than 0 

Nifty= conn.query("SELECT DISTINCT nsecode FROM NSECode") 

def run_code():

    with requests.session() as s:
        r_data = s.get(url)
        soup = bs(r_data.content, "lxml")
        meta = soup.find("meta", {"name" : "csrf-token"})["content"]

        header = {"x-csrf-token" : meta}
        data = s.post(url, headers=header, data=condition).json()

        stock_list = pd.DataFrame(data["data"])


    filtered_stock_list = pd.merge(stock_list, Nifty[['nsecode']], how='inner', left_on='nsecode', right_on='nsecode')

    if len(filtered_stock_list) >  0:
        # st.dataframe(filtered_stock_list)  
        filtered_stock_list.set_index('nsecode', inplace=True) 
        st.dataframe(filtered_stock_list,width=1000,column_order = ("nsecode", "name","per_chg","close"))
    

def run_script():
    try:
        current_minute = datetime.now().minute
        if current_minute % runEvery == 0:
            print(f"Running script at {datetime.now()}...")
            run_code()
        
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Restarting the script...")
        time.sleep(60)  # Wait for a minute before restarting
        run_script()

schedule.every(1).minutes.do(run_script)
while True:
    schedule.run_pending()
    time.sleep(1)  
 