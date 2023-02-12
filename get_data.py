# 获取基金经理的信息进行总结
import grequests
import requests
import time
import json
import sqlite3
from fake_useragent import FakeUserAgent

class FundTrader(object):
    def __init__(self):
        self.result = []
        # 当前有67 页数据
        self.page_number = 67
    def update_fund_trader(self):
        req_list = []
        start_req = time.time()
        for page in range(1,self.page_number+1):
            ua = FakeUserAgent().random
            url = f"https://fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx?dt=14&mc=returnjson&ft=all&pn=50&pi={page}&sc=abbname&st=asc"
            header = {"User-Agent":ua}
            req_list.append(grequests.get(url,headers=header,verify=False))
        res_list = grequests.map(req_list)
        print("req cost time:",time.time()-start_req)
        conn = sqlite3.connect("fundTrader.db")
        cursor = conn.cursor()
        write_sql_start = time.time()
        for res in res_list:
            if res and res.status_code == 200:
                try:
                    data_list = res.content.decode("utf-8").split(":")[1][:-7]
                    data_list = json.loads(data_list)
                    for data in data_list:
                        trader_code = data[0]
                        trader_name = data[1]
                        fund_code = data[4]
                        fund_name = data[3]
                        trader_days = data[6]
                        trader_value = data[7][:-1] if "%" in data[7] else data[7]
                        trader_scale = data[10]
                        # "基金经理: {trader_name},基金编号: {fund_code}, 基金名称: {fund_name}, 基金经理年限: {trader_years},基金经理盈利能力: {trader_value},基金规模: {trader_scale}"+"\n")

                        year = int(trader_days)/365
                        month = int(trader_days)/30
                        trader_daliy_value = str(float(trader_value)/int(trader_days)) if trader_value!='--' else "0"
                        trader_month_value = str(float(trader_value)/month) if trader_value!='--' else "0"
                        trader_year_value = str(float(trader_value)/year) if trader_value!='--' else "0"

                        cursor.execute(f"replace into Trader (id,trader_name,fund_code,fund_name,trader_days,trader_value,trader_scale,trader_daliy_value,trader_month_value,trader_year_value) values ('{trader_code}','{trader_name}','{fund_code}','{fund_name}','{trader_days}','{trader_value}','{trader_scale}','{trader_daliy_value}','{trader_month_value}','{trader_year_value}')")
                        conn.commit()

                except Exception as e:
                    import traceback
                    traceback.print_exc()
        conn.close()
        print("write sql cost time: ",time.time()-write_sql_start)
    def init_sql_struct(self):
        conn = sqlite3.connect("fundTrader.db")
        cursor = conn.cursor()
        cursor.execute("""drop table Trader""")
        conn.commit()
        sql = """create table Trader 
        (id string primary key default "" ,
        trader_name string default "" ,
        fund_code string default "" ,
        fund_name string default "" ,
        trader_days string default "0",
        trader_value string default "0",
        trader_scale string default "0",
        trader_daliy_value string default "0",
        trader_month_value string default "0",
        trader_year_value string default "0"
        );
        """
        cursor.execute(sql)
        conn.commit()
        conn.close()
if __name__ == '__main__':
    t = FundTrader()
    # 只有初始化数据库才执行
    # t.init_sql_struct()
    # select * from Trader where trader_days> 365 and trader_month_value >0.9 and trader_year_value>10 order by trader_value desc limit 10;
    t.update_fund_trader()
