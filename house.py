import requests
import psycopg2
import datetime
import ast
import re
from lxml import etree
import csv
import time
import schedule
import threadpool
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'
}
h = ['链家编号', '标题', '成交日期', '成交价', '单价', '挂牌价格', '成交周期', '房屋户型', '所在楼层', '建筑面积', '户型结构', '套内面积', '建筑类型', '建筑类型',
     '房屋朝向',
     '建成年代', '装修情况', '建筑结构', '供暖方式', '梯户比例', '产权年限', '配备电梯', '交易权属', '挂牌时间', '房屋用途', '房屋年限', '房权所属']


# 获得总页数
def get_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    info_urls = html.xpath('//div[@class="page-box house-lst-page-box"]/@page-data')[0]
    page_info = ast.literal_eval(info_urls)
    return page_info['totalPage']
# 获取地区的URL
def get_area(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    names = html.xpath('//div[@data-role="ershoufang"]/div/a/text()')
    urls = html.xpath('//div[@data-role="ershoufang"]/div/a/@href')
    area_info = {}
    for name,url in zip(names,urls):
        area_info[name] = url
    print(area_info)
    return area_info
# 获得页面的房源详情的URL
def get_house_info_url(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    urls = html.xpath('//ul[@class="listContent"]/li/a/@href')
    return urls
# 获取房源房源详情页面的详细房子信息
def get_house_info_message(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    title = html.xpath('//div[@class="wrapper"]/text()')[0]
    str_deal_time = html.xpath('//div[@class="wrapper"]/span/text()')[0]
    deal_time = re.findall(r"(\d{4}.\d{1,2}.\d{1,2})",str_deal_time)[0]
    deal_total_price_dw = html.xpath('//span[@class="dealTotalPrice"]/text()')[0]
    deal_total_price = html.xpath('//span[@class="dealTotalPrice"]/i/text()')[0]
    str_deal_total_price = deal_total_price + deal_total_price_dw
    deal_price_dw = html.xpath('//div[@class="price"]/text()')[0]
    deal_price = html.xpath('//div[@class="price"]/b/text()')[0]
    str_deal_price = deal_price + deal_price_dw
    msgs = html.xpath('//div[@class="msg"]')[0]
    price_dw = msgs.xpath('./span/text()')[0]
    price = msgs.xpath('./span/label/text()')[0]
    str_orage_price = price + price_dw
    deal_date_dw = msgs.xpath('./span/text()')[1]
    deal_date = msgs.xpath('./span/label/text()')[1]
    str_deal_date = deal_date + deal_date_dw
    base_info = html.xpath('//div[@class="base"]/div[@class="content"]/ul/li/text()')
    transaction_info = html.xpath('//div[@class="transaction"]/div[@class="content"]/ul/li/text()')
    info={}
    info['id'] = transaction_info[0].strip()
    info['title'] = title.strip()
    info['transaction_date'] = deal_time.strip()
    info['final_price'] = str_deal_total_price.strip()
    info['unit_price'] = str_deal_price.strip()
    info['listing_price'] = str_orage_price.strip()
    info['transaction_cycle'] = str_deal_date.strip()
    info['house_type'] = base_info[0].strip()
    info['floor'] = base_info[1].strip()
    info['construction_area'] = base_info[2].strip()
    info['house_structure'] = base_info[3].strip()
    info['inner_area'] = base_info[4].strip()
    info['building_type'] = base_info[5].strip()
    info['house_orientation'] = base_info[6].strip()
    info['built_era'] = base_info[7].strip()
    info['renovation_condition'] = base_info[8].strip()
    info['building_structure'] = base_info[9].strip()
    info['heating_method'] = base_info[10].strip()
    info['ladder_ratio'] = base_info[11].strip()
    info['property_year_right'] = base_info[12].strip()
    info['elevator_info'] = base_info[13].strip()

    info['trading_authority'] = transaction_info[1].strip()
    info['listing_time'] = transaction_info[2].strip()
    info['usage_houses'] = transaction_info[3].strip()
    info['year_house'] = transaction_info[4].strip()
    info['house_ownership'] = transaction_info[5].strip()
    return info
# 用csv方式存储
def save_csv(name,house_info):
    with open(f'{name}.csv', 'a', encoding='utf-8') as f:
        time.sleep(3)
        writer = csv.DictWriter(f, h)
        writer.writerow(house_info)

# 根据总页数，循环获取房子信息，并保存
def del_house_info(or_url,name):
        try:
            allpage = get_page(or_url)
        except:
            print(f"获取总页数出错{or_url}")
        for pg in range(1,int(allpage)+1):
            info_url = or_url + 'pg' + str(pg)
            try:
                urls = get_house_info_url(info_url)
            except:
                print(f"获取页面详情的URL出错{info_url}")
            print(pg,name)
            print(urls)
            for url in urls:
                try:
                    house_info = get_house_info_message(url)
                except:
                    print(f"解析详细信息出错{url}")
                house_info['area'] = name
                #save_csv(name, house_info)
                try:
                    save_info(house_info,'house.house')
                except:
                    print(f"保存结果出错{house_info}")
# 用数据库的方式保存信息
def save_info(house_info,TableName):
    ROWstr = ''
    conn = psycopg2.connect(database="postgres", user="postgres", password="123456", host="192.168.125.128", port="77")
    cur = conn.cursor()
    select_sql = "select count(*) from %s where  id = " "'%s' and title = " "'%s'"  % (TableName, house_info['id'],house_info['title'])
    cur.execute(select_sql)
    num = cur.fetchone()[0]

    if num == 0:
        for key, value in house_info.items():
            ROWstr = (ROWstr + "'%s'" + ',') % (value)
        insert_sql = "INSERT INTO %s VALUES (%s  ,current_timestamp)" % (TableName, ROWstr[:-1])
        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()
# 总的入口方法
def get_house_main_info(city_url,city_name):
    url=city_url+'chengjiao/'
    try:
      area_url_info = get_area(url)
    except:
      print(f"获取地区的URL出错{url}")
    pool = threadpool.ThreadPool(10)
    data = [(None, {'or_url': f'city_url+{or_url}','name': name}) for name,or_url in area_url_info.items()]
    reqs = threadpool.makeRequests(del_house_info, data)
    [pool.putRequest(req) for req in reqs]
    pool.wait()
# 城市url获取
def get_city_url():
    resp = requests.get("https://www.lianjia.com/city/", headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    city_urls = html.xpath('//div[@class="city_province"]//a/@href')
    city_names= html.xpath('//div[@class="city_province"]//a/text()')
    city_info = {}
    for url,name in zip(city_urls,city_names):
        city_info[name] = url
    return city_info;
def main():

    city_urls = get_city_url()
    for name, url in city_urls.items():
        get_house_main_info(url , name)
     #schedule.every().day.at("12:00").do(get_house_main_info())
     #while True:
         #schedule.run_pending()
if __name__ == '__main__':
     main()
