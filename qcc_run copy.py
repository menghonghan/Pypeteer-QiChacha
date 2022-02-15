

# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 15:16:59 2022

@author: hmh
"""

from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
from pyquery import PyQuery as pq
import requests
import asyncio
import random
import time
from pyppeteer import launch
import os
import json
import pymysql
from sqlalchemy import create_engine
import numpy as np
from odps.df import DataFrame
from odps import ODPS
import csv
from datetime import date

#
async def message_risk_to_df(message, key):
    # 统一社会信用代码
    uscc = []
    try:
        uscc.append(
            message.split('统一社会信用代码')[1].split('复制')[0].replace(" ", "").replace("\n", ""))
    except:
        uscc.append('无法收集')

    # 企业名称
    list_companys = []
    try:
        list_companys.append(message.split('企业名称')[1].split('复制')[0].replace(" ", "").replace("\n", ""))
    except:
        list_companys.append('无法收集')

    # 法定代表人
    Legal_Person = []
    try:
        Legal_Person.append(message.split('法定代表人')[1].split('关联')[0].replace("\n", "").replace(" ", ""))
    except:
        Legal_Person.append('无法收集')

    # 登记状态
    Registration_status = []
    try:
        Registration_status.append(message.split('登记状态')[1].split('成立日期')[0].replace(" ", "").replace("\n", ""))
    except:
        Registration_status.append('无法收集')

    # 成立日期
    Date_of_Establishment = []
    try:
        Date_of_Establishment.append(message.split('成立日期')[1].split('注册资本')[0].replace(" ", "").replace("\n", ""))
    except:
        Date_of_Establishment.append('无法收集')

    # 注册资本
    registered_capital = []
    try:
        registered_capital.append(message.split('注册资本')[1].split('实缴资本')[0].replace(' ', '').replace("\n", ""))
    except:
        registered_capital.append('无法收集')

    # 实缴资本
    contributed_capital = []
    try:
        contributed_capital.append(message.split('实缴资本')[1].split('核准日期')[0].replace(' ', '').replace('\n', ''))
    except:
        contributed_capital.append('无法收集')

    # 核准日期
    Approved_date = []
    try:
        Approved_date.append(message.split('核准日期')[1].split('组织机构代码')[0].replace(' ', '').replace("\n", ""))
    except:
        Approved_date.append('无法收集')

    # 组织机构代码
    Organization_Code = []
    try:
        Organization_Code.append(message.split('组织机构代码')[1].split('复制')[0].replace(' ', '').replace("\n", ""))
    except:
        Organization_Code.append('无法收集')

    # 工商注册号
    companyNo = []
    try:
        companyNo.append(message.split('工商注册号')[1].split('复制')[0].replace(' ', '').replace("\n", ""))
    except:
        companyNo.append('无法收集')

    # 纳税人识别号
    Taxpayer_Identification_Number = []
    try:
        Taxpayer_Identification_Number.append(
            message.split('纳税人识别号')[1].split('复制')[0].replace(' ', '').replace("\n", ""))
    except:
        Taxpayer_Identification_Number.append('无法收集')

    # 企业类型
    enterprise_type = []
    try:
        enterprise_type.append(message.split('企业类型')[1].split('营业期限')[0].replace('\n', '').replace(' ', ''))
    except:
        enterprise_type.append('无法收集')

    # 营业期限
    Business_Term = []
    try:
        Business_Term.append(message.split('营业期限')[1].split('纳税人资质')[0].replace('\n', '').replace(' ', ''))
    except:
        Business_Term.append('无法收集')

    # 纳税人资质
    Taxpayer_aptitude = []
    try:
        Taxpayer_aptitude.append(message.split('纳税人资质')[1].split('所属行业')[0].replace(' ', '').replace("\n", ""))
    except:
        Taxpayer_aptitude.append('无法收集')

    # 所属行业
    sub_Industry = []
    try:
        sub_Industry.append(message.split('所属行业')[1].split('所属地区')[0].replace('\n', '').replace(' ', ''))
    except:
        sub_Industry.append('无法收集')

    # 所属地区
    sub_area = []
    try:
        sub_area.append(message.split('所属地区')[1].split('登记机关')[0].replace(' ', '').replace("\n", ""))
    except:
        sub_area.append('无法收集')

    # 登记机关
    Registration_Authority = []
    try:
        Registration_Authority.append(message.split('登记机关')[1].split('人员规模')[0].replace(' ', '').replace("\n", ""))
    except:
        Registration_Authority.append('无法收集')

    # 人员规模
    staff_size = []
    try:
        staff_size.append(message.split('人员规模')[1].split('参保人数')[0].replace(' ', '').replace('\n', ''))
    except:
        staff_size.append('无法收集')

    # 参保人数
    Number_of_participants = []
    try:
        Number_of_participants.append(message.split('参保人数')[1].split('趋势图')[0].replace(' ', '').replace("\n", ""))
    except:
        Number_of_participants.append('无法收集')

    # 曾用名
    Used_Name = []
    try:
        Used_Name.append(message.split('曾用名')[1].split('英文名')[0].replace(' ', '').replace("\n", ""))
    except:
        Used_Name.append('无法收集')

    # 英文名
    English_name = []
    try:
        English_name.append(message.split('英文名')[1].split('进出口企业代码')[0].replace('\n', '').replace(' ', ''))
    except:
        English_name.append('无法收集')

    # 进出口企业代码
    import_and_export_code = []
    try:
        import_and_export_code.append(message.split('进出口企业代码')[1].split('复制')[0].replace(' ', '').replace("\n", ""))
    except:
        import_and_export_code.append('无法收集')

    # 注册地址
    register_adress = []
    try:
        register_adress.append(message.split('注册地址')[1].split('附近企业')[0].replace(' ', '').replace("\n", ""))
    except:
        register_adress.append('无法收集')

    # 经营范围
    Business_Scope = []
    try:
        Business_Scope.append(message.split('经营范围')[1].replace(' ', '').replace("\n", ""))
    except:
        Business_Scope.append('无法收集')


    df = pd.DataFrame({'统一社会信用代码': uscc, \
                       '企业名称': list_companys, \
                       '法定代表人': Legal_Person, \
                       '登记状态': Registration_status, \
                       '成立日期': Date_of_Establishment, \
                       '注册资本': registered_capital, \
                       '实缴资本': contributed_capital, \
                       '核准日期': Approved_date, \
                       '组织机构代码': Organization_Code, \
                       '工商注册号': companyNo, \
                       '纳税人识别号': Taxpayer_Identification_Number, \
                       '企业类型': enterprise_type, \
                       '营业期限': Business_Term, \
                       '纳税人资质': Taxpayer_aptitude,
                       '所属行业': sub_Industry, \
                       '所属地区': sub_area, \
                       '登记机关': Registration_Authority, \
                       '人员规模': staff_size, \
                       '参保人数': Number_of_participants, \
                       '曾用名': Used_Name, \
                       '英文名': English_name, \
                       '进出口企业代码': import_and_export_code, \
                       '注册地址': register_adress, \
                       '经营范围': Business_Scope
                       },index=[0])

    return df


async def num_to_df(det,key,uscc):
    det2 = det.replace(' ', '').replace('\n','')
    parts = det2.split('>')
    results = []
    for part in parts:
        if '<' in part:
            results.append(part.split('<')[0])
    r = []
    for i in results:
        if len(i) > 0:
            r.append(i)
    fine_num_now = []
    fine_num_his = []
    try:
        if r[0] == '行政处罚':
            fine_num_now.append(r[1])
            try:
                fine_num_his.append(r[3])
            except:
                pass
            # fine_num_his.append(r[3])
        else:
            pass
    except:
        fine_num_now.append('无法收集')
        fine_num_his.append('无法收集')
    df1 = pd.DataFrame({'uscc': uscc, \
                        '企业名': key, \
                        '行政处罚': fine_num_now, \
                        '历史行政处罚': fine_num_his}, index=[0])
    return df1


async def main():
    browser = await launch(headless=False, dumpio=True, args=["--start-maximized",
                                                              "--no-sandbox",
                                                              "--disable-infobars",
                                                              "--ignore-certificate-errors",
                                                              "--log-level=3",
                                                              "--enable-extensions",
                                                              "--window-size=1920,1080",
                                                              "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"])


    page = await browser.newPage()
    await page.setViewport({'width': 1920, 'height': 1040})
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299')
    await page.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')

    await page.goto('https://www.qcc.com/?utm_source=baidu1&utm_medium=cpc&utm_term=pzsy')

    await asyncio.sleep(5)

    print("登录成功")

    await asyncio.sleep(5)
    n = 0
    # len(tb['supplier_register_name']
    for i in range(0,len(tb['company_name'])):
        # n += 1

        # if i >=26:
                # and i <=7:
            print(tb['company_name'][i])
            key = tb['company_name'][i]
            uscc= tb['uscc'][i]

#######################

            await page.focus("input#searchKey")  # searchKey

            await page.keyboard.type(uscc, {'delay': 100})
            await asyncio.sleep(2)
            await page.keyboard.press('Enter')
            await asyncio.sleep(3)
            print("检索成功")

            element = await page.querySelector('span.copy-title > a')
            if element:
                await page.goto(await(await element.getProperty('href')).jsonValue())  # 获取href属性
                await asyncio.sleep(3)
                content = await page.content()
                tree = etree.HTML(content)
                try:
                    table = tree.xpath('//div[@class="nav-head"]/a')       #nav-head
                    if len(table)==8:
                        table2=table[2]    #经营风险
                        table1=table[0]    #基本信息
                        # 基本信息
                        if table1.attrib.get('href') != "javascript:;":
                            await page.goto('https://www.qcc.com{}'.format(table1.attrib.get('href')))
                            await asyncio.sleep(3)
                            content1 = await page.content()
                            details_soup = BeautifulSoup(content1, features="html.parser")
                            message = details_soup.find_all({'table': 'ntable'})[0].text
                            df_upper = await message_risk_to_df(message, key)
                            df_upper.to_csv("info0213.csv", mode='a', index=False, encoding="utf_8_sig")
                        else:
                            pass
                        # 经营风险
                        if table2.attrib.get('href') != "javascript:;":
                            await page.goto('https://www.qcc.com{}'.format(table2.attrib.get('href')))
                            await asyncio.sleep(3)
                            content2 = await page.content()
                            tree2 = etree.HTML(content2)
                            try:
                                #详细处罚信息

                                detail = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[2]')[0]
                                detail1 = detail.xpath('.//table[@class = "ntable"]')[0]
                                det = etree.tostring(detail1, encoding='utf8')
                                df = pd.read_html(det, encoding='utf8')[0]
                                df['uscc'] = uscc
                                df['name'] = key
                                df.to_csv("fine_detail0213.csv", mode='a', index=False, encoding="utf_8_sig")
                            except:
                                detail = tree2.xpath('//table[@class ="ntable"]')[0]
                                # detail = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[2]/table')[1]
                                det = etree.tostring(detail, encoding='utf8')
                                df = pd.read_html(det, encoding='utf8')[0]
                                if df.columns.tolist()[1] == '决定文书号':
                                    df['uscc'] = uscc
                                    df['name'] = key
                                    df.to_csv("fine_detail0213.csv", mode='a', index=False, encoding="utf_8_sig")
                                else:
                                    pass
                            try:
                                #处罚数
                                num = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[1]/span[1]')[0]
                                det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                      method="html").decode(
                                    "utf-8")
                                df_num = await num_to_df(det2,key,uscc)
                                df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")
                            except:
                                num = tree2.xpath('//div[@class="tcaption"]')[1]
                                try:
                                    num = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[1]/span[1]')[0]
                                    det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                          method="html").decode(
                                        "utf-8")
                                    df_num = await num_to_df(det2,key,uscc)
                                    df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")

                                except:
                                    num = tree2.xpath('//section[@id="adminpenaltylist"]/div/div[1]')[0]
                                    det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                          method="html").decode("utf-8")
                                    df_num = await num_to_df(det2, key, uscc)
                                    df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")
                                # pass

                        else:
                            pass
                    elif len(table)==9:
                        table2=table[3]
                        table1 = table[1]  # 基本信息
                        # 基本信息
                        if table1.attrib.get('href') != "javascript:;":
                            await page.goto('https://www.qcc.com{}'.format(table1.attrib.get('href')))
                            await asyncio.sleep(4)
                            content1 = await page.content()
                            details_soup = BeautifulSoup(content1, features="html.parser")
                            message = details_soup.find_all({'table': 'ntable'})[0].text
                            df_upper = await message_risk_to_df(message, key)
                            df_upper.to_csv("info0213.csv", mode='a', index=False, encoding="utf_8_sig")
                        else:
                            pass
                            # 经营风险
                        if table2.attrib.get('href') != "javascript:;":
                            await page.goto('https://www.qcc.com{}'.format(table2.attrib.get('href')))
                            await asyncio.sleep(3)
                            content2 = await page.content()
                            tree2 = etree.HTML(content2)
                            try:
                                # 详细处罚信息
                                # detail = tree2.xpath('//table[@class ="ntable"]')[0]
                                # det = etree.tostring(detail, encoding='utf8')
                                detail = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[2]')[0]
                                detail1 = detail.xpath('.//table[@class = "ntable"]')[0]
                                det = etree.tostring(detail1, encoding='utf8')
                                df = pd.read_html(det, encoding='utf8')[0]
                                df['uscc'] = uscc
                                df['name'] = key
                                df.to_csv("fine_detail0213.csv", mode='a', index=False, encoding="utf_8_sig")
                            except:
                                detail = tree2.xpath('//table[@class ="ntable"]')[0]
                                # detail = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[2]/table')[1]
                                det = etree.tostring(detail, encoding='utf8')
                                df = pd.read_html(det, encoding='utf8')[0]
                                if df.columns.tolist()[1] == '决定文书号':
                                    df['uscc'] = uscc
                                    df['name'] = key
                                    df.to_csv("fine_detail0213.csv", mode='a', index=False, encoding="utf_8_sig")
                                else:
                                    pass
                            try:
                                # 处罚数
                                num = tree2.xpath('//section[@id="adminpenaltylist"]/div[2]/div[1]/span[1]')[0]
                                det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                      method="html").decode(
                                    "utf-8")
                                df_num = await num_to_df(det2, key, uscc)
                                df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")
                            except:
                                # num = tree2.xpath('//div[@class="tcaption"]')[0]
                                try:
                                    num = tree2.xpath('//div[@class="tcaption"]')[1]
                                    det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                          method="html").decode("utf-8")
                                    df_num = await num_to_df(det2,key,uscc)
                                    df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")

                                except:
                                    num = tree2.xpath('//section[@id="adminpenaltylist"]/div/div[1]')[0]
                                    det2 = etree.tostring(num, encoding="utf-8", pretty_print=True,
                                                          method="html").decode("utf-8")
                                    df_num = await num_to_df(det2, key, uscc)
                                    df_num.to_csv("fine_num_add.csv", mode='a', index=False, encoding="utf_8_sig")

                        else:
                            pass
                    else:
                        pass
                except:
                    pass
            else:
                await page.click('a:nth-child(1) > img')
                await asyncio.sleep(6)
                continue
            await page.focus('a:nth-child(1) > img')
            await page.click('a:nth-child(1) > img')
            await asyncio.sleep(6)
        # elif i<26:
        #     break
        # else:
        #     continue


if __name__ == '__main__':
    tb = DataFrame(o.get_table("bi_pis.tmp_add2")).to_pandas()
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main())
    loop.run_until_complete(task)
#
#
# wrap up

# df_info = pd.read_csv('info0213.csv',encoding="utf_8_sig")
df_fine_num = pd.read_csv('fine_num_add.csv',encoding="utf_8_sig")
# df_fine_detail = pd.read_csv('fine_detail0213.csv',encoding="utf_8_sig")


#
# df_info = pd.read_csv('info0207.csv',encoding="utf_8_sig")
# df_fine_num = pd.read_csv('fine_num0207.csv',encoding="utf_8_sig")
# df_fine_detail = pd.read_csv('fine_detail0207.csv',encoding="utf_8_sig")

def clean_df(df):
    df = df.drop_duplicates(keep='first')
    df['create_date'] = (date.today()).strftime("%Y-%m-%d")
    df['update_date'] = (date.today()).strftime("%Y-%m-%d")
    df = df.astype('string')
    for i in df.columns:
        df[i] = df[i].fillna('9999')
    return df




# df_info = clean_df(df_info)
df_fine_num = clean_df(df_fine_num)
# df_fine_detail = clean_df(df_fine_detail)

# df_info['统一社会信用代码']
# df_info.dtypes



def info_to_dataworks(df):
    # pt = (date.today()+ timedelta(-1)).strftime("%Y-%m-%d")
    records = np.array(df).tolist()
    t = o.get_table('bi_pis.tmp_supplier_basic_info')
    # t.truncate()
    o.write_table(t, records)
    print('info finished!!!')

info_to_dataworks(df_info)


# df_fine_num.columns
def fine_num_to_dataworks(df):
    # pt = (date.today()+ timedelta(-1)).strftime("%Y-%m-%d")
    df['fine_num_total'] = 0
    order = ['uscc', '行政处罚', '历史行政处罚', 'fine_num_total', '企业名', 'create_date', 'update_date']
    df = df[order]
    records = np.array(df).tolist()
    t = o.get_table('bi_pis.tmp_supplier_fine_num')
    # t.truncate()
    o.write_table(t, records)
    print('num finished!!!')


fine_num_to_dataworks(df_fine_num)





def fine_detail_to_dataworks(df):
    # pt = (date.today()+ timedelta(-1)).strftime("%Y-%m-%d")
    order = ['uscc', 'name', '决定文书号', '处罚事由/违法行为类型', '处罚结果/内容', '处罚单位', '数据来源','处罚日期','create_date','update_date']
    df = df[order]
    records = np.array(df).tolist()
    t = o.get_table('bi_pis.tmp_supplier_fine_detail')
    # t.truncate()
    o.write_table(t, records)
    print('detail finished!!!')

fine_detail_to_dataworks(df_fine_detail)