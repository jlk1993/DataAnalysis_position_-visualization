# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 20:21:02 2019

@author: Administrator
"""

'''
爬取拉钩网数据分析师职位信息

目标城市：
一线城市：北上广深
新一线城市：成杭重武

拉勾网每页显示15个职位，并且搜索某城市某职位最多只显示30页数据
所以，对于搜索某城市时数据总页数达到30页的，可以从城市维度往行政区
维度划分，获取某个城市下所有行政区的职位信息
'''

# encoding: utf-8
import requests
import time
import random
from urllib.request import quote
from bs4 import BeautifulSoup
import pandas as pd

def get_data(page_num, keyword):
    """
        生成post的参数
    """
    if page_num == 1:
        first = "true"
    else:
        first = "false"
    form_data = {
        "first": first,
        "pn": page_num,
        "kd": keyword
    }
    return form_data


def request_page_with_session(session, method, url, data=None):
    '''
    session:会话
    method:请求方式
    url:地址
    data:form_data表单数据
    返回session、总页数和解析页bs
    '''
    #间隔随机时间，设置时间长一点，避免爬取太快被封
    time.sleep(random.randint(10, 30))
    if method == "GET":
        # 获取cookies
        rsp = session.get(url, verify=False)
        # 获取总页数
        doc_info = rsp.text
        #避免断网或者网络没有连接的问题
        if "网络出错啦" in doc_info:
            print("retry...")
            rsp = session.get(url, verify=False)
            doc_info = rsp.text
        #解析网页
        bs = BeautifulSoup(doc_info, 'lxml')
         #当前城市下页面总数
        total_num = int(bs.find('div', class_ = 'page-number').find('span', class_='span totalNum').text)

        return session, total_num, bs
    elif method == "POST":
        r = session.post(url, data=data, verify=False)
        all_jobs = r.json()['content']['positionResult']['result']
         #解析json数据，提取需要的字段信息
        position_info = []
        for job in all_jobs:
            job_info = []
            '''
            职位信息
            '''
            job_info.append(job['positionName']) #职位名称
            job_info.append(job['salary']) #职位薪资
            job_info.append(job['positionLables']) #职位标签
            job_info.append(job['workYear']) #经验要求
            job_info.append(job['positionAdvantage']) #职位福利
            job_info.append(job['education']) #学历要求
            job_info.append(job['jobNature']) #职位属性（全职、兼职、实习）
            job_info.append(job['skillLables']) #技能标签
            job_info.append(job['city']) #所在城市
            '''
            公司信息
            '''
            job_info.append(job['companyFullName']) #公司全称
            job_info.append(job['companyShortName']) #公司简称
            job_info.append(job['companyLabelList']) #公司标签
            job_info.append(job['companySize']) #公司规模
            #job_info.append(job['createTime']) #职位创建日期
            job_info.append(job['district']) #公司地点
            job_info.append(job['financeStage']) #融资情况
            job_info.append(job['industryField']) #所属行业领域
            job_info.append(job['businessZones']) #所在商圈
            '''
            其他信息
            '''
            job_info.append(job['firstType']) #职位第一类型
            job_info.append(job['secondType']) #职位第二类型
            job_info.append(job['thirdType']) #职位第三类型
            job_info.append(job['latitude']) #公司地点纬度
            job_info.append(job['longitude']) #公司地点经度
            job_info.append(job['resumeProcessDay']) #简历处理用时
            job_info.append(job['resumeProcessRate']) #简历处理率
            
            position_info.append(job_info)
        #返回多个职位信息的列表[[job1],[job2],...]
        return position_info


def get_positions_from_city(city_name, job, file_path):
    '''
    city_name:城市名称
    job_name:职位名称
    爬取指定城市中指定职位信息的数据
    '''
    #起始地址
    #quote()函数转换为搜索关键字为'%xxxx'的形式
    start_url = "https://www.lagou.com/jobs/list_{}?city={}&cl=false&fromSearch=true&labelWords=&suginput=".format(
        quote(job), quote(city_name))
    city_api_url = "https://www.lagou.com/jobs/positionAjax.json?city={}&needAddtionalResult=false".format(quote(city_name))
    #请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
        "Referer": start_url,
        "Content-Type": "application/x-www-form-urlencoded;charset = UTF-8"
    }
    #创建一个session对象
    ss = requests.session()
    # 更新 Headers
    ss.headers.update(headers)
    # 获取结果页数 以及返回 session ，以及解析页
    ss, total_num, bs = request_page_with_session(ss, 'GET', start_url)
    print(city_name, '总页数: ', total_num)
    
    #列名列表
#    column_lst = []
    #如果页数小于30，则直接获取职位信息，不再考虑往行政区划分  
    if 0 < total_num < 30:
        for i in range(1, total_num + 1):
            print('===正在处理{} 第{}/{}==='.format(city_name, i, total_num))
            # 使用 GET 获取 session
            city_ss = requests.session()
            city_ss.headers.update(headers)
            city_ss, city_num, city_bs = request_page_with_session(city_ss, 'GET', start_url)
            #间隔随机时间
            time.sleep(random.randint(5,10))
            # post 方式获取API结果
            post_data = get_data(i, job)
            post_result = request_page_with_session(city_ss, 'POST', city_api_url, post_data)
            #将结果保存到本地文件
            print('===正在保存{} 第{}/{}===职位信息'.format(city_name, i, total_num))
            pd.DataFrame(post_result).to_csv(file_path,encoding='utf_8_sig',index=False,mode='a',header=False)
    
    #页数到达30页，说明该城市职位总数至少是450，所以再按行政区划分（其实只有北京的职位数是超过450）
    elif total_num == 30:
        # 获取该城市所有的行政区
        district_list = []
        district_element = bs.find("div", attrs={"class": "contents", "data-type": "district"}).find_all("a")[1:] #去掉'不限'
        for district in district_element:
            district_list.append(district.string)
#            district_name = district.string
#            district_list.append(district_name)
        print('{}总共有{}个行政区'.format(city_name,len(district_list)))
        # 按行政区获取职位列表
        for district_name in district_list:
            district_url = "https://www.lagou.com/jobs/list_{}?px=default&city={}&district={}".format(
                quote(job), quote(city_name), quote(district_name))
            ss = requests.session()
            ss.headers.update(headers)
            ss, dist_page_num, dist_bs = request_page_with_session(ss, 'GET', district_url)
            dist_api_url = "https://www.lagou.com/jobs/positionAjax.json?city={}&district={}&" \
                            "needAddtionalResult=false".format(quote(city_name), quote(district_name))
            for i in range(1, dist_page_num + 1):
                print('===正在处理{}{} 第{}/{}==='.format(city_name, district_name, i, dist_page_num))
                # 使用 GET 获取 session
                dist_ss = requests.session()
                dist_ss.headers.update(headers)
                dist_ss, dist_num, dist_soup = request_page_with_session(dist_ss, 'GET', start_url)
                # post 方式获取API结果
                post_data = get_data(i, job)
                #间隔随机时间
                time.sleep(random.randint(5,10))
                post_result = request_page_with_session(dist_ss, 'POST', dist_api_url, post_data)
                #将结果保存到本地文件
                print('===正在保存{}{} 第{}/{}职位信息==='.format(city_name, district_name, i, dist_page_num))
                pd.DataFrame(post_result).to_csv(file_path,encoding='utf_8_sig',index=False,mode='a',header=False)
    
            
    else:
        print("{} 总页数: {}/{}".format(city_name, 0, total_num))

    return city_name


def main():
    '''
        主函数
    '''
    #把列名写入到csv文件
    pd.DataFrame(column_lst).to_csv(file_path,encoding='utf_8_sig',index=False,mode='a',header=False)
    job = "数据分析"
     #完成爬取的城市列表
    finished_citys = []
    for city in city_list:
        print('正准备处理{}的职位信息数据，请稍等'.format(city))
        if city in finished_citys:
            continue
        else:
            city_name = get_positions_from_city(city, job, file_path)
            finished_citys.append(city_name)
            print('===目前已经爬取完毕的城市列表===',finished_citys)
            

if __name__ == '__main__':
    city_list = ['北京','上海','广州','深圳','成都','杭州','重庆','武汉']
    #city_list = ['重庆','成都']
    #本地文件保存路径
    file_path = 'C:/Users/Administrator/Desktop/lagou.csv'
    
    #存储信息列名列表
    column_lst = [['positionName','salary','positionLables','workYear','positionAdvantage','education','jobNature','skillLables','city',
                  'companyFullName','companyShortName','companyLabelList','companySize','district','financeStage','industryField','businessZones',
                  'firstType','secondType','thirdType','latitude','longitude','resumeProcessDay','resumeProcessRate']]
    main()
