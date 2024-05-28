'''
Description: 
Date: 2024-03-30 09:37:43
LastEditors: yupengyang yupengyang@topscomm.com
LastEditTime: 2024-05-28 15:26:48
Author: 
'''
import os
import pandas as pd
import numpy as np
import json

def check_dates(dates):
    # 获取第一个子列表
    first_date = dates[0]
  
    # 遍历列表
    for date in dates:
        # 检查子列表是否与第一个子列表相等
        if not np.array_equal(first_date, np.array(date)):
            raise ValueError("子列表不一致")
    
    # 返回这个列表
    return first_date

def complete_time(date_str):
    if len(date_str) <= 10:  # 如果日期长度<10，说明只有日期部分
        return date_str + ' 0:00:00'
    return date_str

def process_meters():
    meter_nos = []
    dates = []
    wh_datas = [] 

    # 定义完整的15分钟间隔日期范围
    date_range = pd.date_range(start='2023-10-01 0:00:00', end='2024-03-01 23:45:00', freq='15T')
    date_range_str = date_range.strftime('%Y-%m-%d %H:%M:%S')

    # 遍历文件夹，处理每个文件
    for filename in os.listdir('子表'):
        # 读取xlsx文件
        df = pd.read_excel(f"./子表/{filename}")

        # 检查 '电表号' 列是否全相同
        meter_no_unique = df['电表号'].unique()
        if len(meter_no_unique) != 1:
            raise ValueError("电表号列中的值不全相同")

        # 将 '电表号' 列的值保存为字符串格式
        meter_no = meter_no_unique[0]

        # 处理 '数据时间' 列，将日期格式处理为字符串数组
        df['数据时间'] = df['数据时间'].apply(complete_time)
        df['数据时间'] = pd.to_datetime(df['数据时间'], format='%Y/%m/%d %H:%M:%S')

        # 创建一个包含完整日期范围的DataFrame，并合并现有数据
        complete_df = pd.DataFrame({'数据时间': date_range})
        complete_df = complete_df.merge(df, on='数据时间', how='left')

        # 用0填充缺失值
        complete_df['电表号'] = meter_no
        complete_df['反向有功总'] = complete_df['反向有功总'].fillna(0)
        complete_df['正向有功总'] = complete_df['正向有功总'].fillna(0)

        complete_df = complete_df.sort_values(by='数据时间')

        date = complete_df['数据时间'].dt.strftime('%Y-%m-%d %H:%M:%S').values
        wh_data_reverse = complete_df['反向有功总'].values
        wh_data_forward = complete_df['正向有功总'].values

        wh_data_reverse = np.where(wh_data_reverse == ' ', 0, wh_data_reverse).astype(float)
        wh_data_forward = np.where(wh_data_forward == ' ', 0, wh_data_forward).astype(float)
        
        wh_data = abs(wh_data_reverse - wh_data_forward).transpose()

        for i in range(wh_data.size-1, 0, -1):
            if wh_data[i] == 0 or wh_data[i-1] == 0:
                wh_data[i] = np.nan
            else:
                wh_data[i] = (wh_data[i] - wh_data[i-1]) * 1320000

        wh_data[0] = np.nan

        meter_nos.append(meter_no)
        dates.append(date)
        wh_datas.append(wh_data)

    try:
        date = check_dates(dates)
        print("所有子列表一致")
    except ValueError as e:
        print(e)
    
    meter_nos = np.array(meter_nos).transpose()
    wh_datas = np.array(wh_datas).transpose()
    return date, meter_nos, wh_datas

def process_branch_nosum():
    meter_nos = []
    dates = []
    wh_datas = [] 

    # 定义完整的15分钟间隔日期范围
    date_range = pd.date_range(start='2023-10-01 0:00:00', end='2024-03-01 23:45:00', freq='15T')
    date_range_str = date_range.strftime('%Y-%m-%d %H:%M:%S')

    # 遍历文件夹，处理每个文件
    for filename in os.listdir('总表'):
        # 读取xlsx文件
        df = pd.read_excel(f"./总表/{filename}")

        # 检查 '电表号' 列是否全相同
        meter_no_unique = df['电表号'].unique()
        if len(meter_no_unique) != 1:
            raise ValueError("电表号列中的值不全相同")

        # 将 '电表号' 列的值保存为字符串格式
        meter_no = meter_no_unique[0]

        # 处理 '数据时间' 列，将日期格式处理为字符串数组
        df['数据时间'] = df['数据时间'].apply(complete_time)
        df['数据时间'] = pd.to_datetime(df['数据时间'], format='%Y/%m/%d %H:%M:%S')

        # 创建一个包含完整日期范围的DataFrame，并合并现有数据
        complete_df = pd.DataFrame({'数据时间': date_range})
        complete_df = complete_df.merge(df, on='数据时间', how='left')

        # 用0填充缺失值
        complete_df['电表号'] = meter_no
        complete_df['反向有功总'] = complete_df['反向有功总'].fillna(0)
        complete_df['正向有功总'] = complete_df['正向有功总'].fillna(0)

        complete_df = complete_df.sort_values(by='数据时间')

        date = complete_df['数据时间'].dt.strftime('%Y-%m-%d %H:%M:%S').values
        wh_data_reverse = complete_df['反向有功总'].values
        wh_data_forward = complete_df['正向有功总'].values

        wh_data_reverse = np.where(wh_data_reverse == ' ', 0, wh_data_reverse).astype(float)
        wh_data_forward = np.where(wh_data_forward == ' ', 0, wh_data_forward).astype(float)
        
        wh_data = abs(wh_data_reverse - wh_data_forward).transpose()

        if meter_no != '04019SG000000022K0004921' and meter_no != '04019SG000000022K0004920':
            magnification = 1320000
        else:
            magnification = 66000

        for i in range(wh_data.size-1, 0, -1):
            if wh_data[i] == 0 or wh_data[i-1] == 0:
                wh_data[i] = np.nan
            else:
                wh_data[i] = (wh_data[i] - wh_data[i-1]) * magnification  # 乘以1

        wh_data[0] = np.nan

        meter_nos.append(meter_no)
        dates.append(date)
        wh_datas.append(wh_data)

    try:
        date = check_dates(dates)
        print("所有子列表一致")
    except ValueError as e:
        print(e)
    
    meter_nos = np.array(meter_nos).transpose()
    wh_datas = np.array(wh_datas).transpose()
    return date, meter_nos, wh_datas

meter_date, meter_no, meters_data = process_meters()
branch_date, branch_no, branch_data = process_branch_nosum()

data_dict = {
    'Meters': meters_data.tolist(),
    'Branch': branch_data.tolist(),
    'MeterName': meter_no.tolist(),
    'BranchName': branch_no.tolist(),
    'SampleNum': meters_data.shape[0],
    'MeterNum': meters_data.shape[1],
    'time': meter_date.tolist()
}

with open('关口表数据_主表.json', 'w', encoding='utf-8') as f:
    json.dump(data_dict, f, ensure_ascii=False, indent=4)
