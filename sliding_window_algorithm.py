import math

import pandas as pd
import numpy as np
from datetime import datetime
if __name__ == "__main__":
    df_4g_analyse = pd.read_csv(r"\\smecnas3.smec-cn.com\k2data_share\wireless_call_device_signal\wireless_call_device_signal.csv", low_memory=False, parse_dates=['TIMESTAMP'],
                                dtype={'DEVICENAME': str, 'LICFLAG': str, 'DATATYPE': str, 'VALUE': str, 'REASON': str},
                                encoding='utf-8', skiprows=0)
    # 按日期时间字段排序
    df_4g_analyse.sort_values(by='TIMESTAMP', ascending=False, inplace=True)
    max_time = df_4g_analyse.iloc[0]['TIMESTAMP']

    df_4g_analyse.sort_values(by='TIMESTAMP', inplace=True)

    # 获取最小时间和最大时间
    min_time = df_4g_analyse.iloc[0]['TIMESTAMP']


    # 以设备名称和数据类型分组
    grouped = df_4g_analyse[df_4g_analyse['DATATYPE'] == 'RSRP'].groupby('DEVICENAME')

    # 初始化一个字典，用于存储每个设备的离线窗口数
    offline_window_count = {}

    # 定义窗口大小（6分钟）
    window_size = pd.Timedelta(minutes=6)

    # 初始化总的窗口数量和总的离线窗口数量
    total_window_count = 0
    total_offline_window_count = 0
    # 遍历每个设备的数据
    for device_name, group in grouped:
        # 初始化一个列表，用于记录窗口是否在线
        windows_online = []

        # 获取该设备的最早时间
        min_device_time = group['TIMESTAMP'].min()

        # 计算时间窗口的起始时间
        window_start = min_time

        # 遍历时间窗口
        while window_start <= min_device_time:
            # 计算窗口结束时间
            window_end = window_start + window_size

            # 检查窗口内是否存在记录
            records_in_window = group[(group['TIMESTAMP'] >= window_start) & (group['TIMESTAMP'] < window_end)]

            # 如果存在记录，则认为窗口在线
            if not records_in_window.empty:
                windows_online.append(True)
            else:
                windows_online.append(False)

            # 移动窗口
            window_start = window_end
        # 统计总的窗口数量
        total_window_count += len(windows_online)
        # 统计离线窗口数
        offline_count = np.sum(np.logical_not(windows_online))
        # 统计总的离线窗口数量
        total_offline_window_count += offline_count



        # 存储离线窗口数
        offline_window_count[device_name] = offline_count

    # 打印每个设备的离线窗口数
    for device_name, offline_count in offline_window_count.items():
        print(f"设备名称: {device_name}, 离线窗口数: {offline_count}")

    """计算SLA"""
    # 打印总的窗口数量和总的离线窗口数量
    max_time = datetime.strptime(max_time, "%Y-%m-%d %H:%M:%S")
    min_time = datetime.strptime(min_time, "%Y-%m-%d %H:%M:%S")
    time_diff = (max_time - min_time).total_seconds()
    total_window_count = math.floor(int(time_diff/360)*len(offline_window_count.keys()))


    print(f"总的窗口数量: {total_window_count}")
    print(f"总的离线窗口数量: {total_offline_window_count}")
    df_result = pd.DataFrame(offline_window_count)
    df_result.to_csv(r"D:\my_code\lnk_stability_calculation\sliding_window_algorithm.csv")