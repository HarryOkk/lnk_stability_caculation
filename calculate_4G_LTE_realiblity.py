"""用于可靠性试验测试装置4G—LTE信号丢包率、延迟、传输速率，本代码用于分析计算装置上行的物模型消息"""
import datetime

import pandas as pd
from remes_aliyun_openapi.iot.thing_model_use import query_device_properties_data
from time import sleep
from datetime import datetime

if __name__ == "__main__":
    # 一些接口调用的基本配置
    device_name = '512130000002'
    iot_instance_id = 'iot-060a02m5'
    product_key = 'g4xdEqa2CfX'
    l_dm_identifier = ['RSRP', 'SINR', 'RSRQ', 'RSSI']
    # 时间格式：YYYY-mm-dd HH:MM:SS,或者使用毫秒级时间戳（int）
    start_time = "2023-09-27 09:00:00"
    end_time = "2023-09-27 14:00:00"

    # 接口单次调用最大返回一百条，所以这个是接口调用的返回列表
    l_responses = query_device_properties_data(
        start_time=start_time,
        asc=1,
        end_time=end_time,
        identifier=l_dm_identifier,
        iot_instance_id=iot_instance_id,
        product_key=product_key,
        device_name=device_name,
    )
    # 该接口QPS最大为10
    sleep(10)

    df_total = pd.DataFrame
    # 遍历每次接口调用的结果，并做解析
    for d_response in l_responses:
        df_total_temp = pd.DataFrame
        l_dm_dates = d_response['body']['PropertyDataInfos']['PropertyDataInfo']
        for dm_data in l_dm_dates:
            # 物模型名称：RSRP/SINR/RSRQ/RSSI
            dm_name = dm_data['Identifier']
            # 对应设备单个物模型属性的数据内容，List[Dict]
            l_dm_datas = dm_data['List']['PropertyInfo']
            df_temp = pd.DataFrame(l_dm_datas)
            df_temp.rename(columns={'Value': dm_name}, inplace=True)
            if df_total_temp.empty:
                df_total_temp = df_temp
            else:
                df_total_temp = df_total_temp.merge(df_temp, on='Time', how='outer')
        if df_total.empty:
            df_total = df_total_temp
        else:
            df_total = pd.concat([df_total, df_total_temp], axis=0, ignore_index=True)
    df_total['Time'] = pd.to_datetime(df_total['Time'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
    df_total['TimeDiff'] = df_total['Time'].diff().dt.total_seconds()

    """计算网络抖动（延迟）传输时间"""
    df_total['TimeDiffAbs'] = abs((df_total['TimeDiff'] - 1) * 1000)
    packet_upload_jitter = df_total['TimeDiffAbs'].describe()

    """计算丢包率"""
    start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    time_difference_seconds = (end_datetime - start_datetime).total_seconds()
    packet_loss_rate = str(1 - len(df_total) / time_difference_seconds) + '%'
    print(f'数据采样时间从{start_time} - {end_time}\n'
          f'数据包上传抖动情况（ms）：{packet_upload_jitter}\n'
          f'应收数据包数量：{time_difference_seconds}\n'
          f'实收数据包数量：{len(df_total)}\n'
          f'丢包率：{packet_loss_rate}')

    df_total.to_csv(r'D:\my_documents\Tableau_python_data\4G-LTE_realiblity\4G-LTE_realiblity.csv')
