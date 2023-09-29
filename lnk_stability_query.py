import pandas as pd
from remes_mysql.db_config import AliyunBizDb


def query_lnk_ele_info():
    try:
        sql = """
                select
                    distinct zecn.ele_id,
                    tce.cpd_id,
                    tce.created_date ,
                    tce.status,
                    tce.user_id,
                    reb.ele_contract_no ,
                    reb.customer_name,
                    reb.mnt_build_name,
                    zec.ele_local_name
                from
                    zhdt_data_db.zhdt_ele_commerce_new zecn
                left join remes_db.t_cpd_elevator tce 
                on
                    zecn.ele_id = tce.ele_id
                left join remes_db.remes_elevator_base reb 
                on
                    zecn.ele_id = reb.ele_id
                left join zhdt_view_db.zv_elevator_config zec 
                on
                    zecn.ele_id = zec.ele_id
                where
                    zecn.pot_id = 1000
                    and
                    zecn.has_commerce_flag = 'Y'
                    and 
                    tce.cpd_id is not null ;
        """
        print('sql准备完成，正在查')
        try:
            df_lnk_list = AliyunBizDb().read_data(sql=sql)
        except Exception as error:
            raise error

        if len(df_lnk_list) > 0:
            pass
        else:
            print(f"rds数据库查询到的内容为空，请检查")
    except Exception as error:
        raise


if __name__ == "__main__":
    df_4g_analyse = pd.read_csv(r"\\smecnas3.smec-cn.com\k2data_share\wireless_call_device_signal"
                                r"\wireless_call_device_signal.csv", low_memory=False, parse_dates=['TIMESTAMP'],
                                dtype={'DEVICENAME': str, 'LICFLAG': str, 'DATATYPE': str, 'VALUE': str, 'REASON': str},
                                encoding='utf-8', skiprows=0)
    # 按日期时间字段排序
    df_4g_analyse.sort_values(by='TIMESTAMP', ascending=False, inplace=True)
    max_time = df_4g_analyse.iloc[0]['TIMESTAMP']

    df_4g_analyse.sort_values(by='TIMESTAMP', inplace=True)

    # 获取最小时间和最大时间
    min_time = df_4g_analyse.iloc[0]['TIMESTAMP']
    # 布尔索引的筛选形式
    df_offline = df_4g_analyse[df_4g_analyse['VALUE'] == 'offline']
    print(f"离线的数量为:{df_offline.count()}")

    # 过滤所有RSRP的上传记录
    df_filtered = df_4g_analyse[(df_4g_analyse['DATATYPE'] == 'RSRP') | ((df_4g_analyse['DATATYPE'] == 'OPERATION') &
                                                                         (df_4g_analyse['VALUE'] == 'offline'))]

    count_unique_devicenames = df_filtered['DEVICENAME'].nunique()
    count_unique_reasons = df_filtered['REASON'].nunique()

    """生成首时间记录和尾时间记录，添加到所有device_name中"""
    # 获取设备名称的唯一聚类值
    unique_device_names = df_filtered['DEVICENAME'].unique()

    # 创建一个 Series 对象，将唯一设备名称作为数据，不设置索引
    device_names_series = pd.Series(unique_device_names, name='DEVICENAME')

    # 创建一个新的 DataFrame 包含最小和最大时间戳记录，将 Series 与时间戳一起添加
    df_output_max = pd.DataFrame({'DEVICENAME': unique_device_names, 'TIMESTAMP': max_time})
    df_output_min = pd.DataFrame({'DEVICENAME': unique_device_names, 'TIMESTAMP': min_time})

    # 合并两个 DataFrame
    df_concat_result = pd.concat([df_output_min, df_output_max], axis=0)

    # 既然是造的假的记录，那么除了关键的时间戳信息外，其他值都应该填入Null，以防止其参与计算
    # 将生成的最大最小时间戳加入到装置清单中
    df_concat_result['DATATYPE'] = 'RSRP'
    df_filtered = pd.concat([df_filtered, df_concat_result], axis=0)


    """计算聚类后相邻两个数据点之间的时间间隔"""
    df_filtered['TimeDiff'] = df_filtered.groupby(['DEVICENAME', 'DATATYPE'])['TIMESTAMP'].diff().dt.total_seconds()


    """对df_filtered计算每个时间间隔内是否有记录"""
    df_offline_and_filtered = pd.concat([df_filtered, df_offline], ignore_index=True)
    df_offline_and_filtered.sort_values(by='TIMESTAMP', inplace=True)


    df_offline_record = df_filtered
    df_offline_record.to_csv(r"D:\my_documents\Tableau_python_data\SLA离线时长精确计算.csv")
    pass
