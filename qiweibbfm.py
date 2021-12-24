# -*- coding: utf-8 -*-
import sys

sys.path.append("/data/di_log/etl_file/bi-job/realtime/real_time")
sys.path.append("/data/di_log/etl_file/bi-job")
# sys.path.append("/Users/shuidihuzhu/PycharmProjects/bi-job-online/bi-job/realtime/real_time")
# sys.path.append("/Users/shuidihuzhu/PycharmProjects/bi-job-online/bi-job/")
from db_conn import dbhelper
import datetime
import pandas as pd
import ast
from sdutil.SqlHelper import SqlHelper
import json
from sdutil import wx
from urllib3 import encode_multipart_formdata
from sdutil.draw_image.DrawTableImage import DrawTableImage
import requests


def sendFileToGroup(groupId, filename, filepath):
    header = {"Content-Type": "multipart/form-data"}
    data = {
        "groupId": groupId,
        'file': (filename, open(filepath, 'rb').read(), 'image/png')
    }
    # (filename,open(filepath,'rb').read())
    # 转换data数据的类型
    encode_data = encode_multipart_formdata(data)
    data = encode_data[0]
    header['Content-Type'] = encode_data[1]
    res = requests.post('http://alarm.shuiditech.com/innerapi/alarm/send-file-to-group', headers=header, data=data)
    print
    res.text
    if res.status_code != 200:
        print
        res
        exit(-1)


if __name__ == "__main__":
    try:
        i = datetime.datetime.now()
        cur_hour = i.hour
        format_date_one = i.strftime('%Y%m%d')
        if (len(sys.argv) > 1 or i.hour == 21):
            print(i.now())
        else:
            sys.exit()

        hive_conn = dbhelper.DbHelper(db_type="hive", db_mode="sc_hive")
        bd_sum_sql = """
select t1.channel
     , t1.random                                                                                            random_tar        --随机目标
     , nvl(t2.random_cnt, 0)                                                                                random_cnt        --已经随机数量
     , nvl(concat(cast(round(t2.random_cnt / t1.random * 100, 0) as int), '%'),
           '0%')                                                                                            tar_rate          --达成率
     , t1.income                                                                                            income_tar        --营收目标
     , nvl(t2.income_amt, 0)                                                                                                  --营收
     , nvl(concat(cast(round(t2.income_amt / t1.income * 100, 0) as int), '%'),
           '0%')                                                                                            income_rate       --营收达成率
     , concat(cast(round(dayofmonth(current_date()) / day(last_day(current_date())) * 100, 0) as int), '%') time_sche
     , nvl(t2.last_random_cnt, 0)                                                                                             --上月随机数
     , nvl(concat(cast(round((t2.random_cnt - t2.last_random_cnt) / t2.last_random_cnt * 100, 0) as int), '%'),
           '0%')                                                                                            random_ratio_rate --随机环比
     , nvl(t2.last_income_amt, 0)                                                                                             --上月实际营收
     , nvl(concat(cast(round((t2.income_amt - t2.last_income_amt) / t2.last_income_amt * 100, 0) as int), '%'),
           '0%')                                                                                            income_ratio_rate --营收环比

from (
         select *
         from tmp.tmp_income
         where dt = '2021-12-16'
     ) t1
         left join
     (
         select now_user_type
              , count(case
                          when randomized_status = '随机成功' and to_date(randomized_time) >= trunc(current_date(), 'MM')
                              then patient_id
                          else null end) random_cnt
              , cast(round(sum(case
                                   when to_date(randomized_time) >= trunc(current_date(), 'MM') then settle_price
                                   else 0 end) /
                           1.06,
                           0) as int)    income_amt --营收
              , count(case
                          when randomized_status = '随机成功' and to_date(randomized_time) <= add_months(current_date(), -1)
                              then patient_id
                          else null end) last_random_cnt
              , cast(round(sum(case
                                   when to_date(randomized_time) <= add_months(current_date(), -1) then settle_price
                                   else 0 end) /
                           1.06,
                           0) as int)    last_income_amt
         from (
                  select patient_id
                       , case
                             when user_type in (2, 3) then 'to_D'
                             when user_type = 1 then 'Z计划'
                             when user_type in (15, 17, 23) then '第三方'
                             when user_type in (7, 10) then '线上'
                             when user_type in (20, 21) then '内部推荐'
                             else null end as now_user_type
                       , settle_price
                       , randomized_status
                       , randomized_time
                  from shuidi_rpt.rpt_cro_channel_income_info_full_d
                  where dt = current_date()
                    and to_date(randomized_time) >= add_months(trunc(current_date(), 'MM'), -1)
                  union
                  select patient_id
                       , '整体' as now_user_type
                       , settle_price
                       , randomized_status
                       , randomized_time
                  from shuidi_rpt.rpt_cro_channel_income_info_full_d
                  where dt = current_date()
                    and to_date(randomized_time) >= add_months(trunc(current_date(), 'MM'), -1)
              ) t
         group by now_user_type
     ) t2
     on t1.channel = t2.now_user_type
"""

        sum_str = "|\n"

        result = hive_conn.query(bd_sum_sql)

        time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        zt_res = []
        data = []
        title = [u"渠道", u"随机目标", u"已经随机数量", u"达成率", u"营收目标", u"营收", u"营收达成率", u"时间进度", u"上月随机数", u"随机环比", u"上月实际营收",
                 u"营收环比"]
        data.append(title)
        for value in result:
            data.append([unicode(str(i), "utf-8") for i in value])
        zt_res.append({'title': format_date_one + u'招募营收播报', 'data': data})  # 在集群的名字
        drawimage = DrawTableImage(image_name=time_str + 'zt_res.png', col_data_width=100,
                                   col_data_width_list=[80, 95, 110, 85, 90, 100, 95, 105, 105, 90, 110, 100])
        image = drawimage.create_table_img(zt_res)
        sendFileToGroup("wx-alarm-prod-20210510-0002", '招募营收播报' + time_str, image) #长期播报
        sendFileToGroup("wx-alarm-prod-20200113-0002", '招募营收播报' + time_str, image) #重点群（未进）
        #sendFileToGroup("wx-alarm-prod-20210916-0002", '招募营收播报' + time_str, image)

    except Exception, ex:
        raise Exception(str(ex))
