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
        hive_conn = dbhelper.DbHelper(db_type="hive", db_mode="sc_hive")
        bd_sum_sql = """
select project_name_adviser                                                                      `项目简称`,
       c.task                                                                                    `随机目标`,
       nvl(`三方`, 0)                                                                              `三方`,
       nvl(`知情`, 0)                                                                              `知情`,
       nvl(`随机`, 0)                                                                              `随机`,
       concat(round(if(c.task = 0, 0, nvl(`随机`, 0) / c.task) * 100, 1), '%')                     `随机达成`,
       concat(round(dayofmonth(current_date()) / (day(last_day(current_date()))) * 100, 1), '%') `时间进度`
from (
         select *
         from tmp.tmp_task
         where dt = '2021-07-30'
     ) c --任务信息表
         left join
     (
         select project_num,
                substr(`日期`, 1, 7)   `月份`,
                sum(nvl(`提报三方数`, 0)) `三方`,
                sum(nvl(`知情成功`, 0))  `知情`,
                sum(nvl(`随机成功`, 0))  `随机`
         from (
                  select project_id
                       , project_num
                       , to_DAte(submit_crc_time)      `日期`
                       , count(distinct patient_id) as `提报三方数`
                       , 0                          as `知情成功`
                       , 0                          as `随机成功`
                  from shuidi_dwb.dwb_cro_user_project_schedule_full_d
                  where dt = current_date()
                    and submit_crc_time is not null
                  group by project_id,
                           project_num,
                           to_DAte(submit_crc_time)
                  union
                  select project_id
                       , project_num
                       , to_DAte(icf_obtain_time)      `日期`
                       , 0                          as `提报三方数`
                       , count(distinct patient_id) as `知情成功`
                       , 0                          as `随机成功`
                  from shuidi_dwb.dwb_cro_user_project_schedule_full_d
                  where dt = current_date()
                    and icf_obtain_time is not null
                    and icf_obtain_status = '知情成功'
                  group by project_id,
                           project_num,
                           to_DAte(icf_obtain_time)
                  union
                  select project_id
                       , project_num
                       , to_DAte(randomized_time)      `日期`
                       , 0                          as `提报三方数`
                       , 0                          as `知情成功`
                       , count(distinct patient_id) as `随机成功`
                  from shuidi_dwb.dwb_cro_user_project_schedule_full_d
                  where dt = current_date()
                    and randomized_time is not null
                    and randomized_status = '随机成功'
                  group by project_id,
                           project_num
                          , to_DAte(randomized_time)
              ) a
         where substr(`日期`, 1, 7) = substr(current_date(), 1, 7)
         group by project_num, substr(`日期`, 1, 7)
     ) a
     on C.project_num = a.project_num
         left join
     (
         select project_id
              , project_name_adviser
              , project_number
         from shuidi_ods.ods_cro_recruit_project_manage_ext_d
         where dt = current_date()
     ) b --项目信息拓展表
     on c.project_num = b.project_number
where project_name_adviser is not null
order by `项目简称` asc
limit 50
"""
        sum_str = "|\n"

        result = hive_conn.query(bd_sum_sql)
        time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        zt_res = []
        data = []
        title = [u"项目简称", u"随机目标", u"三方", u"知情", u"随机", u"随机达成", u"时间进度"]
        data.append(title)
        for value in result:
            data.append([unicode(str(i), "utf-8") for i in value])
        zt_res.append({'title': u'各项目本月业绩达成进展情况test', 'data': data})
        drawimage = DrawTableImage(image_name=time_str + 'zt_res.png', col_data_width=100,
                                   col_data_width_list=[300, 80, 80, 80, 80, 80, 80])
        image = drawimage.create_table_img(zt_res)
        sendFileToGroup("wx-alarm-prod-20210913-0002", '各项目本月业绩达成进展情况test', image)
        # sendFileToGroup("wx-alarm-prod-20210420-0001", '各项目本月业绩达成进展情况', image) --测试

    except Exception, ex:
        raise Exception(str(ex))