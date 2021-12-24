# -*- coding: utf-8 -*-
import base64
import hashlib
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


def sendImageByBot(url, image_path):
    with open(image_path, "rb") as f:
        fcont = f.read()
        # 转化图片的base64
        ls_f = base64.b64encode(fcont)
        # 计算图片的md5
        fmd5 = hashlib.md5(fcont)
    data = {"msgtype": "image", "image": {"base64": ls_f.decode('utf8'), "md5": fmd5.hexdigest()}}
    data_json = json.dumps(data)
    print('推送的json%s' % data_json)
    res = requests.post(url, data=data_json)
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
        if (len(sys.argv) > 1 or i.hour == 21):
            print(i.now())
        else:
            sys.exit()

        hive_conn = dbhelper.DbHelper(db_type="hive", db_mode="sc_hive")
        bd_sum_sql = """
select a.d_area                                                                                             `区域`, --新
       a.p_area                                                                                             `辖区`,
       case when a.leader_user_name in ('杨成玲', '许明辉', '闫波林') then 'TBA' else a.leader_user_name end as      `leader`,
       nvl(`患者提交`, 0)                                                                                       `患者提交`,
       nvl(`提报三方`, 0)                                                                                       `提报三方`,
       nvl(`知情`, 0)                                                                                         `知情`,
       nvl(`随机`, 0)                                                                                         `随机`,
       random_target                                                                                        `随机目标`,
       nvl(concat(cast(if(random_target = 0, 0, `随机` / cast(random_target as int)) * 100 as int), '%'), 0)  `随机达成`,
       --nvl(cast(`实际营收` as int), 0)                                                                         `实际营收`,
       --income_target                                                                                       `营收目标`,
       concat(cast(round(if(income_target = 0, 0, cast(nvl(`实际营收`, 0) as decimal(10, 2)) / 1.06 / income_target) * 100,
                         0) as int),
              '%')                                                                                          `营收达成`,
       concat(cast(round(dayofmonth(current_date()) / day(last_day(current_date())) * 100, 0) as int), '%') `时间进度`,
       nvl(`知情后进程中`, 0)                                                                                     `知情后进程中`
from (select *
      from tmp.tmp_tod_income_target
      where dt = '2021-10-08'
        and p_area not in ('全国', '东区', '西区', '南区', '北区')) a
         left join
     (select d_area,
             p_area,
             sum(`患者提交`)   `患者提交`,
             sum(`提报三方`)   `提报三方`,
             sum(`知情`)     `知情`,
             sum(`随机`)     `随机`,
             sum(`知情后进程中`) `知情后进程中`,
             sum(`实际营收`)   `实际营收`
      from (SELECT mis_id, user_name, user_num, leader, p_area, d_area
            from (select *
                  from tmp.tmp_tod_income_target
                  where dt = '2021-10-08'
                    and p_area not in ('全国', '东区', '西区', '南区', '北区')) a
                     left join
                 (select mis_id,
                         user_name,
                         user_num,
                         leader,
                         if(province rlike '招募', bd_city, province) province,
                         leader_user_num
                  from (
                           select user_num, user_name, mis_id, province, area, bd_city
                           from shuidi_dwb.dwb_cro_bd_base_info_full_d
                           where user_type = 2
                             and dt = current_date()
                       ) a
                           left join
                       (select b.mis                                                            as mis,
                               (case when b.user_level = 2 then b.mis_name else c.mis_name end) as leader,
                               (case when b.user_level = 2 then b.user_num else c.user_num end) as leader_user_num
                        from (select *
                              from shuidi_ods.ods_cro_bd_crm_organization_d
                              where is_delete = 0
                                and dt = current_date()) a
                                 join
                             (select *
                              from shuidi_ods.ods_cro_bd_crm_org_user_relation_d
                              where is_delete = 0
                                and dt = current_date()) b
                             on a.id = b.org_id
                                 join
                             (select *
                              from shuidi_ods.ods_cro_bd_crm_org_user_relation_d
                              where is_delete = 0
                                and dt = current_date()) c
                             on a.parent_id = c.org_id) b
                       on a.mis_id = b.mis
                  group by mis_id, user_name, user_num, leader, leader_user_num,
                           if(province rlike '招募', bd_city, province), leader_user_num) b
                 on a.province = b.province
            group by mis_id, user_name, user_num, leader, p_area, d_area
           ) a
               left join
           (select now_user_num,
                   `月份`,
                   sum(`患者提交`) as `患者提交`,
                   sum(`提报三方`) as `提报三方`,
                   sum(`知情`)   as `知情`,
                   sum(`随机`)   as `随机`,
                   sum(`知情后进程中`)  `知情后进程中`,
                   sum(`实际营收`) as `实际营收`
            from (select now_user_num,
                         substr(first_submit_time, 1, 7)                                                       `月份`,
                         count(distinct case when first_submit_time is not null then patient_id else null end) `患者提交`,
                         0 as                                                                                  `提报三方`,
                         0 as                                                                                  `知情`,
                         0 as                                                                                  `随机`,
                         0 as                                                                                  `实际营收`,
                         0 as                                                                                  `知情后进程中`
                  from (
                           select patient_id,
                                  now_user_num,
                                  first_submit_time,
                                  first_submit_crc_time,
                                  first_icf_obtain_time,
                                  first_randomized_time
                           from shuidi_dwb.dwb_cro_user_recruitment_detail_full_d
                           where dt = current_date()
                             and now_user_type = 2) a
                  group by now_user_num, substr(first_submit_time, 1, 7)

                  union
                  select now_user_num,
                         substr(first_submit_crc_time, 1, 7)                                                 `月份`,
                         0                                                                                as `患者提交`,
                         count(distinct
                               case when first_submit_crc_time is not null then patient_id else null end) as `提报三方`,
                         0                                                                                as `知情`,
                         0                                                                                as `随机`,
                         0                                                                                as `实际营收`,
                         0                                                                                as `知情后进程中`
                  from (
                           select patient_id,
                                  now_user_num,
                                  first_submit_time,
                                  first_submit_crc_time,
                                  first_icf_obtain_time,
                                  first_randomized_time
                           from shuidi_dwb.dwb_cro_user_recruitment_detail_full_d
                           where dt = current_date()
                             and now_user_type = 2) a
                  group by now_user_num, substr(first_submit_crc_time, 1, 7)

                  union

                  select now_user_num,
                         substr(first_icf_obtain_time, 1, 7)                                                 `月份`,
                         0                                                                                as `患者提交`,
                         0                                                                                as `提报三方`,
                         count(distinct
                               case when first_icf_obtain_time is not null then patient_id else null end) as `知情`,
                         0                                                                                as `随机`,
                         0                                                                                as `实际营收`,
                         0                                                                                as `知情后进程中`
                  from (
                           select patient_id,
                                  now_user_num,
                                  first_submit_time,
                                  first_submit_crc_time,
                                  first_icf_obtain_time,
                                  first_randomized_time
                           from shuidi_dwb.dwb_cro_user_recruitment_detail_full_d
                           where dt = current_date()
                             and now_user_type = 2) a
                  group by now_user_num, substr(first_icf_obtain_time, 1, 7)

                  union
                  select now_user_num,
                         substr(first_randomized_time, 1, 7)                                                 `月份`,
                         0                                                                                as `患者提交`,
                         0                                                                                as `提报三方`,
                         0                                                                                as `知情`,
                         count(distinct
                               case when first_randomized_time is not null then patient_id else null end) as `随机`,
                         0                                                                                as `实际营收`,
                         0                                                                                as `知情后进程中`
                  from (
                           select patient_id,
                                  now_user_num,
                                  first_submit_time,
                                  first_submit_crc_time,
                                  first_icf_obtain_time,
                                  first_randomized_time
                           from shuidi_dwb.dwb_cro_user_recruitment_detail_full_d
                           where dt = current_date()
                             and now_user_type = 2) a
                  group by now_user_num, substr(first_randomized_time, 1, 7)

                  union
                  select user_num                                                                            as now_user_num,
                         substr(current_Date(), 1, 7)                                                           `月份`,
                         0                                                                                   as `患者提交`,
                         0                                                                                   as `提报三方`,
                         0                                                                                   as `知情`,
                         0                                                                                   as `随机`,
                         0                                                                                   as `实际营收`,
                         count(distinct case when icf_obtain_time is not null then patient_id else null end) as `知情后进程中`
                  from (
                           select patient_id,
                                  user_num,
                                  icf_obtain_time,
                                  datediff(current_date(), icf_obtain_time) shichang
                           from shuidi_dwb.dwb_cro_user_project_schedule_full_d
                           where dt = current_date()
                             and user_type = 2
                             and finnal_status = '知情成功') a
                  where shichang < 30
                  group by user_num, substr(current_Date(), 1, 7)
                  union
                  select user_num    as                now_user_num,
                         substr(randomized_time, 1, 7) `月份`,
                         0           as                `患者提交`,
                         0           as                `提报三方`,
                         0           as                `知情`,
                         0           as                `随机`,
                         sum(income) as                `实际营收`,
                         0           as                `知情后进程中`
                  from (
                           select patient_id,
                                  a.project_id,
                                  supplier_price,
                                  randomized_status,
                                  now_supplier_price,
                                  supplier_screen_fail_cost,
                                  maxfailcost,
                                  case
                                      when randomized_status = '随机成功' and now_supplier_price is not null
                                          then now_supplier_price
                                      when randomized_status = '随机成功' and now_supplier_price is null then supplier_price
                                      when randomized_status = '随机失败' and supplier_screen_fail_cost is not null
                                          then supplier_screen_fail_cost
                                      when randomized_status = '随机失败' and supplier_screen_fail_cost is null
                                          then maxfailcost
                                      else null end income,
                                  randomized_time,
                                  user_num
                           from (
                                    select patient_id,
                                           project_id,
                                           supplier_price,
                                           randomized_status,
                                           now_supplier_price,
                                           supplier_screen_fail_cost,
                                           current_supplier_id,
                                           randomized_time,
                                           user_num
                                    from shuidi_dwb.dwb_cro_user_project_schedule_full_d
                                    where dt = current_date()
                                      and user_type = 2
                                      and randomized_time is not null
                                    group by patient_id,
                                             project_id,
                                             supplier_price,
                                             randomized_status,
                                             now_supplier_price,
                                             supplier_screen_fail_cost,
                                             current_supplier_id,
                                             randomized_time,
                                             user_num
                                ) a
                                    left join
                                (
                                    select project_id, max(supplier_screen_fail_cost) / 100 maxfailcost
                                    from shuidi_ods.ods_cro_recruit_project_manage_supplier_d
                                    where dt = current_date()
                                      and is_delete = 0
                                    group by project_id) b
                                on a.project_id = b.project_id) a
                  group by user_num, substr(randomized_time, 1, 7)) a
            where `月份` = substr(current_date(), 1, 7)
              and now_user_num not in (76070, 54313, 11010)
            group by now_user_num, `月份`) b
           on a.user_num = b.now_user_num
      group by d_area, p_area) b
     on a.p_area = b.p_area
group by a.d_area, a.p_area,
         case when a.leader_user_name in ('杨成玲', '许明辉', '闫波林') then 'TBA' else a.leader_user_name end,
         nvl(`患者提交`, 0), nvl(`提报三方`, 0),
         nvl(`知情`, 0), nvl(`随机`, 0), random_target,
         nvl(concat(cast(if(random_target = 0, 0, `随机` / cast(random_target as int)) * 100 as int), '%'), 0),
         --nvl(cast(`实际营收` as int), 0), income_target,
         concat(cast(round(
                     if(income_target = 0, 0, cast(nvl(`实际营收`, 0) as decimal(10, 2)) / 1.06 / income_target) * 100,
                     0) as int),
                '%'),
         concat(cast(round(dayofmonth(current_date()) / day(last_day(current_date())) * 100, 0) as int), '%'),
         nvl(`知情后进程中`, 0)
order by `区域`, `辖区`, `leader`
limit 1000
"""

        sum_str = "|\n"

        result = hive_conn.query(bd_sum_sql)

        time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        zt_res = []
        data = []
        title = [u"区域", u"辖区", u"leader", u"患者提交", u"提报三方", u"知情", u"随机", u"随机指标", u"指标达成率", u"营收达成",
                 u"时间进度", u"知情后进程中"]
        data.append(title)
        for value in result:
            data.append([unicode(str(i), "utf-8") for i in value])
        zt_res.append({'title': u'全国各片区数据进展情况', 'data': data})  # 在集群的名字
        drawimage = DrawTableImage(image_name=time_str + 'zt_res.png', col_data_width=100,
                                   col_data_width_list=[80, 120, 80, 80, 80, 80, 80, 80, 95, 130, 80, 120])
        image = drawimage.create_table_img(zt_res)
        # 测试群sendImageByBot('https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e6f145eb-1a70-4442-b089-0f391eb1aedf', image)
        sendImageByBot('https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6f39d88f-c78a-4156-a608-fd41b1359a94',
                       image)


    except Exception, ex:
        raise Exception(str(ex))
