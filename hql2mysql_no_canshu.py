strList = []
while True:
    strin = input()
    if "PARTITIONED BY" in strin:
        break
    else:
        strList.append(strin)
print("\n")

print("------------------------------------mysql------------------------------")
flag = 1
for number in range(len(strList)):
    strSpilt = strList[number].split()  # 拆分成：`patient_id`  bigint     COMMENT    '患者id',

    if number == 0:
        headStr0 = strSpilt[-1].split(".")  # 切分成`shuidi_sdm  sdm_cro_patient_project_rel_opt_log_full_d`(
        headStr1 = headStr0[1][:-2]
        headStr2 = "CREATE TABLE " + "`" + headStr1 + "`" + "("
        print(headStr2)
    if flag == 1:
        print("  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',")  # 主键id
        flag = 0

    if 0 < number < len(strList) - 1:
        if number == len(strList)-2:
            strtmp = strSpilt[3].split(")")
            strSpilt[3] = strtmp[0]+','

        if strSpilt[1] == "timestamp":
            writer2 = "  " + strSpilt[0] + " " + "timestamp NULL  COMMENT " + strSpilt[3]
            print(writer2)
        elif strSpilt[1] == "string":
            writer2 = "  " + strSpilt[0] + " " + "varchar(200)  COMMENT " + strSpilt[3]
            print(writer2)
        elif strSpilt[1] == "int":
            writer2 = "  " + strSpilt[0] + " " + "int(10)  COMMENT " + strSpilt[3]
            print(writer2)
        elif strSpilt[1] == "bigint":
            writer2 = "  " + strSpilt[0] + " " + "int(10)  COMMENT " + strSpilt[3]
            print(writer2)
        elif strSpilt[1] == "decimal(10,2)":
            writer2 = "  " + strSpilt[0] + " " + "decimal(10,2) unsigned  COMMENT " + strSpilt[3]
            print(writer2)
        elif strSpilt[1] == "double":
            writer2 = "  " + strSpilt[0] + " " + "decimal(10,2) unsigned  COMMENT " + strSpilt[3]
            print(writer2)
        else:

            print("没遇到过的类型 需要手动添加")

    if number == len(strList) - 1:
        print("  PRIMARY KEY (`id`) USING BTREE")
        lastStr = ") ENGINE=InnoDB AUTO_INCREMENT=2612 DEFAULT CHARSET=utf8mb4 " + strSpilt[0] + "=" + strSpilt[1]
        print(lastStr)

print("\n")
print("----------------------------加不加----------------------------------------")
print("`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',")
print("`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',")
print("`is_delete` tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '是否逻辑删除',")