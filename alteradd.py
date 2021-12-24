strList = []
while True:
    strin = input()
    if "PARTITIONED BY" in strin:
        break
    else:
        strList.append(strin)
print("\n")
for number in range(len(strList)):
    strSpilt = strList[number].split()  # 拆分成：`patient_id`  bigint     COMMENT    '患者id',
    writer = 'alter table dwb_cro_user_recruitment_detail_full_d add column '+' '+strSpilt[0]+ 'varchar (128) comment' + strSpilt[3]
    print(writer)
