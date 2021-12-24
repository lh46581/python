strList = []
while True:
    strin = input()
    if "PARTITIONED BY" in strin:
        break
    else:
        strList.append(strin)
print("\n")

for number in range(len(strList)):
    strSpilt = strList[number].split()


