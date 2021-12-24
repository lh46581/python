# -*- coding: UTF-8 -*-
import requests, sys, datetime, time, date, timedelta


##定义当前时间
def delTime():
    later_days = datetime.datetime.now() + datetime.timedelta(days=7)
    friday = later_days.strftime('%Y-%m-%d')
    return friday


def getMetting(room=sys.argv[1], day=sys.argv[2], mins=sys.argv[3]):
    count = 0
    while (count < 9):

    count = count + 1
    # 获取当前小时
    startday = day.split("-", -1)[1]
    endday = datetime.datetime.now() + datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    headers = {
    "Cookie": "shuidiopenid=ifE86137V%2FQmTo8q71kdBA%3D%3D; pushMessageInfo=%257B%2522lastTime%2522:1630031649955%2C%2522messageIdList%2522:%255B5440%2C4043%2C1724%2C26%2C23%2C22%2C21%255D%2C%2522hasClosed%2522:false%257D; cloudtoken=xVxYOPtliS4eO5UQr96x0w$0; token=xVxYOPtliS4eO5UQr96x0w$0; userInfo=%257B%2522token%2522:%2522xVxYOPtliS4eO5UQr96x0w$0%2522%2C%2522id%2522:79673%2C%2522name%2522:%2522%25E6%259D%258E%25E5%2587%2586%2522%2C%2522mis%2522:%2522lizhun%2522%2C%2522password%2522:null%2C%2522mobile%2522:%252218310718606%2522%2C%2522mail%2522:%2522lizhun@shuidi-inc.com%2522%2C%2522accountType%2522:1%2C%2522bizType%2522:-1%2C%2522valid%2522:true%2C%2522status%2522:1%2C%2522dateCreated%2522:1616727609188%2C%2522lastModified%2522:1616727609190%2C%2522workNum%2522:null%2C%2522nickName%2522:null%2C%2522accountStatusEnum%2522:%2522WORKING%2522%2C%2522bizTypeStr%2522:null%2C%2522phoneSeatNum%2522:%2522%2522%2C%2522department%2522:%2522%2522%2C%2522departmentId%2522:%2522%2522%2C%2522idCard%2522:null%257D; cipherUserId=3DgOgpNwRxMLicSPlWclAA$0; cryptoUserId=r_o6MudIeTh_D_1pr3Q6lw; headImgUrl=http://thirdwx.qlogo.cn/mmopen/2ich6h3EefaXoWoavyWTGXkXKxibNbkicicyCWqKGjIiaoSM5M1aMrHFTF6LWoEJxrg9sqE3NOUVRhyA7WBeH5VdCjm4mhTfFzaXJ/132; isBindMobile=true; mobile=183****8606; nickname=%255B%25E5%25B7%25A6%25E5%2593%25BC%25E5%2593%25BC%255DCube%2520Sugar%255B%25E5%258F%25B3%25E5%2593%25BC%25E5%2593%25BC%255D; refreshToken=6a9b225471bb468ebf078ce72512ba15; timeStamp=1616741513425; uuid=a2SpJhkkfX2HaRT8meD1616741475055; code=E0EfePNNAM9cxvgAe2n0G4bjhLEUMNoxwKT5UdlbVjM",
    "Referer": "http://meeting.shuiditech.com:8185/bespokepc/index/index/userid/lizhun"

}
url = 'http://meeting.shuiditech.com:8185/bespokepc/Index/addSavePC/'
data = {
"roomid": room,
"appORweb": "1",
"day": day,
"start": "10:30",
"end": "12:00",
"hc_select": "21",
"keyuyue": "",
"theme": "开会",
"content": "",
"search": "",
"canjiaemail": "",
"canjiastaff1": "",
"canjiastaff": "",
"appointID": "",
"emailstr": ""
}
# print(data)

res = requests.post(url=url, headers=headers, data=data)


def getFutureMetting():
    friday = delTime()
    headers = {
        "Cookie": "shuidiopenid=ifE86137V%2FQmTo8q71kdBA%3D%3D; pushMessageInfo=%257B%2522lastTime%2522:1630031649955%2C%2522messageIdList%2522:%255B5440%2C4043%2C1724%2C26%2C23%2C22%2C21%255D%2C%2522hasClosed%2522:false%257D; cloudtoken=xVxYOPtliS4eO5UQr96x0w$0; token=xVxYOPtliS4eO5UQr96x0w$0; userInfo=%257B%2522token%2522:%2522xVxYOPtliS4eO5UQr96x0w$0%2522%2C%2522id%2522:79673%2C%2522name%2522:%2522%25E6%259D%258E%25E5%2587%2586%2522%2C%2522mis%2522:%2522lizhun%2522%2C%2522password%2522:null%2C%2522mobile%2522:%252218310718606%2522%2C%2522mail%2522:%2522lizhun@shuidi-inc.com%2522%2C%2522accountType%2522:1%2C%2522bizType%2522:-1%2C%2522valid%2522:true%2C%2522status%2522:1%2C%2522dateCreated%2522:1616727609188%2C%2522lastModified%2522:1616727609190%2C%2522workNum%2522:null%2C%2522nickName%2522:null%2C%2522accountStatusEnum%2522:%2522WORKING%2522%2C%2522bizTypeStr%2522:null%2C%2522phoneSeatNum%2522:%2522%2522%2C%2522department%2522:%2522%2522%2C%2522departmentId%2522:%2522%2522%2C%2522idCard%2522:null%257D; cipherUserId=3DgOgpNwRxMLicSPlWclAA$0; cryptoUserId=r_o6MudIeTh_D_1pr3Q6lw; headImgUrl=http://thirdwx.qlogo.cn/mmopen/2ich6h3EefaXoWoavyWTGXkXKxibNbkicicyCWqKGjIiaoSM5M1aMrHFTF6LWoEJxrg9sqE3NOUVRhyA7WBeH5VdCjm4mhTfFzaXJ/132; isBindMobile=true; mobile=183****8606; nickname=%255B%25E5%25B7%25A6%25E5%2593%25BC%25E5%2593%25BC%255DCube%2520Sugar%255B%25E5%258F%25B3%25E5%2593%25BC%25E5%2593%25BC%255D; refreshToken=6a9b225471bb468ebf078ce72512ba15; timeStamp=1616741513425; uuid=a2SpJhkkfX2HaRT8meD1616741475055; code=E0EfePNNAM9cxvgAe2n0G4bjhLEUMNoxwKT5UdlbVjM",
        "Referer": "http://meeting.shuiditech.com:8185/bespokepc/index/index/userid/lizhun"
    }
    url = 'http://meeting.shuiditech.com:8185/bespokepc/Index/addSavePC/'
    data = {
        "roomid": "112",
        "appORweb": "1",
        "day": friday,
        "start": "10:30",
        "end": "12:00",
        "hc_select": "21",
        "keyuyue": "",
        "theme": "数仓开会",
        "content": "",
        "search": "",
        "canjiaemail": "",
        "canjiastaff1": "",
        "canjiastaff": "",
        "appointID": "",
        "emailstr": ""
    }
    # print(data)

    res = requests.post(url=url, headers=headers, data=data)


if __name__ == '__main__':
    ## 时间 会议室号 预定人 结束时间
    if (len(sys.argv)=4):
        ##适用于预定未放出会议室，当年时间+7天
        getTodayMetting(room=sys.argv[1], day=sys.argv[2], mins=sys.argv[3])
    elif (len(sys.argv)=5):
        getFutureMetting()
    else:
        print("The number of parameters is incorrect")
        sys.exit()

##从最大时间范围开始订 直到预定成功
