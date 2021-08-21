# coding=utf-8

import sys
from datetime import date

import numpy as np
import pandas as pd

q = [['Google', 10], ['Runoob', 12], ['Wiki', 13]]
z = [['hanzi','shuzi']]
z = z+q
print(z)
x = {'1':q}
tmp1 = pd.DataFrame(q)
tmpdf = pd.DataFrame(z).to_html
print(tmpdf)
#df = pd.DataFrame(data,columns=['Site','Age'],dtype=float)


m = ['90', '60', '20']
lab = ['yuwen', 'shuxue', 'yingyuu']
c = {"分数": m, "姓名": lab}
df = pd.DataFrame(c).to_html(index=0)

print(df)