# python 3.9 pyautogui 可以控制鼠标 有时间可以研究一下
import pyautogui as gui
#gui.dragTo(200,300,button = 'left') #按住鼠标左键 把鼠标拖到200，300 位置
#gui.dragTo(500,200,1,button = 'left')
x,y = gui.size()
print(x,y)