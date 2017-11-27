# -*- coding: UTF-8 -*-
import ConfigParser
import sys
import os
import time
import datetime


now_time = datetime.datetime.now()
yester_time = now_time+datetime.timedelta(days=-1)
yesterday=yester_time.strftime('%Y%m%d')
cf = ConfigParser.ConfigParser()
cf.read('conf.ini')
department_get = sys.argv[1]
department_dir = cf.get('nas',department_get+'_dir')   # S01_CFF
while True:
	if os.path.exists(department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday):
		break
	else:
		time.sleep(60)
while True:
	checkout = os.listdir(department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
	if department_get+'.succ'in checkout:
		print('检测到'+department_get+'标识文件')
		file_day = open('yesterday.txt','w')
		file_day.write(yesterday)
		file_day.close()
		break
	else:
		time.sleep(60)
