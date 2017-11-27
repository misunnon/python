# -*- coding: UTF-8 -*-
from utils.HBaseConn import *
import os
import ConfigParser
import threading
import time
import sys
import shutil
import datetime
import logging


try:
	# 获取参数 部门名称  如：S01_CFF
	department_get = sys.argv[1]
except Exception,e:
	print(e)
	logging.error('sys.argv error')
	sys.exit(1)
# 配置日志
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=department_get+'myapp.log',
                filemode='w')

try:
	# 获取前一天日期
	file_day = open('yesterday.txt','r')
	yesterday = file_day.read()
except Exception,e:
	print(e)
	logging.error('date error')
	sys.exit(1)

cf = ConfigParser.ConfigParser()
try:
	# 读取对应配置文件，获取参数
	cf.read('conf.ini')
	department_dir = cf.get('nas', department_get+'_dir')
	local_data_dir = cf.get('local', 'data_dir')
	hbase_data_table = cf.get('hbase', 'table_data')
	hbase_ip_list = cf.get('hbase', 'ip').split(',')
	hbase_file_table = cf.get('hbase','table_audio')
except Exception,e:
	print(e)
	logging.error('conf.ini error')
	sys.exit(1)
# 目录cp
try:
	if os.path.exists(local_data_dir+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday):
		shutil.rmtree(local_data_dir+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
	shutil.copytree(department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday, local_data_dir+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
except Exception,e:
	print(e)
	logging.error('copy error')
	sys.exit(1)
# os.system('chmod -R 777 '+local_data_dir)

try:
	# hbase连接创建对表操作的对象
	hbase_conn_main=hbase_conn(hbase_ip_list[0])
	data_table_operation = open_table(hbase_conn_main,hbase_data_table)
except Exception,e:
	print(e)
	logging.error('hbase conn error')
	sys.exit(1)
# 获取所有文件名
file_name = os.listdir(local_data_dir+'/'+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
# 删除非.wav文件
file_name_list = os.listdir(local_data_dir+'/'+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
for i in file_name_list:

	if i.endswith('.wav'):
		pass
	else:
		file_name.remove(i)
# 录入音频对应数据
try:
	for f in file_name:
		f_1 = f.replace('.wav','')
		f_2 = f_1.split('_')
		rowkey = f_2[0]+f_2[1]+f_2[2]
		data_table_operation.put(rowkey[::-1],{'info:date':f_2[0],'info:seats':f_2[1],'info:number': f_2[2],'info:call': f_2[3],'info:centreDate':f_2[4],'info:centreID':f_2[5]})
except Exception,e:
	print(e)
	logging.error('hbase data put error')
	sys.exit(1)
hbase_conn_main.close()

# 每个hbase thift服务开启2个连接
thread_num = 2 * len(hbase_ip_list)
# 存储hbase连接
hbase_conn_list = []
# 存储hbase操作表对象
table_operation_list = []
# 线程job_list
thread_job_list = []
for i in range(thread_num):
	hbase_conn_list.append('hbase_conn_'+str(i+1))
	table_operation_list.append('operation_'+str(i+1))
	thread_job_list.append('thred_'+str(i+1))
os.chdir(local_data_dir+department_dir+'/'+time.strftime("%Y", time.gmtime())+'/'+yesterday)
# 分配单个线程需要下载数量
allot_file = len(file_name)/thread_num
allot_main_file = len(file_name)%thread_num

hbase_ip_deputy = hbase_ip_list
hbase_ip_deputy.extend(hbase_ip_deputy)

def deputy_put(num,hbase_conn_num,operation_table):
	if num == thread_num-1:
		for i in range(num*allot_file,(num+1)*allot_file+allot_main_file):
			file_data = open(file_name[i],'rb').read()
			f_2 = file_name[i].replace('.wav','').split('_')
			rowkey = f_2[0]+f_2[1]+f_2[2]
			operation_table.put(rowkey[::-1],{'info:file':file_data})
			time.sleep(2)
		hbase_conn_num.close()
	else:
		for i in range(num * allot_file,(num+1) * allot_file):
			file_data = open(file_name[i],'rb').read()
			f_2 = file_name[i].replace('.wav','').split('_')
			rowkey = f_2[0]+f_2[1]+f_2[2]
			operation_table.put(rowkey[::-1],{'info:file':file_data})
			time.sleep(2)
		hbase_conn_num.close()
try:
	for i in range(len(hbase_ip_deputy)):
		hbase_conn_list[i] = hbase_conn(hbase_ip_deputy[i])
except Exception,e:
	print(e)
	logging.error('hbase conn error')
	sys.exit(1)

try:
	for i in range(thread_num):
		table_operation_list[i] = open_table(hbase_conn_list[i],hbase_file_table)
		# 创建线程
		thread_job_list[i] = threading.Thread(target=deputy_put,args=(i,hbase_conn_list[i],table_operation_list[i]))
		# 开启线程
		thread_job_list[i].start()
except Exception,e:
	print(e)
	logging.error('thread error')
	sys.exit(1)
for i in range(thread_num):
	# 等待线程结束
	thread_job_list[i].join()
