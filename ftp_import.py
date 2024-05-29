import math
import os
from datetime import datetime
from ftplib import FTP
import numpy as np
import pandas as pd
import clickhouse_connect


def kill_EOF(filename):
    f_read = open(filename, encoding="utf-8", mode='r')
    last_line = f_read.readlines()
    with open(filename, encoding="utf-8", mode='w') as fp:
        if last_line[-1].count(';') != last_line[-2].count(';'):
            for line in last_line[0:-1]:
                fp.write(line)


script_b = datetime.now()
table_type = {'AdID': np.int32, 'SKU': np.int32, 'Price': np.float32, 'TotalC': np.int32}
parse_date = ['Date1']

host = 'localhost'
port = 2001
usr = 'cronuser'
pwd = 'cron_pass_123'
ftp = FTP()
ftp.connect(host, port)
ftp.login(usr, pwd)
ftp.cwd('~/upload/')
try:
    with open('ad.csv', 'wb') as file:
        print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Выгрузка ad.csv')
        ftp.retrbinary('RETR %s' % 'ad.csv', file.write)
except Exception:
    print('Выгрузка завершена с ошибкой! Завершение работы программы.')
    exit()
print(ftp.nlst())
ftp.quit()

chclient = clickhouse_connect.get_client(host='localhost',
                                         username='cronuser',
                                         password='cronuser_pass')
with chclient:
    flag = True
    offset = 0
    target_value = 500000
    while flag:
        print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Загрузка ad.csv, offset {offset}')
        if offset == 0:
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Очистка Import_Analytics.Advertisement')
            chclient.command(f'TRUNCATE TABLE Import_Analytics.Advertisement')
        try:
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
                  + f'Формирование фрейма на импорт')
            data_frame = pd.read_csv('ad.csv', sep=';',
                                     encoding='utf-8', skiprows=offset, nrows=target_value, dayfirst=True,
                                     dtype=table_type, parse_dates=parse_date,
                                     engine='python').replace(np.nan, None)
        except Exception as e:
            try:
                print('ad.csv', "Ошибка, исправляем EOF")
                kill_EOF('ad.csv')
                data_frame = pd.read_csv('ad.csv', sep=';',
                                         encoding='utf-8', skiprows=offset, nrows=target_value,
                                         dtype=table_type, parse_dates=parse_date, dayfirst=True,
                                         engine='python').replace(np.nan, None)
            except:
                print('Непредвиденная ошибка! Завершение работы программы')
                exit()
        if data_frame.shape[0] < target_value - 1:
            flag = False
        print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Импорт в таблицу Import_Analytics.Advertisement')
        chclient.insert('Import_Analytics.Advertisement', data_frame)
        offset += target_value

script_time = datetime.now() - script_b
print('\n' + "Скрипт " + os.path.basename(__file__) + " выполнялся " + str(
    math.floor(script_time.total_seconds())) + "сек. " + str(
    math.ceil((math.floor(script_time.total_seconds()) * 1000000 - script_time.microseconds) / -1000)) + "мс.")
