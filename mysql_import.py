import math
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import clickhouse_connect
import pymysql.cursors

script_start = datetime.now()
chclient = clickhouse_connect.get_client(host='localhost',
                                         username='cronuser',
                                         password='cronuser_pass')
mysqlconnection = pymysql.connect(host='localhost',
                                  user='root',
                                  password='cronuser_pass',
                                  database='medcorp_import',
                                  cursorclass=pymysql.cursors.DictCursor)
script_b = datetime.now() + relativedelta(months=-6)
target_month_arr = [
    int(str(script_b.year - 1 if script_b.month < 4 else script_b.year) + '%02d' % ((12 + (script_b.month - 3)) % 12)),
    int(str(script_b.year - 1 if script_b.month < 3 else script_b.year) + '%02d' % ((12 + (script_b.month - 2)) % 12)),
    int(str(script_b.year - 1 if script_b.month < 2 else script_b.year) + '%02d' % ((12 + (script_b.month - 1)) % 12))]
print('Загрузка 3 последних месяцев:' + str(target_month_arr))
with mysqlconnection:
    with mysqlconnection.cursor() as cursor:
        # Read a single record
        try:
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Чтение данных Sales')
            sql = f'''
            SELECT Date1, WeekNum, AdId, Country, City, Adress, SKU, 
            AVG(Price), SUM(Sale_P), SUM(Buy_P), AVG(BPrice)
            FROM medcorp_import.sales
            WHERE DATE_FORMAT(Date1, '%Y%m') IN ({str(target_month_arr)[1:-1]})
            GROUP BY Date1, WeekNum, AdId, Country, City, Adress, SKU
            '''
            cursor.execute(sql)
            result = cursor.fetchall()
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ") + f'Чтение завершено, строк получено:{len(result)}')
        except Exception:
            print('Чтение завершено с ошибкой! Завершение работы программы.')
            exit()
with chclient:
    data = []
    for row in result:
        data.append(list(row.values()))
    try:
        print(datetime.now().strftime(
            "%Y-%m-%d %H:%M : ") + f'Очистка ALTER TABLE Import_Analytics.Sales за {target_month_arr}')
        sql = f'''
        ALTER TABLE Import_Analytics.Sales DELETE
        WHERE formatDateTime(DataDate, '%Y%m') IN ({str(target_month_arr)[1:-1]})
        '''
        chclient.command(sql)
        print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
              + f'Загрузка INSERT INTO Import_Analytics.Sales за {target_month_arr}')
        chclient.insert(f'Import_Analytics.Sales', data)
        print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
              + f'Загрузка Import_Analytics.Sales за {target_month_arr} завершена')
    except Exception:
        print('Запись завершена с ошибкой! Завершение работы программы.')
        exit()
script_time = datetime.now() - script_start
print('\n' + "Скрипт " + os.path.basename(__file__) + " выполнялся " + str(
    math.floor(script_time.total_seconds())) + "сек. " + str(
    math.ceil(script_time.microseconds/1000)) + "мс.")
