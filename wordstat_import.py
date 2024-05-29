import os
import time
import json
import urllib.request
import urllib.error
from datetime import datetime
import math
import pandas as pd
import clickhouse_connect

url = 'https://api-sandbox.direct.yandex.ru/v4/json/'
token = 'y0_AgAAAAAgIZadAAvNgQAAAAEFHPcxAADOq0-DCC5IW4iIEPVUE4pM-rPbPg'
username = 'martinmaider'
script_b = datetime.now()
date_f = datetime.today().strftime('%Y-%m-%d')
minusWords = [
    '-аналоги'
]

phrases = [
    'эргоферон',
    'анаферон',
    'тенотен',
    'ренгалин'
]

geo = [
    '225'
]
names_list = ['drug1', 'drug2', 'drug3', 'drug4']
parse = []
for i in range(len(phrases)):
    parse.append(phrases[i])
    for j in range(len(minusWords)):
        parse[i] += ' ' + minusWords[j]
print(parse)
for drug_num in range(4):
    try:
        data = {
            'method': 'GetClientsUnits',
            'token': token,
            'param': [username]
        }
        data = json.dumps(data, ensure_ascii=False).encode('utf8')
        request = urllib.request.urlopen(url, data)
        response = json.loads(request.read().decode('utf8'))
        if 'data' in response:
            print('Баллов осталось: ', response['data'][0]['UnitsRest'])
        else:
            raise Exception('Не удалось получить баллы', response)

        data = {
            'method': 'CreateNewWordstatReport',
            'token': token,
            'param': {
                'Phrases': [parse[drug_num]],
                'GeoID': geo
            }
        }
        data = json.dumps(data, ensure_ascii=False).encode('utf8')
        request = urllib.request.urlopen(url, data)
        response = json.loads(request.read().decode('utf8'))
        if 'data' in response:
            reportID = response['data']
            print('Создается отчет с ID = ', reportID)
        else:
            raise Exception('Не удалось создать отчет', response)

        data = {
            'method': 'GetWordstatReportList',
            'token': token
        }
        data = json.dumps(data, ensure_ascii=False).encode('utf8')
        request = urllib.request.urlopen(url, data)
        response = json.loads(request.read().decode('utf8'))
        if 'data' in response:
            lastReport = response['data'][len(response['data']) - 1]
            i = 0
            while lastReport['StatusReport'] != 'Done':
                print('>>> Подготовка отчета, ждите ... (' + str(i) + ')')
                time.sleep(2)
                data = {
                    'method': 'GetWordstatReportList',
                    'token': token
                }
                data = json.dumps(data, ensure_ascii=False).encode('utf8')
                request = urllib.request.urlopen(url, data)
                reportList = json.loads(request.read().decode('utf8'))
                lastReport = reportList['data'][len(reportList['data']) - 1]
                i += 1
            print('>>> Отчет ID = ', lastReport['ReportID'], ' получен!')
        else:
            raise Exception('Не удалось прочитать список отчетов', response)

        # Читаем отчет
        data = {
            'method': 'GetWordstatReport',
            'token': token,
            'param': reportID
        }
        data = json.dumps(data, ensure_ascii=False).encode('utf8')
        request = urllib.request.urlopen(url, data)
        report = json.loads(request.read().decode('utf8'))
        if 'data' in response:
            phrases_file = open(f'{names_list[drug_num]}.csv', 'w', encoding="utf-8")
            for i in range(len(report['data'])):
                for j in report['data'][i]['SearchedWith']:
                    showsToReport = str(j['Shows'])
                    phrases_file.write(showsToReport + '\n')
            phrases_file.close()
            print('>>> Результаты парсига успешно сохранены в файлы!')
        else:
            raise Exception('Не удалось прочитать отчет', report)
        data = {
            'method': 'DeleteWordstatReport',
            'token': token,
            'param': reportID
        }
        data = json.dumps(data, ensure_ascii=False).encode('utf8')
        request = urllib.request.urlopen(url, data)
        response = json.loads(request.read().decode('utf8'))
        if 'data' in response:
            print('>>> Отчет с ID = ', reportID, ' успешно удален с сервера Яндекс.Директ')
        else:
            raise Exception('Не удалось удалить отчет', report)
        print('>>> Все готово!')
    except Exception as e:
        print('>>> Поймано исключение:', e)

chclient = clickhouse_connect.get_client(host='localhost',
                                         username='cronuser',
                                         password='cronuser_pass')
with chclient:
    for i in range(4):
        data = []
        try:
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
                  + f'Формирование фрейма на импорт')
            data_frame = pd.read_csv(f'drug{i + 1}.csv', sep=';',
                                     encoding='utf-8', header=None)
            data.append([datetime.today(), i + 1, phrases[i], data_frame[0][0]])
            data.append([datetime.today(), i + 1, phrases[i] + '_total', data_frame[0].sum()])
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
                  + f'Загрузка INSERT INTO Import_Analytics.Wordstat за {date_f}')
            chclient.insert(f'Import_Analytics.Wordstat', data,
                            column_names=['QueryDate', 'SKUid', 'Query', 'QueryCount'])
            print(datetime.now().strftime("%Y-%m-%d %H:%M : ")
                  + f'Загрузка Import_Analytics.Wordstat за {date_f} завершена')
        except Exception:
            print('Выполнение завершено с ошибкой!')
            exit()
script_time = datetime.now() - script_b

print('\n' + "Скрипт " + os.path.basename(__file__) + " выполнялся " + str(
    math.floor(script_time.total_seconds())) + "сек. " + str(math.ceil(script_time.microseconds / 1000)) + "мс.")
