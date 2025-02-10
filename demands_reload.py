from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests
import os
from sqlalchemy import create_engine
import psycopg2
import dotenv
import telegram

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def sum_price(x):
    '''
    Вычисляет себестоимость отгрузки
    '''
    cost = 0
    for position in x:
        cost += position['stock']['cost']
    return cost / 100

# Алерт в tg, если DAG не выполнился
def send_telegram(context):
    CHAT_ID = Variable.get('MY_CHAT_PASSWORD_TG')
    token = Variable.get('BOT_ALERT_TOKEN')
    time = context['data_interval_end'] + timedelta(hours=7) # Для отображения времени по Красноярску
    dag_id = context['dag'].dag_id
    message = f'DAG {dag_id} failed at {time}.'
    bot = telegram.Bot(token=token)
    bot.send_message(chat_id=CHAT_ID, text=message)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.anikin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 7, 1),
    'on_failure_callback': send_telegram
}

# Интервал запуска DAG
schedule_interval = '5 */1 * * *'

@dag(
    dag_id='demands_reload',
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1 
)

def reload_demands():

    @task()
    def download_demands():
        '''
        Выгрузка отгрузок
        '''
        TOKEN_MOYSKLAD = Variable.get('API_TOKEN_MOYSKLAD')
        headers = {'Authorization': f'Bearer {TOKEN_MOYSKLAD}'}
        # Колонки для выгрузки
        columns = ['id', 'accountId', 'shared', 'updated', 'name', 'description',
                   'moment', 'applicable', 'sum', 'created', 'printed', 'published',
                   'vatEnabled', 'payedSum', 'meta.type', 'owner.meta.href', 'owner.meta.type',
                   'group.meta.href', 'store.meta.href', 'agent.meta.href', 'organization.meta.href',
                   'agentAccount.meta.href', 'state.meta.href', 'shipmentAddress',
                   'project.meta.href', 'customerOrder.meta.href', 'customerOrder.meta.type', 'salesChannel.meta.href',
                   'vatIncluded', 'vatSum', 'contract.meta.href', 'organizationAccount.meta.href', 'overhead.sum',
                   'overhead.distribution', 'positions.rows']
        # Выгружаем данные за период, можно задавать через переменную
        days_for_reload = int(Variable.get('days_for_reload'))
        print(f'days_for_reload: {days_for_reload}!!!')
        # Выгружается на первый день выпавшего месяца
        time_d = (datetime.today() - timedelta(days=days_for_reload)).strftime("%Y-%m-01")
        response = requests.get(f"https://api.moysklad.ru/api/remap/1.2/entity/demand?expand=positions&limit=100&fields=stock&filter=moment>={time_d}", 
                                headers=headers)
        # Делаем первый запрос
        request = response.json()
        if request['meta']['size'] != 0:
            df = pd.json_normalize(request['rows'], max_level=5)
            # т.к. ограничение выдачи в 1000 строк, делаем несколько запросов, если необходимо
            while True:
                try:
                    response = requests.get(request['meta']['nextHref'], 
                                            headers=headers)
                    request = response.json()
                    df = pd.concat([df,
                                    pd.json_normalize(request['rows'],
                                                      max_level=5)])
                except:
                    break
            # Оставляем выбранные колонки
            intersection_cols = [x for x in columns if x in df.columns]
            df = df[intersection_cols]
            # Проверка кол-ва строк в запросе и фактически полученных 
            print('ПРОВЕРКА:')
            print(request['meta']['size'], df.shape[0])
            # Получем себестоимость
            df['costs'] = df['positions.rows'].apply(lambda x: sum_price(x))
            return df
        return pd.DataFrame()

    @task
    def parse_state(df):
        '''
        Добавляет состояние state по запросу API
        '''
        if df.shape[0] != 0:
            TOKEN_MOYSKLAD = Variable.get('API_TOKEN_MOYSKLAD')
            headers = {'Authorization': f'Bearer {TOKEN_MOYSKLAD}'}
            # Переименовываем колонки в формат snake_case
            df = df.rename(columns={'accountId': 'account_id',
                                'paymentPurpose': 'payment_purpose',
                                'vatEnabled': 'vat_enabled',
                                'vatSum': 'vat_sum',
                                'sum': 'demand_sum',
                                'payedSum': 'pay_sum',
                                'meta.type': 'demand_type',
                                'owner.meta.type': 'owner_type',
                                'owner.meta.href': 'owner_id',
                                'group.meta.href': 'group_id',
                                'store.meta.href': 'store_id',
                                'agent.meta.href': 'agent_id',
                                'agentAccount.meta.href': 'agent_account_id',
                                'organization.meta.href': 'organization_id',
                                'organizationAccount.meta.href': 'organization_account_id',
                                'state.meta.href': 'state',
                                'shipmentAddress': 'shipment_address',
                                'project.meta.href': 'project_id',
                                'salesChannel.meta.href': 'channel_id',
                                'vatIncluded': 'vat_included',
                                'vatSum': 'vat_sum',
                                'customerOrder.meta.href': 'customer_order_id',
                                'customerOrder.meta.type': 'customer_order_type',
                                'contract.meta.href': 'contract_id',
                                'organizationAccount.meta.href': 'organization_account_id',
                                'overhead.sum': 'overhead_sum',
                                'overhead.distribution': 'overhead_distribution'})
            # Оставляем только id в ссылке 
            id_list = ['owner_id', 'group_id', 'store_id', 'agent_id', 'agent_account_id', 'organization_id',
                       'organization_account_id', 'positions_id', 'project_id', 'channel_id', 'customer_order_id',
                       'contract_id', 'organization_account_id']
            intersection_ids = [x for x in id_list if x in df.columns] 
            for i in intersection_ids:
                df[i] = df[i].fillna('/').apply(lambda x:  x.split('/')[-1])
            # Парсим лист состояний
            state_list = df['state'].dropna().unique()
            states = {}
            for url in state_list:
                if type(url) == type('string'):
                    response = requests.get(url, 
                                            headers=headers)
                    request = response.json()
                    states[url] = request['name']
                else:
                    continue
            # Подставляем состояние state, суммы делим на 100 (переводим копейки в рубли)
            df['state'] = df['state'].apply(lambda x: states[x] if x in states.keys() else x)
            df['pay_sum'] = df['pay_sum'].fillna(0) / 100
            df['vat_sum'] = df['vat_sum'].fillna(0) / 100
            df['demand_sum'] = df['demand_sum'].fillna(0) / 100
            if 'overhead_sum' in df.columns:
                df['overhead_sum'] = df['overhead_sum'].fillna(0) / 100
            else:
                df['overhead_sum'] = 0
            return df
        else:
            return pd.DataFrame()

    @task
    def get_positions(df):
        '''
        Получаем позиции отгрузок
        '''
        if df.shape[0] != 0:
            rows = pd.DataFrame()
            columns = ['id', 'accountId', 'quantity', 'price', 'discount', 'vat', 'vatEnabled',
                   'overhead', 'assortment.meta.href', 'assortment.meta.uuidHref', 'stock.cost', 'pack.quantity',
                    'stock.quantity', 'stock.reserve', 'stock.intransit', 'stock.available', 'pack.uom.meta.href']
            for i in range(df.shape[0]):
                try:
                    row = pd.json_normalize(df['positions.rows'].iloc[i], max_level=5)
                    intersection_cols = [x for x in columns if x in row.columns]
                    row = row[intersection_cols]
                    row['demand_id'] = df.iloc[i]['id'] 
                    row['demand_name'] = df.iloc[i]['name']
                    row['moment'] = df.iloc[i]['moment']
                    row['applicable'] = df.iloc[i]['applicable']
                    row['demand_state'] = df.iloc[i]['state']
                    if 'pack.quantity' in row.columns:
                        row['pack.quantity'] = row['pack.quantity'].fillna(1)
                    else:
                        row['pack.quantity'] = 1
                    rows = pd.concat([rows, row])
                # Бывают отгрузки без цен и себестоимости, их не учитываем, но отправляем уведомление
                except:

                    print(f'DAG demands_update: {df.iloc[i]["id"]} - не получены позиции.')

            # Переименовываем столбцы, как в PosgreSQL
            rows = rows.rename(columns={'accountId': 'account_id',
                                         'vatEnabled': 'vat_enabled',
                                         'assortment.meta.href': 'product_id',
                                         'assortment.meta.uuidHref': 'assortment_href',
                                         'stock.cost': 'cost',
                                         'pack.quantity': 'pack_quantity',
                                         'stock.quantity': 'stock_quantity',
                                         'stock.reserve': 'stock_reserve',
                                         'stock.intransit': 'stock_intransit',
                                         'stock.available': 'stock_available',
                                         'pack.uom.meta.href': 'pack_type'})
            # Переводчим копейки в рубли
            rows['price'] = rows['price'] / 100
            rows['discount'] = rows['discount'] / 100
            rows['vat'] = rows['vat'] / 100
            rows['overhead'] = rows['overhead'] / 100
            rows['cost'] = rows['cost'] / 100
            # Получаем id продукта
            rows['product_id'] = rows['product_id'].fillna('/').apply(lambda x: x.split('/')[-1])
            return rows
        else:
            return pd.DataFrame()

    @task
    def parse_pack_type(df):
        '''
        Добавляет ед. измерения по запросу API
        '''
        if df.shape[0] != 0:
            TOKEN_MOYSKLAD = Variable.get('API_TOKEN_MOYSKLAD')
            headers = {'Authorization': f'Bearer {TOKEN_MOYSKLAD}'}
            # Парсим лист упаковок
            pack_list = df['pack_type'].dropna().unique()
            packs = {}
            for url in pack_list:
                if type(url) == type('string'):
                    response = requests.get(url, 
                                            headers=headers)
                    request = response.json()
                    packs[url] = request['name']
                else:
                    continue
            # Подставляем состояние state, суммы делим на 100 (переводим копейки в рубли)
            df['pack_type'] = df['pack_type'].apply(lambda x: packs[x] if x in packs.keys() else x)
            df['pack_type'] = df['pack_type'].fillna('Штука')
            return df
        else:
            return pd.DataFrame()
    
    @task
    def replace_series(df):
        '''
        Заменяем id серии на id товара (чтобы в будущем джойнить характеристики товаров по id)
        '''
        # выгружаем все серии
        TOKEN_MOYSKLAD = Variable.get('API_TOKEN_MOYSKLAD')
        headers = {'Authorization': f'Bearer {TOKEN_MOYSKLAD}'}
        if df.shape[0] != 0:
            response = requests.get(f"https://api.moysklad.ru/api/remap/1.2/entity/consignment", 
                                    headers=headers)
            # Делаем первый запрос
            request = response.json()
            if request['meta']['size'] != 0:
                series_df = pd.json_normalize(request['rows'], max_level=5)
                # т.к. ограничение выдачи в 1000 строк, делаем несколько запросов, если необходимо
                while True:
                    try:
                        response = requests.get(request['meta']['nextHref'], 
                                                headers=headers)
                        request = response.json()
                        series_df = pd.concat([series_df,
                                               pd.json_normalize(request['rows'],
                                                                 max_level=5)])
                    except:
                        break
            # получаем id товара в серии
            series_df = series_df[['id', 'assortment.meta.href']]
            series_df['p_id'] = series_df['assortment.meta.href'].fillna('/').apply(lambda x:  x.split('/')[-1])
            series_df = series_df.rename(columns={'id': 'product_id'})
            # отбираем позиции отгрузок с сериями
            series_positions = df[df['assortment_href'].str.contains('https://online.moysklad.ru/app/#consignment')]
            # мерджим id товара по id серии
            series_positions = series_positions.merge(series_df[['product_id', 'p_id']], how='left', on='product_id')
            series_positions = series_positions.drop(columns=['product_id'])
            series_positions = series_positions.rename(columns={'p_id': 'product_id'})
            # получаем id позиций отгрузок, где были серии
            drop_id = series_positions['id'].to_list()
            # берем позиции без серий и добавляем исправленные позции
            df = df.query('id not in @drop_id').copy()
            correct_positions = pd.concat([df, series_positions])
            del series_df, series_positions, drop_id
            return correct_positions
        else:
            return pd.DataFrame()

    @task
    def del_updated_rows(df, table):
        '''
        Удаляем строки, которые необходимо обновить, удаляем по дате и по id
        '''
        if df.shape[0] != 0:
            days_for_reload = int(Variable.get('days_for_reload'))
            del_id = df['id'].to_list()
            time_d = (datetime.today() - timedelta(days=days_for_reload)).strftime("%Y-%m-01")
            query = f"""
DELETE FROM
    {table} 
WHERE
    moment::date >= '{time_d}'
    OR (id in ({str(del_id)[1:-1]}))
"""
            conn = psycopg2.connect(Variable.get('CONNECT_TOKEN_POSGRESQL'))
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close() # закрываем курсор
            conn.close() # закрываем соединение
        else:
            return
    
    @task
    def psql_update(df, table):
        '''
        Загрузка обновленных контрагентов в PosgreSQL
        '''
        if df.shape[0] != 0:
            engine = create_engine(Variable.get('ENGINE_PSQL_TOKEN'))
            if table == 'demands':
                df = df.drop(columns=['positions.rows'])
            df.to_sql(table,
                      engine,
                      if_exists='append',
                      index=False)
        else:
            return

    demands_ms = download_demands()
    demands = parse_state(demands_ms)

    demand_positions = get_positions(demands)
    demand_positions = replace_series(demand_positions)
    demand_positions = parse_pack_type(demand_positions)
            
    del_updated_rows(demand_positions, 'demand_positions') >> psql_update(demand_positions, 'demand_positions')
    del_updated_rows(demands, 'demands') >> psql_update(demands, 'demands')    
    
reload_demands = reload_demands()