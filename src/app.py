github_url = 'https://raw.githubusercontent.com/datavedam/support-jobs/main/annual-enterprise-survey-2021-financial-year-provisional-size-bands-csv.csv'
import requests
import snowflake.connector as sf

conn=sf.connect(user='datavedam',password='Sfdvgaya1610#',account='qn29156.us-east4.gcp')


def download_github_content(link):
    return requests.get(github_url).content.decode('utf-8')

def save_to_file(filename, data):
    with open(filename, 'w') as f:
        f.write(data)

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
    

def load_data(filename):
    try:
        sql = 'use role {}'.format('ACCOUNTADMIN')
        execute_query(conn, sql)
        print('Using role ACCOUNTADMIN')

        sql = 'use database {}'.format('DEMO_DB')
        execute_query(conn, sql)
        print('Using database DEMO_DB')

        sql = 'use warehouse {}'.format('TEST')
        execute_query(conn, sql)
        print('Using warehouse TEST')

        sql = 'use schema {}'.format('PUBLIC')
        execute_query(conn, sql)
        print('Using schema PUBLIC')

        sql = 'create table finance(year varchar,industry_code_ANZSIC varchar,industry_name_ANZSIC varchar,rme_size_grp varchar,variable varchar,value varchar,unit varchar)'
        execute_query(conn, sql)
        print('Table finance created...')

        sql = 'drop stage if exists data_stage'
        execute_query(conn, sql)

        sql = 'create stage data_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1)'
        execute_query(conn, sql)

        csv_file = './'+filename
        sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true"
        execute_query(conn, sql)
        print('Staging the data..')

        sql = 'copy into finance from @DATA_STAGE/'+filename+'.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
              'ON_ERROR = "CONTINUE" '
        execute_query(conn, sql)
        print('Data Loading completed')

    except Exception as e:
        print(e)
        
        
data = download_github_content(github_url)
filename='output2.csv'
save_to_file(filename, data)
load_data(filename)
