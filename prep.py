#Создание таблиц и первое заполнение
import pandas as pd
import jaydebeapi

#Импорт моих модулей
import mod
import read_xlsx
import report

conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:de3at/bardthebowman@de-oracle.chronosavant.ru:1521/deoracle',
    ['de3at','bardthebowman'],
    '/home/de3at/ojdbc8.jar'
)
curs = conn.cursor()
conn.jconn.setAutoCommit(False)

#Создаем таблицы на сервере

#10 таблиц из экселя
sql_categorization_stg = '''
    create table de3at.shes_stg_categorization_xlsx (
        inn number,
        category varchar2(50),
        dates date
        )
'''
curs.execute(sql_categorization_stg)
conn.commit()

sql_clients_stg = '''
    create table de3at.shes_stg_clients_xlsx (
	inn number,
	bo_du_dep varchar2(100),
	clients number,
	dates date
	)
'''
curs.execute(sql_clients_stg)
conn.commit()

sql_company_stg = '''
    create table de3at.shes_stg_company_xlsx (
        id number,
        inn number,
        name varchar2(50),
        address varchar2(500),
        type varchar2(50),
        update_date date default(current_date)
        )
'''
curs.execute(sql_company_stg)
conn.commit()

sql_credit_org_stg = '''
    create table de3at.shes_stg_credit_org_xlsx (
        inn number
        )
'''
curs.execute(sql_credit_org_stg)
conn.commit()

sql_curators_stg = '''
    create table de3at.shes_stg_curators_xlsx (
        inn number,
        curator varchar2(1000),
        department varchar2(500),
        other_dep varchar2(500),
        update_date date default(current_date)
        )
'''
curs.execute(sql_curators_stg)
conn.commit()

sql_licenses_stg = '''
    create table de3at.shes_stg_licenses_xlsx (
        lic_id number,
        id number,
        lic_type varchar2(100),
        update_date date default(current_date)
        )
'''
curs.execute(sql_licenses_stg)
conn.commit()

sql_lines_stg='''
    create table de3at.shes_stg_lines_xlsx (
        line_id number,
        line_name varchar2(3000),
        update_date date default(current_date)
        )
'''
curs.execute(sql_lines_stg)
conn.commit()

sql_MRSS_stg = '''
    create table de3at.shes_stg_MRSS_xlsx (
        id number,
        form_id number,
        line_id number,
        value number,
        dates date
        )
'''
curs.execute(sql_MRSS_stg)
conn.commit()

sql_reports_stg = '''
    create table de3at.shes_stg_reports_xlsx (
        form_id number,
        version number
        )
'''
curs.execute(sql_reports_stg)
conn.commit()

sql_SS_stg = '''
    create table de3at.shes_stg_SS_xlsx (
        id number,
        form_id number,
        line_id number,
        value number,
        dates date
        )
'''
curs.execute(sql_SS_stg)
conn.commit()

#Создание меты
curs.execute('''
create table de3at.SHES_metadata(
    schema_name varchar2(30),
    table_name varchar2(30),
    update_date date)
''')
conn.commit()

#Создание таблицы категоризации в SCD2

curs.execute('''
create table de3at.shes_stg_categorization (
    inn number,
    category varchar2 (50 byte),
    effective_from date,
    effective_to date)
''')
conn.commit()

#Создание таблицы триггеров
curs.execute('''
create table de3at.shes_stg_triggers_xlsx (
    trg_num varchar2(200),
    dates date,
    period varchar2(50),
    trg_dep varchar2(50),
    department varchar2(200),
    curator varchar2(200),
    views varchar2(50),
    inn number,
    name varchar2(200),
    sign varchar2(50),
    category varchar2(200),
    trg_name varchar2(200),
    ed_izm varchar2(50),
    trg_wasvalue number,
    trg_nowvalue number,
    abs_ch number,
    otn_ch number,
    diapazon varchar2(200),
    trig varchar2(1),
    update_date date)
''')
conn.commit()

#Первое выполнение ETL процесса
read_xlsx.read_xlsx(curs,"to_date('31.12.1900','DD.MM.YYYY')")
conn.commit()

for tab in ['curators','lines','licenses','company']:
    if tab == 'curators':
        id = 'inn'
    elif tab == 'lines':
        id = 'line_id'
    elif tab == 'licenses':
        id = 'lic_id'
    else:
        id = 'id'            
    mod.create_stg(curs,tab,id)
    conn.commit()
    mod.insert_meta(curs,tab)
    conn.commit()
    mod.create_target(curs,tab)
    conn.commit()
    mod.first_insert_target(curs,tab)
    conn.commit()

mod.categ(curs)
conn.commit()
report.report(curs)
conn.commit()

mod.create_stg(curs,'triggers','trg_num')
conn.commit()
mod.insert_meta(curs,'triggers')
conn.commit()
mod.create_target(curs,'triggers')
conn.commit()
mod.first_insert_target(curs,'triggers')    
conn.commit()

