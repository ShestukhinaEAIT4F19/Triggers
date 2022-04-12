#То, что будет запускаться ежемесячно
import pandas
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

#Очищение таблиц после отчета
for i in ['company','categorization','clients','credit_org','curators','lines','licenses','mrss','reports','ss','triggers']:    
    mod.truncate_table(curs,i)
curs.execute('truncate table de3at.shes_stg_categorization')
conn.commit()

#Чтение файлов
read_xlsx.read_xlsx(curs,'current_date')
conn.commit()

#Пример обновления
#curs.execute("""update de3at.SHES_stg_curators_xlsx
#set curator='Иванов Иван Иванович', update_date = current_date
#where inn =7828050148 """)
#conn.commit()
#curs.execute('''delete from de3at.SHES_stg_curators_xlsx where inn = 7807104659''')
#conn.commit()
#curs.execute("""update de3at.SHES_stg_clients_xlsx
#set clients=15
#where inn =7828050148 and bo_du_dep = 'Брокерская деятельность' and dates = to_date('31.01.2022','DD.MM.YYYY') """)
#conn.commit()

#ETL процесс
s = '''    or stg.{val} <> tgt.{val} or ( stg.{val} is null and tgt.{val} is not null ) or ( stg.{val} is not null and tgt.{val} is null )'''
col = ''' ,tgt.{val}'''
col1 =''' ,stg.{val}'''
for tab in ['curators','lines','licenses','company']:
    if tab == 'curators':
        id = 'inn'
        fields = ['curator','department','other_dep']
    elif tab == 'lines':
        id = 'line_id'
        fields =['line_name']
    elif tab == 'licenses':
        id = 'lic_id'
        fields = ['id','lic_type']
    else:
        id = 'id'
        fields = ['inn','name','address','type']
    term = ''
    columns = ''
    columns1 = ''
    for f in fields:
        term += s.format(val=f)
        columns += col.format(val=f)
        columns1 += col1.format(val=f)
    mod.extract(curs,tab,id)
    conn.commit()
    mod.transform_load(curs,tab,id,term,columns1)
    conn.commit()
    mod.delet(curs,tab,id,columns)
    conn.commit()
    mod.update_meta(curs,tab)
    conn.commit()

mod.categ(curs)
conn.commit()
#Создание отчета
report.report(curs)
conn.commit()

#Перевод отчета в SCD2-формат
fields = ['dates','period','trg_dep','department','curator','views','inn','name','sign','category'
,'trg_name','ed_izm','trg_wasvalue','trg_nowvalue','abs_ch','otn_ch','diapazon','trig']
term = ''
columns = ''
columns1 =''
for f in fields:
    term += s.format(val=f)
    columns += col.format(val=f)
    columns1 += col1.format(val=f)
mod.extract(curs,'triggers','trg_num')
conn.commit()
mod.transform_load(curs,'triggers','trg_num',term,columns1)
conn.commit()
mod.delet(curs,'triggers','trg_num',columns)
conn.commit()
mod.update_meta(curs,'triggers')
conn.commit()
