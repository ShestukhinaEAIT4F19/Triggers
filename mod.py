#Подготовка структур

def create_stg(curs,tab,id):
   # curs.execute('create table de3at.shes_stg_'+tab+' as select * from de3at.shes_stg_'+tab+'_xlsx where 1 = 0')
    curs.execute('create table de3at.shes_stg_'+tab+'_del as select '+id+' from de3at.shes_stg_'+tab+'_xlsx where 1 = 0') 
   
#Преобразование категоризации в SCD2 формат через оконные функции
def categ(curs):
    curs.execute('''
    insert into de3at.shes_stg_categorization 
    select
        inn,
        category,
        dates as effective_from,
        coalesce (
            lead(dates, 1) over (partition by inn order by dates)- interval '1' second,
            to_date('2999-12-31', 'YYYY-MM-DD')
            ) as effective_to
    from de3at.shes_stg_categorization_xlsx
    ''')
  
#Заполнение меты
def insert_meta(curs,tab):
    curs.execute("""
    insert into de3at.SHES_metadata( schema_name, table_name, update_date )
    values( 'DE3AT','SHES_STG_"""+tab.upper()+"""_XLSX', current_date)
    """)
    
#Создание таргета
def create_target(curs, tab):
    curs.execute('create table de3at.SHES_'+tab+' as select * from de3at.SHES_stg_'+tab+'_xlsx where 1=0')
    curs.execute('ALTER TABLE de3at.SHES_'+tab+' DROP COLUMN update_date')
    curs.execute("""ALTER TABLE de3at.SHES_"""+tab+""" add
	(effective_from date,
	effective_to date,
    is_deleted varchar2(1) default 0)""")
    
#Самое первое заполнение таргета
def first_insert_target(curs,tab):
    curs.execute('''
    insert into de3at.SHES_'''+tab+'''
     select 
        sources.*, 
        to_date('31.12.2999 00:00:00','DD.MM.YYYY HH24:MI:SS'),
        0 
    from de3at.SHES_stg_'''+tab+'''_xlsx sources
    ''')
    
#Инкрементальная загрузка

#1. Загрузка в STG (захват, extract)
def extract(curs,tab,id):
   # curs.execute('truncate table de3at.shes_stg_'+tab)
    curs.execute('truncate table de3at.shes_stg_'+tab+'_del')
   
   # curs.execute("""
   # insert into de3at.shes_stg_"""+tab+
   # """ select * from de3at.shes_stg_"""+tab+"""_xlsx
   # where update_date>=(
   # select max(update_date)
   # from de3at.shes_metadata
   # where schema_name = 'DE3AT' 
   # and table_name = 'SHES_STG_"""+tab.upper()+"""_XLSX')""")
    curs.execute('''insert into de3at.shes_stg_'''+tab+'''_del ('''+id+''')
    select '''+id+' from de3at.shes_stg_'+tab+'_xlsx')
    
#Выделение вставок и изменений (transform); вставка в их приемник (load)
def transform_load(curs,tab,id,term,columns): 
    curs.execute('''
    insert into de3at.SHES_'''+tab+''' select
    stg.*,
    to_date('31.12.2999','DD.MM.YYYY'),
    0
    from de3at.SHES_stg_'''+tab+'''_xlsx stg
    join de3at.SHES_'''+tab+''' tgt on stg.'''+id+'''=tgt.'''+id+'''
    and tgt.effective_to = to_date('31.12.2999','DD.MM.YYYY') and tgt.is_deleted = 0 where 1=0
    '''+term)
    
    curs.execute('merge into de3at.SHES_'+tab+''' tgt
    using de3at.SHES_stg_'''+tab+'''_xlsx stg
    on( stg.'''+id+' = tgt.'+id+''' and tgt.is_deleted = 0)
    when matched then 
    update set tgt.effective_to = stg.update_date - interval '1' second
    where tgt.effective_to = to_date('31.12.2999','DD.MM.YYYY') and (1=0
        '''+term+''')
    when not matched then 
        insert 
        values ( stg.'''+id+columns+''', stg.update_date, to_date( '31.12.2999', 'DD.MM.YYYY' ), 0 )'''    
    ) 
   
# 3. Логическое удаление
def delet(curs,tab,id,columns):
    curs.execute('update de3at.SHES_'+tab+''' set effective_to = sysdate - interval '1' second
    where '''+id+''' in (
    select tgt.'''+id+'''
    from de3at.SHES_'''+tab+''' tgt 
    left join de3at.SHES_stg_'''+tab+'''_del stg
    on tgt.'''+id+''' = stg.'''+id+'''
    where stg.'''+id+' is null)')
    
    curs.execute('insert into de3at.SHES_'+tab+'''
    select delet.*,
        sysdate as effective_from,
        to_date('31.12.2999 00:00:00','DD.MM.YYYY HH24:MI:SS') as effective_to,
        1 as is_deleted
    from (
    select tgt.'''+id+columns+'''
    from de3at.SHES_'''+tab+''' tgt 
    left join de3at.SHES_stg_'''+tab+'''_del stg
    on tgt.'''+id+' = stg.'+id+'''
    where
        stg.'''+id+''' is null and
        tgt.effective_to > sysdate - interval '5' second) delet''')
    
#4. Обновление метаданных.
def update_meta(curs,tab):
    curs.execute("""update de3at.SHES_metadata
    set update_date = ( select max( update_date ) from de3at.SHES_"""+tab+""")
    where schema_name = 'DE3AT' 
    and table_name =  'SHES_STG_"""+tab.upper()+"_XLSX'")
    
#Очищение таблиц после отчета
def truncate_table(curs,tab):
    curs.execute('truncate table de3at.shes_stg_'+tab+'_xlsx')
    
