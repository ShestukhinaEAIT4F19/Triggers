#Импортируем excel файлы
import pandas as pd
def read_xlsx(curs,dates):
    categorization = pd.read_excel('/home/de3at/shes/source/categorization.xlsx', index_col=None, header=0)
    clients = pd.read_excel('/home/de3at/shes/source/clients.xlsx', index_col=None, header=0)
    credit_org = pd.read_excel('/home/de3at/shes/source/credit_org.xlsx', index_col=None, header=0)
    company = pd.read_excel('/home/de3at/shes/source/company.xlsx', index_col=None, header=0)
    curators = pd.read_excel('/home/de3at/shes/source/curators.xlsx', index_col=None, header=0)
    licenses = pd.read_excel('/home/de3at/shes/source/licenses.xlsx', index_col=None, header=0)
    lines = pd.read_excel('/home/de3at/shes/source/lines.xlsx', index_col=None, header=0)
    mrss = pd.read_excel('/home/de3at/shes/source/MRSS.xlsx', index_col=None, header=0)
    reports = pd.read_excel('/home/de3at/shes/source/reports.xlsx', index_col=None, header=0)
    ss = pd.read_excel('/home/de3at/shes/source/SS.xlsx', index_col=None, header=0)

#меняем формат дат
    categorization.iloc[:,2] = categorization.iloc[:,2].apply(lambda s: str(s))
    clients.iloc[:,3]  = clients.iloc[:,3].apply(lambda s: str(s))
    mrss.iloc[:,4] = mrss.iloc[:,4].apply(lambda s: str(s))
    ss.iloc[:,4] = ss.iloc[:,4].apply(lambda s: str(s))
    
#Заполняем таблицы данными
    sql_insert_categorization_stg = '''
        insert into de3at.shes_stg_categorization_xlsx values
        (?, ?,to_date(?,'YYYY-MM-DD HH24:MI:SS'))'''
    curs.executemany(sql_insert_categorization_stg,[tuple(y for y in x) for x in categorization.values])
    
    sql_insert_clients_stg = '''
        insert into de3at.shes_stg_clients_xlsx values
        (?,?,?,to_date(?,'YYYY-MM-DD HH24:MI:SS'))'''
    curs.executemany(sql_insert_clients_stg,[tuple(y for y in x) for x in clients.values])
   
    sql_insert_company_stg = '''
        insert into de3at.shes_stg_company_xlsx values
        (?,?,?,?,?,'''+dates+')'
    curs.executemany(sql_insert_company_stg,[tuple(y for y in x) for x in company.values])
    
    sql_insert_credit_org_stg = '''
        insert into de3at.shes_stg_credit_org_xlsx values
        (?)'''
    df_to_export = credit_org.to_dict('list')
    curs.executemany(sql_insert_credit_org_stg, zip(*(df_to_export.values())))
    
    sql_insert_curators_stg = '''
        insert into de3at.shes_stg_curators_xlsx (inn,curator,department,other_dep,update_date) values
        (?,?,?,?,'''+dates+')'
    curs.executemany(sql_insert_curators_stg,[tuple(y for y in x) for x in curators.values])
    
    sql_insert_licenses_stg = '''
        insert into de3at.shes_stg_licenses_xlsx values
        (?,?,?,'''+dates+')'
    curs.executemany(sql_insert_licenses_stg,[tuple(y for y in x) for x in licenses.values])
    
    sql_insert_lines_stg  = '''
        insert into de3at.shes_stg_lines_xlsx values
        (?,?,'''+dates+')'''
    curs.executemany(sql_insert_lines_stg,[tuple(y for y in x) for x in lines.values])
    
    sql_insert_MRSS_stg = '''
        insert into de3at.shes_stg_MRSS_xlsx values
        (?,?,?,?,to_date(?,'YYYY-MM-DD HH24:MI:SS'))'''
    curs.executemany(sql_insert_MRSS_stg,[tuple(y for y in x) for x in mrss.values])
    
    sql_insert_reports_stg = '''
        insert into de3at.shes_stg_reports_xlsx values
        (?,?)'''
    curs.executemany(sql_insert_reports_stg,[tuple(y for y in x) for x in reports.values])
    
    sql_insert_SS_stg = '''
        insert into de3at.shes_stg_SS_xlsx values
        (?,?,?,?,to_date(?,'YYYY-MM-DD HH24:MI:SS'))'''
    curs.executemany(sql_insert_SS_stg,[tuple(y for y in x) for x in ss.values])
    
