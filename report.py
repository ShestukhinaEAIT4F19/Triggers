#Подготовка отчета по триггеру
def report(curs):
    curs.execute('''
    insert into de3at.shes_stg_triggers_xlsx
    --Формирование CTE таблицы СС с необходимыми компаниями
    --(Нужны компании с лицензиями брокеров, депозитариев, управляющих компаний и дилеров)
    --(Необходимо исключить кредитные организации, имеющие только лицензию дилера)
    with rss as (select distinct 
        ss.dates,
        company.inn,
        company.name,
        ss.value as trg_nowvalue
    from de3at.shes_stg_ss_xlsx ss
    join de3at.shes_company company
    on ss.id = company.id
    and ss.dates between company.effective_from and company.effective_to
    and company.is_deleted = 0
    join de3at.shes_licenses licenses
    on company.id = licenses.id
    and licenses.lic_type in ('PT_BR','PT_DP', 'PT_SC', 'PT_DL')
    and ss.dates between licenses.effective_from and licenses.effective_to
    and licenses.is_deleted = 0
    join de3at.shes_stg_reports_xlsx reports
    on ss.form_id = reports.form_id 
    join de3at.shes_lines liness
    on ss.line_id = liness.line_id 
    and ss.dates between liness.effective_from and liness.effective_to
    and liness.is_deleted = 0
    where liness.line_name = 'Размер собственных средств'
    and reports.version = 0
    and company.inn not in
    (select inn from de3at.shes_stg_credit_org_xlsx
    where inn not in (
    select distinct company.inn 
    from de3at.shes_company company 
    join de3at.shes_licenses licenses on company.id = licenses.id where licenses.lic_type in ('PT_BR','PT_DP', 'PT_SC'))
    and inn in (select distinct company.inn from de3at.shes_company company 
    join de3at.shes_licenses licenses on company.id = licenses.id where licenses.lic_type = 'PT_DL'))),
    
    --Формирование CTE таблицы МРСС с необходимыми компаниями
    mrss as (select distinct 
        mrss.dates,
        company.inn,
        company.name,
        mrss.value as trg_nowvalue
    from de3at.shes_stg_mrss_xlsx mrss
    join de3at.shes_company company
    on mrss.id = company.id
    and mrss.dates between company.effective_from and company.effective_to
    and company.is_deleted = 0
    join de3at.shes_licenses licenses
    on company.id = licenses.id
    and licenses.lic_type in ('PT_BR','PT_DP', 'PT_SC', 'PT_DL')
    and mrss.dates between licenses.effective_from and licenses.effective_to
    and licenses.is_deleted = 0
    join de3at.shes_stg_reports_xlsx reports
    on mrss.form_id = reports.form_id 
    and reports.version = 0
    and company.inn not in
    (select inn from de3at.shes_stg_credit_org_xlsx
    where inn not in (
    select distinct company.inn 
    from de3at.shes_company company 
    join de3at.shes_licenses licenses on company.id = licenses.id where licenses.lic_type in ('PT_BR','PT_DP', 'PT_SC'))
    and inn in (select distinct company.inn from de3at.shes_company company
    join de3at.shes_licenses licenses on company.id = licenses.id where licenses.lic_type = 'PT_DL'))),
    
    --Формирование CTE таблицы клиентов с необходимыми компаниями
    clients as 
    (select 
        clients.dates,
        clients.trg_name,
        clients.inn,
        clients.name,
        sum(clients.trg_nowvalue) as trg_nowvalue 
    from
    (select distinct
        clients.dates,
        clients.inn,
        company.name,
        case when clients.bo_du_dep = 'Брокерская деятельность' then 'Клиенты на БО'
        when clients.bo_du_dep = 'Доверительное управление' then 'Клиенты ДУ'
        else 'Количество депонентов'
        end as trg_name,
        coalesce(clients.clients,0) as trg_nowvalue
    from de3at.shes_stg_clients_xlsx clients
    join de3at.shes_company company
    on clients.inn = company.inn 
    and clients.dates between company.effective_from and company.effective_to
    and company.is_deleted = 0
    join de3at.shes_licenses licenses
    on company.id = licenses.id
    and licenses.lic_type in ('PT_BR','PT_DP', 'PT_SC')
    and clients.dates between licenses.effective_from and licenses.effective_to
    and licenses.is_deleted = 0
    where clients.bo_du_dep in ('Брокерская деятельность','Доверительное управление','Депозитарная деятельность')
    ) clients
    group by dates, trg_name, inn, name),
    
    --Соединение трех таблиц (СС, МРСС, клиенты), с показателями за текущий и предыдущий месяц
    alldata as (
    select rss.dates,
        rss.inn,
        rss.name,
        'Собственные средства' as trg_name,
        coalesce(rss_last.trg_nowvalue,0) as trg_wasvalue,
        coalesce(rss.trg_nowvalue,0) as trg_nowvalue
    from rss
    left join rss rss_last
    on rss.inn = rss_last.inn
    and rss.dates > (rss_last.dates + interval '27' day)
    and rss.dates < (rss_last.dates + interval '32' day)
    where rss.dates > to_date('2021-12-31', 'YYYY-MM-DD')
    union all
    select distinct mrss.dates,
        mrss.inn,
        mrss.name,
        'РСС ниже норматива' as trg_name,
        coalesce(mrss.trg_nowvalue,0) as trg_wasvalue,
        coalesce(rss.trg_nowvalue,0) as trg_nowvalue
    from mrss
    left join rss
    on rss.inn = mrss.inn and rss.dates = mrss.dates
    where mrss.dates > to_date('2021-12-31', 'YYYY-MM-DD')
    union all
    select clients.dates,
        clients.inn,
        clients.name,
        clients.trg_name,
        coalesce(clients_last.trg_nowvalue,0) as trg_wasvalue,
        clients.trg_nowvalue
    from clients
    left join clients clients_last
    on clients.inn = clients_last.inn and clients.trg_name = clients_last.trg_name
    and clients.dates > (clients_last.dates + interval '27' day)
    and clients.dates < (clients_last.dates + interval '32' day)
    where clients.dates > to_date('2021-12-31', 'YYYY-MM-DD')
    ),
    
    --Подтягивание вспомогательных стобцов (категория, куратор и тд), 
    --расчет максимального значения из текущего и предыдущего
    parametrs as (
    select distinct
        alldata.dates,
        'месяц' as period,
        case when coalesce(categorization.category,'Малые') <> 'Крупные' 
            and coalesce(coalesce(curators.department,curators.other_dep),'ДИФР') <> 'ДИФР' 
            and alldata.trg_name in ('РСС ниже норматива', 'Собственные средства') then 'ВВГУ'
        else '-' end as trg_dep,
        coalesce(coalesce(curators.department,curators.other_dep),'ДИФР') as department,
        coalesce(curators.curator,'-') as curator,
        'ПУРЦБ' as views,
        alldata.inn,
        alldata.name,
        case when alldata.inn in (select inn from de3at.shes_stg_credit_org_xlsx) then 'КО'
        else 'НФО' end as sign,
        coalesce(categorization.category,'Малые') as category,
        alldata.trg_name,
        case when alldata.trg_name = 'РСС ниже норматива' then 
        'ТР_'||cast(alldata.inn as varchar2(100))||'_'||cast(extract(year from alldata.dates) as varchar2(100))||'_'||cast(extract(month from alldata.dates) as varchar2(100))||'_МРСС'
        when alldata.trg_name = 'Собственные средства' then
        'ТР_'||cast(alldata.inn as varchar2(100))||'_'||cast(extract(year from alldata.dates) as varchar2(100))||'_'||cast(extract(month from alldata.dates) as varchar2(100))||'_РСС'
        when alldata.trg_name = 'Клиенты на БО' then
        'ТР_'||cast(alldata.inn as varchar2(100))||'_'||cast(extract(year from alldata.dates) as varchar2(100))||'_'||cast(extract(month from alldata.dates) as varchar2(100))||'_КБО'
        when alldata.trg_name = 'Клиенты ДУ' then
        'ТР_'||cast(alldata.inn as varchar2(100))||'_'||cast(extract(year from alldata.dates) as varchar2(100))||'_'||cast(extract(month from alldata.dates) as varchar2(100))||'_КДУ'
        when alldata.trg_name = 'Количество депонентов' then
        'ТР_'||cast(alldata.inn as varchar2(100))||'_'||cast(extract(year from alldata.dates) as varchar2(100))||'_'||cast(extract(month from alldata.dates) as varchar2(100))||'_КД'
        end as trg_num,
        case when alldata.trg_name = 'РСС ниже норматива' then '-'
        when alldata.trg_name = 'Собственные средства' then  'Руб.'
        else 'ед.' end as ed_izm,
        round(alldata.trg_wasvalue,2) as trg_wasvalue,
        round(alldata.trg_nowvalue,2) as trg_nowvalue,
        round(alldata.trg_nowvalue-alldata.trg_wasvalue,2) as abs_ch,
        case when alldata.trg_wasvalue <>0 then round(alldata.trg_nowvalue/alldata.trg_wasvalue -1,4)
        else 0 end as otn_ch,
        case when alldata.trg_nowvalue>alldata.trg_wasvalue then alldata.trg_nowvalue
        else alldata.trg_wasvalue end as maximum
    from alldata
    left join de3at.shes_curators curators
    on curators.inn = alldata.inn and alldata.dates between curators.effective_from and curators.effective_to
    and curators.is_deleted = 0
    left join de3at.shes_stg_categorization categorization
    on categorization.inn = alldata.inn and alldata.dates 
        between categorization.effective_from and categorization.effective_to),
    
    --Расчет пороговых значений для выявления триггеров: для разного уровня 
    --максимального значения показателя разные пороговые значения
    trig as (
    select parametrs.*,
    case when trg_name = 'РСС ниже норматива' then 0
    when trg_name = 'Собственные средства' then
         case when maximum/1000000> 7000 then 0.2
                when maximum/1000000 >1000 
                    and maximum/1000000 <=7000 then 0.25
                when maximum/1000000 >3500 
                    and maximum/1000000 <=1000 then 0.28
                when maximum/1000000 >100 
                    and maximum/1000000 <=350 then 0.3
                when maximum/1000000 >50 
                    and maximum/1000000 <=100 then 0.35
                else 0.4 end
    when trg_name = 'Клиенты на БО' then
        case when maximum> 100000 then 0.05
                when maximum >30000 
                    and maximum <=100000 then 0.07
                when maximum >10000 
                    and maximum <=30000 then 0.1
                when maximum >3000 
                    and maximum <=10000 then 0.15
                when maximum >1000
                    and maximum <=3000 then 0.2
                when maximum >400
                    and maximum <=1000 then 0.25
                when maximum >100
                    and maximum <=400 then 0.3
                when maximum >10
                    and maximum <=100 then 0.7
                else 
                    case when otn_ch<0 then 1 else 3 end end
    when trg_name = 'Клиенты ДУ' then
        case when maximum> 5000 then 0.1
                when maximum >1000 
                    and maximum <=5000 then 0.15
                when maximum >500 
                    and maximum <=1000 then 0.2
                when maximum >100 
                    and maximum <=500 then 0.25
                when maximum >30
                    and maximum <=100 then 0.3
                when maximum >10
                    and maximum <=30 then 0.35
                else 
                    case when otn_ch<0 then 1 else 3 end end
    when trg_name = 'Количество депонентов' then
        case when maximum> 100000 then 0.05
                when maximum >30000 
                    and maximum <=100000 then 0.1
                when maximum >10000 
                    and maximum <=30000 then 0.15
                when maximum >5000 
                    and maximum <=10000 then 0.2
                when maximum >1000
                    and maximum <=5000 then 0.25
                when maximum >500
                    and maximum <=1000 then 0.3
                when maximum >100
                    and maximum <=500 then 0.35
                when maximum >50
                    and maximum <=100 then 0.4
                when maximum >10
                    and maximum <=50 then 0.45
                else 
                    case when otn_ch<0 then 1 else 3 end end
     end as parametr
     from parametrs)
    
    --Расчет триггеров и получения столбца диапазона значений
    select trg_num,dates,period,trg_dep,department,curator,views,inn,name,sign,category,
        trg_name,ed_izm,trg_wasvalue,trg_nowvalue,abs_ch,otn_ch,
        case when trg_name = 'РСС ниже норматива' then '-'
        when trg_name = 'Собственные средства' 
            then case when trg_wasvalue = 0 then '0' else
            '(-'||cast(parametr as varchar2(100))||';'||cast(parametr as varchar2(100))||')' end
        else case when trg_wasvalue = 0 then '(-10;10)'
        else '(-'||cast(parametr as varchar2(100))||';'||cast(parametr as varchar2(100))||')'
        end
        end as diapazon,
        case when trg_name = 'РСС ниже норматива' then
            case when trg_nowvalue<trg_wasvalue then 1 else 0 end
        when trg_name = 'Собственные средства' then
         case when trg_wasvalue = 0 then 1 else 
            case when abs(otn_ch) >= parametr then 1 else 0 end end
        else case when trg_wasvalue = 0 then
            case when abs(abs_ch) >= parametr then 1 else 0 end
            else case when abs(otn_ch) >= parametr then 1 else 0 end end
        end as trig,
        current_date as update_date
    from trig
    order by trg_name,inn
    ''')
    
