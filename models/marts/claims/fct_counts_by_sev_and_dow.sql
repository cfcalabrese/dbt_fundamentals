with int_table as (
    select * from {{ ref('int_severity_time_road') }}
),

acc_sev as (
    select 
        cast(val as int) as accident_severity,
        label as accident_severity_name
        
    from {{ ref('stg_lookup_accident') }}
    where column_name = 'accident_severity'    
),

dow as (
    select 
        cast(val as int) as day_of_week,
        label as day_of_week_name
        
    from {{ ref('stg_lookup_accident') }}
    where column_name = 'day_of_week'
)

select 
    count(*) as counts,
    int_table.casualty_severity as casualty_severity,
    acc_sev.accident_severity_name as accident_severity,
    dow.day_of_week_name as day_of_week

from int_table
left join acc_sev using (accident_severity)
left join dow using (day_of_week)

group by 
    casualty_severity,
    accident_severity,
    day_of_week

order by 
    day_of_week asc,
    casualty_severity asc