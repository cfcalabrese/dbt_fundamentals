with lu_accident_casualty as (
  
  select 
    string_field_1 as column_name,
    string_field_2 as val,
    string_field_3 as label
  
  from {{ source('gov_road_safety_data', 'lookup') }} 
  where string_field_0 = 'Casualty'
    and string_field_3 is not null
)

select * from lu_accident_casualty