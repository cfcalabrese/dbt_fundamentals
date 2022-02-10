with casualty as (
  select
    accident_reference,
    casualty_severity
  from {{ source('gov_road_safety_data', 'casualty') }}
),

accident as (
  select
    accident_reference,
    accident_severity,
    day_of_week,
    road_type
  from {{ source('gov_road_safety_data', 'accident') }}
),

final as (
  select
    casualty_severity,
    accident_severity,
    day_of_week,
    road_type
  from casualty
  left join accident 
  using (accident_reference)
)

select * from final