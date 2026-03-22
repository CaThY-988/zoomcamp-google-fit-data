{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('stg_googlefit') }}

),

final as (

    select

        -- grain
        user_id,
        date,

        -- demographics (optional carry-through)
        age,
        sex,
        bmi,

        -- activity
        steps,
        calories_burned,
        move_minutes,

        case
            when steps > 12000 then 'high'
            when steps > 8000 then 'medium'
            else 'low'
        end as activity_level,

        -- sleep
        sleep_hours,
        sleep_efficiency,

        case
            when sleep_hours >= 7 then 'good'
            when sleep_hours >= 5 then 'moderate'
            else 'poor'
        end as sleep_quality,

        -- cardiovascular
        avg_heart_rate,
        resting_hr,
        bp_systolic,
        bp_diastolic,

        -- derived health signals
        (bp_systolic - bp_diastolic) as pulse_pressure,

        -- lifestyle
        calories_consumed,
        water_intake_l,

        -- outcome
        cardiometabolic_risk_state,

        -- simple composite score (nice for demo)
        (
            coalesce(steps / 1000.0, 0)
            + coalesce(sleep_hours, 0)
            - coalesce(resting_hr / 10.0, 0)
        ) as wellness_score

    from base

)

select *
from final