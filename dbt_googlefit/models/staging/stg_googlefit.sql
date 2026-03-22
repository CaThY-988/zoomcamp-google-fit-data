{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('google_fit', 'raw_google_fit') }}

),

renamed as (

    select

        -- identifiers
        cast(user_id as integer) as user_id,
        cast(date as date) as date,
        cast(day_index as integer) as day_index,

        -- demographics
        cast(age as integer) as age,
        cast(sex as integer) as sex,
        cast(bmi as double) as bmi,
        cast(smoking_status as integer) as smoking_status,
        cast(family_history_cvd as integer) as family_history_cvd,
        cast(fitness_level as integer) as fitness_level,

        -- physical metrics
        cast(height_m as double) as height_m,
        cast(weight_kg as double) as weight_kg,

        -- activity
        lower(activity_type) as activity_type,
        cast(steps as integer) as steps,
        cast(heart_points as integer) as heart_points,
        cast(move_minutes as integer) as move_minutes,
        cast(calories_burned as double) as calories_burned,
        cast(distance_km as double) as distance_km,
        cast(step_cadence as double) as step_cadence,
        cast(cycling_power_watts as double) as cycling_power_watts,

        -- heart & vitals
        cast(avg_heart_rate as integer) as avg_heart_rate,
        cast(resting_hr as integer) as resting_hr,
        cast(hrv as integer) as hrv,

        cast(bp_systolic as integer) as bp_systolic,
        cast(bp_diastolic as integer) as bp_diastolic,

        cast(fasting_glucose as double) as fasting_glucose,
        cast(postprandial_glucose as double) as postprandial_glucose,

        cast(spo2 as integer) as spo2,
        cast(body_temp_c as double) as body_temp_c,

        -- sleep
        cast(sleep_hours as double) as sleep_hours,
        cast(sleep_efficiency as double) as sleep_efficiency,

        -- lifestyle
        cast(fatigue_score as integer) as fatigue_score,
        cast(calories_consumed as double) as calories_consumed,
        cast(water_intake_l as double) as water_intake_l,

        -- flags
        cast(sleep_data_available as boolean) as sleep_data_available,
        cast(bp_measured as boolean) as bp_measured,
        cast(glucose_measured as boolean) as glucose_measured,

        -- outcome
        cast(cardiometabolic_risk_state as integer) as cardiometabolic_risk_state

    from source

)

select *
from renamed