# Assignment

## Brief

Write the SQL statements for the following questions.

## Instructions

Paste the answer as SQL in the answer code section below each question.

### Question 1

Let's revisit our `austin_bikeshare_demo` dbt project. Modify the `dim_station.sql` model to include the following columns:

- `total_duration` (sum of `duration` for each station in seconds)
- `total_starts` (count of `start_station_name` for each station)
- `total_ends` (count of `end_station_name` for each station)

Then, rebuild the models with the following command to see if the changes are correct:

```bash
dbt run
```

Answer:

Paste the `dim_station.sql` model here:

```sql
{{ config(materialized='table') }}

WITH station_base AS (
    SELECT
        station_id,
        name AS station_name,
        status,
        address,
        number_of_docks,
        council_district
    FROM {{ source('austin_bikeshare', 'bikeshare_stations') }}
),

trip_starts AS (
    -- Clean + cast start_station_id once, then aggregate
    SELECT
        station_id,
        SUM(duration_minutes * 60) AS total_duration,
        COUNT(*) AS total_starts
    FROM (
        SELECT
            SAFE_CAST(start_station_id AS INT64) AS station_id,
            duration_minutes
        FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}
        WHERE start_station_id IS NOT NULL
    )
    WHERE station_id IS NOT NULL
    GROUP BY station_id
),

trip_ends AS (
    -- Clean + cast end_station_id once, then aggregate
    SELECT
        station_id,
        COUNT(*) AS total_ends
    FROM (
        SELECT
            SAFE_CAST(end_station_id AS INT64) AS station_id
        FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}
        WHERE end_station_id IS NOT NULL
    )
    WHERE station_id IS NOT NULL
    GROUP BY station_id
)

SELECT
    sb.station_id,
    sb.station_name,
    sb.status,
    sb.address,
    sb.number_of_docks,
    sb.council_district,
    COALESCE(ts.total_duration, 0) AS total_duration,
    COALESCE(ts.total_starts, 0)   AS total_starts,
    COALESCE(te.total_ends, 0)     AS total_ends
FROM station_base sb
LEFT JOIN trip_starts ts USING (station_id)
LEFT JOIN trip_ends  te USING (station_id);

```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
