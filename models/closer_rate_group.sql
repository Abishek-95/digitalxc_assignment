WITH cte AS (
    SELECT 
        inc_assigned_to, 
        COUNT(*) AS closed_count
    FROM {{ ref('cleaned_data') }}
    WHERE inc_state = 'Closed'
    GROUP BY inc_assigned_to
)
SELECT 
    *,
    round(closed_count::numeric / (SELECT COUNT(*) FROM cleaned_data WHERE inc_state = 'Closed') * 100,2) AS ticket_closure_rate
FROM cte