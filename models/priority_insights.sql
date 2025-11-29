SELECT 
    split_part(stp.inc_priority ,'-',2) AS priority_status,
    AVG(EXTRACT(EPOCH FROM (stp.inc_resolved_at_ts - stp.inc_sys_created_on_ts)) / 3600) AS avg_resolved_hours
FROM {{ ref('cleaned_data') }} stp
GROUP BY 1