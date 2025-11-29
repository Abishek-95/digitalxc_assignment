SELECT 
    EXTRACT(MONTH FROM stp.inc_sys_created_on) AS month,
    COUNT(*) AS total_ticket,
    AVG(EXTRACT(EPOCH FROM (stp.inc_resolved_at - stp.inc_sys_created_on)) / 3600) AS avg_resolved_hours,
    ROUND(COUNT(*)::numeric / 
          (SELECT COUNT(*) FROM servicenow_ticket_prod WHERE inc_state = 'Closed') * 100, 2
    ) AS ticket_closure_rate
FROM servicenow_ticket_prod stp
GROUP BY 1