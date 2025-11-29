WITH deduped AS (
    SELECT DISTINCT ON (
        inc_business_service,
        inc_category,
        inc_number,
        inc_priority,
        inc_sla_due,
        inc_sys_created_on,
        inc_resolved_at,
        inc_assigned_to,
        inc_state,
        inc_cmdb_ci,
        inc_caller_id,
        inc_short_description,
        inc_assignment_group,
        inc_close_code,
        inc_close_notes
    ) *
    FROM servicenow_tickets
    ORDER BY
        inc_business_service,
        inc_category,
        inc_number,
        inc_priority,
        inc_sla_due,
        inc_sys_created_on,
        inc_resolved_at,
        inc_assigned_to,
        inc_state,
        inc_cmdb_ci,
        inc_caller_id,
        inc_short_description,
        inc_assignment_group,
        inc_close_code,
        inc_close_notes
)

SELECT
    st.inc_business_service,
    st.inc_category,
    st.inc_number,
    st.inc_priority,
    st.inc_sla_due,
    to_timestamp(st.inc_sys_created_on, 'DD/MM/YY HH24:MI') AS inc_sys_created_on_ts,
    CASE
        WHEN st.inc_resolved_at IS NOT NULL THEN to_timestamp(st.inc_resolved_at, 'DD/MM/YY HH24:MI')
        ELSE to_timestamp(st.inc_sys_created_on, 'DD/MM/YY HH24:MI')
             + INTERVAL '1 second' *
               (CASE 
                    WHEN st.inc_category = 'Network' THEN 98.31 * 3600
                    WHEN st.inc_category = 'Software' THEN 86.325 * 3600
                    WHEN st.inc_category = 'Finance' THEN 84.29 * 3600
                    WHEN st.inc_category = 'Workstation' THEN 19.44 * 3600
                    WHEN st.inc_category = 'Facilities' THEN 17.366 * 3600
                    WHEN st.inc_category = 'Hardware-Infrastructure' THEN 12.199 * 3600
                    WHEN st.inc_category = 'Business Service' THEN 12.19 * 3600
                    WHEN st.inc_category = 'Collaboration' THEN 1.281 * 3600
                    WHEN st.inc_category = 'Hardware' THEN 0.06 * 3600
                    ELSE 0
                END)
    END AS inc_resolved_at_ts,
    st.inc_assigned_to,
    st.inc_state,
    st.inc_cmdb_ci,
    st.inc_caller_id,
    st.inc_short_description,
    st.inc_assignment_group,
    st.inc_close_code,
    st.inc_close_notes
FROM deduped st
WHERE st.inc_state = 'Closed'
