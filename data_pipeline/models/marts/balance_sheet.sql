SELECT 
    s.company_name, 
    n.period_end_date,
    p.preferred_label,
    n.reported_amount
FROM
    {{ ref('stg_num') }} as n
JOIN
    {{ ref('stg_pre') }} as p
        on n.submission_id = p.submission_id
        AND n.tag = p.tag
JOIN
    {{ ref('stg_sub') }} as s
        on n.submission_id = s.submission_id
WHERE p.statement_type = 'BS'
ORDER BY s.company_name, n.period_end_date, p.line