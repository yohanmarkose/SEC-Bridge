SELECT 
    s.company_name,
    s.company_id,
    s.filing_date,
    s.period,
    s.fiscal_year,
    s.fiscal_period,
    n.unit,
    p.preferred_label,
    SUM(n.reported_amount) AS total_reported_amount,  -- Aggregation
    t.tag,
    t.datatype,
    t.documentation,
FROM
    {{ ref('stg_num') }} as n
JOIN
    {{ ref('stg_sub') }} as s
        on n.submission_id = s.submission_id
JOIN
    {{ ref('stg_pre') }} as p
        on n.submission_id = p.submission_id
        AND n.tag = p.tag
        AND n.version = p.version
JOIN
    {{ ref('stg_tag') }} as t
        on n.tag = t.tag
        AND n.version = t.version
WHERE
    p.statement_type = 'IS'
GROUP BY  -- Group by all non-aggregated columns
    s.company_name,
    s.company_id,
    s.filing_date,
    s.period,
    s.fiscal_year,
    s.fiscal_period,
    n.unit,
    t.tag,
    p.preferred_label,
    t.datatype,
    t.documentation
ORDER BY 
    s.company_name, 
    s.period