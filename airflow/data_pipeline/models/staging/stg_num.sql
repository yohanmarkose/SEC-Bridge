SELECT
    adsh AS submission_id,
    tag AS tag,
    version as version,
    TRY_TO_DATE(ddate, 'YYYYMMDD') AS period_end_date, 
    TRY_CAST(qtrs AS NUMBER) AS num_quaters_covered,
    uom AS unit,
    segments as segments,
    coreg as coreg,
    TRY_CAST(value AS NUMBER) AS reported_amount,
    footnote as footnote
FROM {{ source('sec_source', 'raw_num') }}