SELECT
    adsh AS submission_id,
    tag AS tag,
    TRY_TO_DATE(ddate, 'YYYYMMDD') AS period_end_date, 
    TRY_CAST(qtrs AS INTEGER) AS num_quaters_covered,
    uom AS unit,
    segments as segments,
    TRY_CAST(value AS NUMBER) AS reported_value,
FROM {{ source('sec_source', 'raw_num') }}