
SELECT
    adsh AS submission_id,
    TRY_CAST(report AS NUMBER) AS report,
    TRY_CAST(line AS NUMBER) AS line,
    stmt AS statement_type,
    TRY_CAST(inpth AS BOOLEAN) AS directly_reported,
    rfile AS rfile,
    tag AS tag,
    version as version,
    plabel AS preferred_label,
    TRY_CAST(negating AS BOOLEAN) AS negating
FROM {{ source('sec_source', 'raw_pre') }}