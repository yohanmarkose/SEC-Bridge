SELECT
    adsh AS submission_id,
    TRY_CAST(report AS INTEGER) AS report,
    TRY_CAST(line AS INTEGER) AS line,
    stmt AS stmt,
    TRY_CAST(inpth AS INTEGER) AS inpth,
    rfile AS rfile,
    tag AS tag,
    plabel AS presentation_label,
FROM {{ source('sec_source', 'raw_pre') }}