SELECT
    tag AS tag,
    version as version,
    TRY_CAST(custom AS BOOLEAN) AS custom,
    TRY_CAST(abstract AS BOOLEAN) AS abstract,
    datatype as datatype,
    iord AS item_order,
    crdr AS balance_type,
    tlabel AS tag_label,
    doc AS documentation,
FROM {{ source('sec_source', 'raw_tag') }}