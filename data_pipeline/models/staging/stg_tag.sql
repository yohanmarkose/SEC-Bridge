SELECT
    tag AS tag,
    iord AS item_order,
    crdr AS balance_type,
    tlabel AS tag_label,
    doc AS documentation,
FROM {{ source('sec_source', 'raw_tag') }}