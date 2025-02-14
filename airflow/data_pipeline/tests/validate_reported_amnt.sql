SELECT
  submission_id,
  reported_amount
FROM {{ ref('stg_num') }}
WHERE
  -- Example: Revenue should be positive
  (tag = 'us-gaap:Revenue' AND reported_amount < 0)
  OR
  -- Example: Liabilities should not be negative
  (tag = 'us-gaap:Liabilities' AND reported_amount < 0)