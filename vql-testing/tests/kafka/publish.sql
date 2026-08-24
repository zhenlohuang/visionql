INSERT INTO events
SELECT *
FROM (
  VALUES
    (CAST(42 AS BIGINT), CAST(NULL AS VARCHAR)),
    (CAST(7 AS BIGINT), 'seven')
) AS rows(answer, note)
