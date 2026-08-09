CREATE FUNCTION is_large(x BIGINT)
RETURNS BOOLEAN
AS (x > 2);

SELECT value, is_large(value) AS large
FROM (VALUES (CAST(NULL AS BIGINT)), (1), (3)) AS input(value)
ORDER BY value NULLS FIRST;
