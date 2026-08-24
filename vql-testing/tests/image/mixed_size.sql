WITH inferred AS (
  SELECT
    width,
    VQL_DETECT(image) AS detections
  FROM photos
  WHERE uri LIKE '%/000000000049.jpg'
     OR uri LIKE '%/000000000061.jpg'
     OR uri LIKE '%/000000000077.jpg'
)
SELECT
  COUNT(*) = 3
  AND COUNT(detections) = 3
  AND COUNT(DISTINCT width) = 3 AS valid
FROM inferred
