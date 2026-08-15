WITH inferred AS (
  SELECT
    width,
    IMAGE_DETECTION('detector', image) AS detections
  FROM photos
  WHERE uri LIKE '%/000000000049.jpg'
     OR uri LIKE '%/000000000061.jpg'
     OR uri LIKE '%/000000000077.jpg'
)
SELECT
  CAST(COUNT(*) AS BIGINT) AS images,
  COUNT(detections) = 3 AS all_inferred,
  COUNT(DISTINCT width) = 3 AS mixed_widths
FROM inferred;
