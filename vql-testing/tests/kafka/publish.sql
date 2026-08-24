INSERT INTO events
SELECT
  '000000000049.jpg' AS image,
  CARDINALITY(
    VQL_DETECT(
      image,
      classes => ['person'],
      min_score => 0.25
    )
  ) > 0 AS detected
FROM photos
WHERE uri LIKE '%/000000000049.jpg'
