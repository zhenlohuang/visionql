WITH inferred AS (
  SELECT
    VQL_DETECT(
      image,
      classes => ['person'],
      min_score => 0.25
    ) AS detections,
    VQL_DETECT(
      image,
      classes => ['not_a_coco_label']
    ) AS no_matches
  FROM photos
  WHERE uri LIKE '%/000000000049.jpg'
)
SELECT
  CARDINALITY(detections) > 0
  AND detections[1]['locator'] IS NOT NULL
  AND CARDINALITY(no_matches) = 0 AS valid
FROM inferred
