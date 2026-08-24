WITH sampled AS (
  SELECT
    pts_ms,
    frame_id,
    width,
    height,
    VQL_DETECT(
      frame,
      classes => ['person'],
      min_score => 0.5
    ) AS detections
  FROM clips
  WHERE uri LIKE '%/people-detection.mp4'
  ORDER BY pts_ms
  LIMIT 8
)
SELECT
  COUNT(*) = 8
  AND COUNT(DISTINCT frame_id) = 8
  AND MIN(frame_id) = 0
  AND MAX(frame_id) = 7
  AND COUNT(DISTINCT pts_ms) = 8
  AND MIN(pts_ms) = 0
  AND MIN(width) > 0
  AND MIN(height) > 0
  AND SUM(CARDINALITY(detections)) > 0 AS valid
FROM sampled
