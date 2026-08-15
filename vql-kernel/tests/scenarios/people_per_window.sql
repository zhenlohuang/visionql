WITH per_window AS (
  SELECT
    TUMBLE(ts, INTERVAL '5' SECOND) AS window_start,
    SUM(CARDINALITY(IMAGE_DETECTION(
      'detector', frame,
      classes => ['person'], min_confidence => 0.5
    ))) AS people
  FROM clips
  WHERE uri LIKE '%/people-detection.mp4'
  GROUP BY 1
)
SELECT
  CAST(COUNT(*) AS BIGINT) AS windows,
  SUM(people) > 0 AS found_people
FROM per_window;
