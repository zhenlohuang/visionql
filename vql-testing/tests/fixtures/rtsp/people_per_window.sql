WITH detected AS (
  SELECT
    ts,
    CARDINALITY(
      IMAGE_DETECTION(
        'detector',
        frame,
        classes => ['person'],
        min_confidence => 0.5
      )
    ) AS people
  FROM people_stream
)
SELECT
  TUMBLE(ts, INTERVAL '2' SECOND) AS window_start,
  COUNT(*) AS frames,
  SUM(people) AS total_people,
  AVG(people) AS average_people,
  MIN(people) AS minimum_people,
  MAX(people) AS maximum_people
FROM detected
GROUP BY 1
LIMIT 2
