SELECT CAST(COUNT(*) AS BIGINT) AS windows
FROM (
  SELECT TUMBLE(ts, INTERVAL '5' SECOND)
  FROM clips
  WHERE uri LIKE '%/people-detection.mp4'
  GROUP BY 1
);
