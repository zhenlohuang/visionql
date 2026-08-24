SELECT
  frame_id,
  CARDINALITY(
    VQL_DETECT(
      frame,
      classes => ['person'],
      min_score => 0.5
    )
  ) AS people
FROM people_stream
LIMIT 8
