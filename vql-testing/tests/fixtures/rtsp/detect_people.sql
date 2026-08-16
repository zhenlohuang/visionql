SELECT
  frame_id,
  CARDINALITY(
    IMAGE_DETECTION(
      'detector',
      frame,
      classes => ['person'],
      min_confidence => 0.5
    )
  ) AS people
FROM people_stream
LIMIT 8
