SELECT COUNT_OBJECTS(
  DETECT_OBJECTS('detector', image, classes => ['person'], min_confidence => 0.25),
  'person',
  0.25
) > 0 AS found
FROM photos
WHERE uri LIKE '%/000000000049.jpg';
