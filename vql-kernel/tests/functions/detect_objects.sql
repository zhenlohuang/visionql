SELECT COUNT(*) > 0 AS found
FROM photos,
     UNNEST(DETECT_OBJECTS(
       'detector',
       image,
       classes => ['person'],
       min_confidence => 0.25
     )) AS u(detection)
WHERE uri LIKE '%/000000000049.jpg';
