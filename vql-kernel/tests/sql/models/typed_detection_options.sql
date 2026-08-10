CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person';

SELECT
  uri,
  COUNT_OBJECTS(
    DETECT_OBJECTS(
      'detector',
      image,
      min_confidence => 0.8,
      classes => ['vehicle']
    ),
    'person',
    0
  ) AS excluded_count,
  COUNT_OBJECTS(
    DETECT_OBJECTS(
      'detector',
      image,
      classes => ['person'],
      min_confidence => 0.85
    ),
    'person',
    0
  ) AS people_count
FROM photos
ORDER BY uri;
