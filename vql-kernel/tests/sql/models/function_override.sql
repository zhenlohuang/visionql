CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person'
WITH (
  labels = ['person'],
  classes = ['vehicle'],
  min_confidence = 0.8
);

CREATE FUNCTION model_defaults USING MODEL detector;
CREATE FUNCTION people USING MODEL detector
WITH (classes = ['person'], min_confidence = 0.85);

SELECT
  uri,
  COUNT_OBJECTS(model_defaults(image), 'person', 0) AS default_count,
  COUNT_OBJECTS(people(image), 'person', 0) AS overridden_count
FROM photos
ORDER BY uri;
