CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person'
FUNCTION detect;

SELECT uri, COUNT_OBJECTS(detect(image), 'person', 0.6) AS people
FROM photos
ORDER BY uri;

SELECT uri, det
FROM photos, UNNEST(detect(image)) AS u(det)
ORDER BY uri;
