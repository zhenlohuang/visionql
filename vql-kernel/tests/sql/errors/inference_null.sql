CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/broken-images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'endpoint://http://127.0.0.1:9/infer'
FUNCTION detect;

SELECT COUNT_OBJECTS(detect(image), 'person', 0.5) IS NULL AS inference_failed
FROM photos;
