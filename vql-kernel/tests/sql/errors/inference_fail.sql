CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/broken-images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'endpoint://http://127.0.0.1:9/infer'
FUNCTION detect;

SET vql.on_error = 'fail';

SELECT detect(image)
FROM photos;
