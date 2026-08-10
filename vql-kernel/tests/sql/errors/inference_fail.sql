CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/broken-images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'endpoint://http://127.0.0.1:9'
WITH (
  runtime.kind = 'triton',
  runtime.protocol = 'kserve_v2_http',
  runtime.model_name = 'detector'
);

SET vql.on_error = 'fail';

SELECT DETECT_OBJECTS('detector', image)
FROM photos;
