CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/images';

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person'
WITH (
  runtime.kind = 'onnxruntime',
  pre_processor.kind = 'vision.image_tensor@1',
  pre_processor.options = {
    input_name = 'images', width = 640, height = 640, resize = 'letterbox'
  },
  post_processor.kind = 'vision.yolo_e2e@1',
  post_processor.options = {output_name = 'output0', labels = ['person']}
);

SELECT uri,
       COUNT_OBJECTS(DETECT_OBJECTS('detector', image), 'person', 0.6) AS people
FROM photos
ORDER BY uri;

SELECT uri, det
FROM photos, UNNEST(DETECT_OBJECTS('detector', image)) AS u(det)
ORDER BY uri;
