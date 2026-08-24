CREATE TABLE people_stream USING RTSP OPTIONS (
  url = '${RTSP_URL}',
  fps = 1,
  event_time = 'capture_time',
  watermark = '2 seconds',
  transport = 'tcp'
);

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'file://${MODEL_PATH}'
OPTIONS (image_size = 640, format = 'yolo_e2e', labels = 'coco80', box_format = 'xyxy');

RESOLVE MODEL detector;
