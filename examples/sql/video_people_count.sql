CREATE TABLE entrance_videos
USING VIDEOS
LOCATION './data/datasets/videos/sample-videos/'
OPTIONS (fps = 5);

CREATE MODEL yolo26n
TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx'
USING ONNX_RUNTIME
WITH (
  input = {
    name = 'images',
    width = 640,
    height = 640,
    resize = 'letterbox',
    color_space = 'rgb',
    layout = 'nchw'
  },
  output = {
    name = 'output0',
    format = 'yolo_e2e',
    box_format = 'xyxy',
    labels = 'coco80'
  }
);

RESOLVE MODEL yolo26n;

SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         CARDINALITY(IMAGE_DETECTION(
           'yolo26n',
           frame,
           classes => ['person'],
           min_confidence => 0.6
         )) AS person_cnt
  FROM entrance_videos
)
GROUP BY 1;
