CREATE TABLE entrance_videos
USING VIDEOS
LOCATION './data/datasets/videos/sample-videos/'
WITH (fps = 5);

CREATE MODEL yolo26n
TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx'
WITH (
  runtime.kind = 'onnxruntime',
  pre_processor.kind = 'vision.image_tensor@1',
  pre_processor.options = {
    input_name = 'images',
    width = 640,
    height = 640,
    resize = 'letterbox',
    color_space = 'rgb',
    layout = 'nchw'
  },
  post_processor.kind = 'vision.yolo_e2e@1',
  post_processor.options = {
    output_name = 'output0',
    box_format = 'xyxy',
    labels = 'coco80'
  }
);

CREATE SINK console_output TYPE console;

INSERT INTO console_output
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
