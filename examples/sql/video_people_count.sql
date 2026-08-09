CREATE TABLE entrance_videos
USING VIDEOS
LOCATION './data/datasets/videos/sample-videos/'
WITH (fps = 5);

CREATE MODEL yolo26n
TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx'
WITH (processor = 'yolo26-detect-v1');

CREATE FUNCTION detect USING MODEL yolo26n
WITH (classes = ['person'], min_confidence = 0.6);

CREATE SINK console_output TYPE console;

INSERT INTO console_output
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts,
         COUNT_OBJECTS(detect(frame), 'person', 0.6) AS person_cnt
  FROM entrance_videos
)
GROUP BY 1;
