CREATE TABLE entrance_videos USING VIDEOS
LOCATION './data/datasets/videos/sample-videos/' OPTIONS (fps = 5);
CREATE MODEL yolo26n TYPE OBJECT_DETECTION
FROM './data/models/yolo26n.onnx';
RESOLVE MODEL yolo26n;
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS window_start,
       AVG(person_cnt) AS avg_people,
       MAX(person_cnt) AS peak_people
FROM (
  SELECT ts, CARDINALITY(yolo26n(frame, classes => ['person'],
         min_confidence => 0.6)) AS person_cnt
  FROM entrance_videos
)
GROUP BY 1;
