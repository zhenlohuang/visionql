CREATE STREAM people_stream FROM '${RTSP_URL}' WITH (
  fps = 1,
  event_time = 'capture_time',
  watermark = INTERVAL '2' SECOND,
  transport = 'tcp'
);

CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'file://${MODEL_PATH}'
USING ONNX_RUNTIME;

RESOLVE MODEL detector;
