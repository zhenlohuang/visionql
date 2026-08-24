CREATE TABLE people_stream USING RTSP OPTIONS (
  url = '${RTSP_URL}',
  fps = 1,
  event_time = 'capture_time',
  watermark = '2 seconds',
  transport = 'tcp'
);
