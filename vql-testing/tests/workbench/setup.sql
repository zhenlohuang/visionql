CREATE TABLE parity_photos USING IMAGES LOCATION '${IMAGES_LOCATION}';

CREATE MODEL parity_detector TYPE OBJECT_DETECTION FROM '${MODEL}'
OPTIONS (image_size = 640, format = 'yolo_e2e', labels = 'coco80', box_format = 'xyxy');

RESOLVE MODEL parity_detector;
