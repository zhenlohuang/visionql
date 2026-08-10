CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person'
WITH (post_processor.kind = 'unknown');
