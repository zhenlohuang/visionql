CREATE MODEL detector
TYPE OBJECT_DETECTION
FROM 'mock://person'
WITH (processor = 'unknown');
