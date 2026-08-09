CREATE TABLE photos
USING IMAGES LOCATION '${TEST_DATA}/images';

SELECT uri, image, width, height
FROM photos
ORDER BY uri;
