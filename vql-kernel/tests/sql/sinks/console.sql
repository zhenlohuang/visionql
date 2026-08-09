CREATE SINK terminal TYPE console;

INSERT INTO terminal
SELECT 42 AS answer;

SHOW SINKS;
