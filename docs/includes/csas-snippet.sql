CREATE STREAM foo WITH (TIMESTAMP='t2') AS
  SELECT * FROM bar
  WINDOW TUMBLING (size 10 seconds);