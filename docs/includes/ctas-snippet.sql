CREATE TABLE foo WITH (TIMESTAMP='t2') AS
  SELECT host, COUNT(*) FROM bar
  WINDOW TUMBLING (size 10 seconds)
  GROUP BY host
  EMIT CHANGES;
