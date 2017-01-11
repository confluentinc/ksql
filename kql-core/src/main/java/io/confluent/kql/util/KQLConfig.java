package io.confluent.kql.util;


import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class KQLConfig extends StreamsConfig {

  public final static String QUERY_FILE_PATH_CONFIG = "kql.query.file";
  public final static String SCHEMA_FILE_PATH_CONFIG = "kql.schema.file";
  public final static String PROP_FILE_PATH_CONFIG = "kql.properties.file";

  public final static String QUERY_CONTENT_CONFIG = "query";
  public final static String QUERY_EXECUTION_TIME_CONFIG = "terminate.in";

  public static final String DEFAULT_QUERY_FILE_PATH_CONFIG = "cli";
  public static final String DEFAULT_SCHEMA_FILE_PATH_CONFIG = "NULL";
  public static final String DEFAULT_PROP_FILE_PATH_CONFIG = "";

  public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
  public static final String DEFAULT_AUTO_OFFSET_RESET_CONFIG = "earliest";

  public static final String AVRO_SERDE_SCHEMA_CONFIG = "avro.serde.schema";

  public KQLConfig(Map<?, ?> props) {
    super(props);

  }
}
