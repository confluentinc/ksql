package io.confluent.ksql.util;


import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class KSQLConfig extends StreamsConfig {

    public final static String QUERY_FILE_PATH_CONFIG = "ksql.query.file";
    public final static String SCHEMA_FILE_PATH_CONFIG = "ksql.schema.file";
    public final static String PROP_FILE_PATH_CONFIG = "ksql.properties.file";

    public static final String DEFAULT_QUERY_FILE_PATH_CONFIG = "cli";
    public static final String DEFAULT_SCHEMA_FILE_PATH_CONFIG = "/tmp/ksql/schema.json";
    public static final String DEFAULT_PROP_FILE_PATH_CONFIG = "";

    public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final String DEFAULT_AUTO_OFFSET_RESET_CONFIG = "earliest";



    public KSQLConfig(Map<?, ?> props) {
        super(props);

    }
}
