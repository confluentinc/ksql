package io.confluent.ksql.util;


import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class KSQLConfig extends StreamsConfig {

    public final static String SCHEMA_FILE_PATH_CONFIG = "ksql.schema.file";
    public final static String PROP_FILE_PATH_CONFIG = "ksql.properties.file";

    public KSQLConfig(Map<?, ?> props) {
        super(props);
    }
}
