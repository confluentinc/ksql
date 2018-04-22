/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.ddl;

public class DdlConfig {

  public static final String VALUE_FORMAT_PROPERTY = "VALUE_FORMAT";
  public static final String AVRO_SCHEMA_FILE = "AVROSCHEMAFILE";
  public static final String AVRO_SCHEMA = "AVROSCHEMA";
  public static final String KAFKA_TOPIC_NAME_PROPERTY = "KAFKA_TOPIC";
  public static final String TOPIC_NAME_PROPERTY = "REGISTERED_TOPIC";
  public static final String STATE_STORE_NAME_PROPERTY = "STATESTORE";
  public static final String KEY_NAME_PROPERTY = "KEY";
  public static final String IS_WINDOWED_PROPERTY = "WINDOWED";
  public static final String TIMESTAMP_NAME_PROPERTY = "TIMESTAMP";
  public static final String PARTITION_BY_PROPERTY = "PARTITION_BY";
  public static final String SCHEMA_FILE_CONTENT_PROPERTY = "ksql.schema.file.content";
  public static final String RUN_SCRIPT_STATEMENTS_CONTENT = "ksql.run.script.statements";
  public static final String TIMESTAMP_FORMAT_PROPERTY = "TIMESTAMP_FORMAT";
}
