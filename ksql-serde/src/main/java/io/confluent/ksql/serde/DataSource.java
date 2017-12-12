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

package io.confluent.ksql.serde;

public interface DataSource {

  enum DataSourceType {
    KTOPIC("TOPIC"),
    KSTREAM("STREAM"),
    KTABLE("TABLE");

    private final String kqlType;

    DataSourceType(String ksqlType) {
      this.kqlType = ksqlType;
    }

    public String getKqlType() {
      return kqlType;
    }
  }

  enum DataSourceSerDe { JSON, AVRO, DELIMITED }

  String AVRO_SERDE_NAME = "AVRO";
  String JSON_SERDE_NAME = "JSON";
  String DELIMITED_SERDE_NAME = "DELIMITED";

  String getName();

  DataSourceType getDataSourceType();

}
