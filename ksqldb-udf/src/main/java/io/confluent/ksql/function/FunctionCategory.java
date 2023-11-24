/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function;

public final class FunctionCategory {

  private FunctionCategory() {
    // extra code for style adherence
  }

  public static final String CONDITIONAL = "CONDITIONAL";
  public static final String MATHEMATICAL = "MATHEMATICAL";
  public static final String STRING = "STRING";
  public static final String REGULAR_EXPRESSION = "REGULAR EXPRESSION";
  public static final String JSON = "JSON";
  public static final String DATE_TIME = "DATE / TIME";
  public static final String ARRAY = "ARRAY";
  public static final String MAP = "MAP";
  public static final String URL = "URL";
  public static final String OTHER = "OTHER";
  public static final String AGGREGATE = "AGGREGATE";
  public static final String TABLE = "TABLE";
  public static final String LAMBDA = "LAMBDA";
  public static final String CONVERSIONS = "CONVERSIONS";
}
