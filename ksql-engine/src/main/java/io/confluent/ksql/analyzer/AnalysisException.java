/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.analyzer;

import io.confluent.ksql.util.KsqlException;

/**
 * @author andy
 * created 27/04/2018
 */
public class AnalysisException extends KsqlException {
  public AnalysisException(final String message) {
    super(message);
  }

  public AnalysisException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
