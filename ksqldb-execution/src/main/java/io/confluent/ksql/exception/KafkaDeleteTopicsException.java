/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.exception;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.Pair;
import java.util.List;

public class KafkaDeleteTopicsException extends KafkaTopicClientException {

  private final List<Pair<String, Throwable>> exceptionList;

  public KafkaDeleteTopicsException(
      final String message,
      final List<Pair<String, Throwable>> failList
  ) {
    super(message);
    exceptionList = ImmutableList.copyOf(failList);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "exceptionList is ImmutableList")
  public final List<Pair<String, Throwable>> getExceptionList() {
    return exceptionList;
  }

}
