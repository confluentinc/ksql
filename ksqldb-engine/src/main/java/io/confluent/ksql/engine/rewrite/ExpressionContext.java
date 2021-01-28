/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.engine.rewrite;

import io.confluent.ksql.util.KsqlException;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class ExpressionContext {
  private final List<String> lambdaArguments;
  
  public ExpressionContext(final List<String> lambdaArguments) {
    this.lambdaArguments = Objects.requireNonNull(lambdaArguments, "lambdaArguments");
  }
  
  public void addLambdaArguments(final List<String> newArguments) {
    final int previousLambdaArgumentsLength = lambdaArguments.size();
    lambdaArguments.addAll(newArguments);
    if (new HashSet<>(lambdaArguments).size()
        < previousLambdaArgumentsLength + newArguments.size()) {
      throw new KsqlException("Duplicate lambda arguments are not allowed.");
    }
  }

  public List<String> getLambdaArguments() {
    return lambdaArguments;
  }
}
