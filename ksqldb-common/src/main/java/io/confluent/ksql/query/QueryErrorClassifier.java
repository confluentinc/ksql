/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import io.confluent.ksql.query.QueryError.Type;

public interface QueryErrorClassifier {

  QueryErrorClassifier DEFAULT_CLASSIFIER = e -> Type.UNKNOWN;

  /**
   * Classifies this error with a specific type
   *
   * @param e the error
   * @return the classification
   */
  Type classify(Throwable e);

  /**
   * Chains two classifiers so that:
   * <ol>
   *   <li>If they classify the error in the same way, return that classification</li>
   *   <li>If they classify the error differently and exactly one is classified as
   *       {@link Type#UNKNOWN}, return the other classification.</li>
   *   <li>If they classify the error differently and neither is classified as
   *       {@link Type#UNKNOWN}, return {@link Type#UNKNOWN}</li>
   * </ol>
   *
   * @param other the other classifier to chain
   * @return a {@code QueryErrorClassifier} that chains both
   */
  default QueryErrorClassifier and(QueryErrorClassifier other) {
    return e -> {
      final Type first = this.classify(e);
      final Type second = other.classify(e);

      if (first == second) {
        return first;
      } else if (first == Type.UNKNOWN) {
        return second;
      } else if (second == Type.UNKNOWN) {
        return first;
      } else {
        return Type.UNKNOWN;
      }
    };
  }

}
