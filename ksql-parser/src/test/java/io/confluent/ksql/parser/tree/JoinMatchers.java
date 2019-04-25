/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.Matchers.is;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class JoinMatchers {

  private JoinMatchers() {
  }

  public static Matcher<Join> hasLeft(final Relation relationship) {
    return hasLeft(is(relationship));
  }

  public static Matcher<Join> hasLeft(final Matcher<? super Relation> relationship) {
    return new FeatureMatcher<Join, Relation>
        (relationship, "left relationship", "left") {
      @Override
      protected Relation featureValueOf(final Join actual) {
        return actual.getLeft();
      }
    };
  }

  public static Matcher<Join> hasRight(final Relation relationship) {
    return hasRight(is(relationship));
  }

  public static Matcher<Join> hasRight(final Matcher<? super Relation> relationship) {
    return new FeatureMatcher<Join, Relation>
        (relationship, "right relationship", "right") {
      @Override
      protected Relation featureValueOf(final Join actual) {
        return actual.getRight();
      }
    };
  }
}