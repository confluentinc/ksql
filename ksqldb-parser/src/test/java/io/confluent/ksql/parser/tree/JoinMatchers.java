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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.CoreMatchers;
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

  public static Matcher<Join> hasRights(final Relation... relationship) {
    return hasRightMatchers(
        Arrays.stream(relationship).map(CoreMatchers::is).collect(Collectors.toList())
    );
  }

  public static Matcher<Join> hasRightMatchers(final List<Matcher<? super Relation>> relationships) {
    final List<Matcher<? super Join>> rights = new ArrayList<>();

    for (int i = 0; i < relationships.size(); i++) {
      final int j = i;
      final Matcher<? super Relation> relationship = relationships.get(i);
      rights.add(
          new FeatureMatcher<Join, Relation> (relationship, "right relationship", "right") {
            @Override
            protected Relation featureValueOf(final Join actual) {
              return actual.getRights().get(j).getRelation();
            }
          }
      );
    }

    return allOf(rights);
  }
}
