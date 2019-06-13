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

package io.confluent.ksql.test.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ValueSpec {
  private final Object spec;

  public ValueSpec(final Object spec) {
    this.spec = spec;
  }

  private static boolean compare(final Object o1, final Object o2, final String path) {
    if (o1 == null && o2 == null) {
      return true;
    } else if (o1 == null || o2 == null) {
      fail("null mismatch at " + path);
    }

    if (o1 instanceof Map) {
      assertThat("type mismatch at " + path, o2, instanceOf(Map.class));
      assertThat("keyset mismatch at " + path,
          ((Map) o1).keySet(),
          equalTo(((Map)o2).keySet()));
      for (final Object k : ((Map) o1).keySet()) {
        compare(((Map) o1).get(k), ((Map) o2).get(k), path + "." + k);
      }
      return true;
    } else if (o1 instanceof List) {
      assertThat("type mismatch at " + path, o2, instanceOf(List.class));
      assertThat("list size mismatch at " + path,
          ((List) o1).size(),
          equalTo(((List)o2).size()));
      for (int i = 0; i < ((List) o1).size(); i++) {
        compare(((List) o1).get(i), ((List) o2).get(i), path + "." + i);
      }
      return true;
    } else if (o1 instanceof BigDecimal) {
      assertThat("type mismatch at " + path, o2, instanceOf(String.class));
      assertThat("big decimal mismatch at " + path, o1, is(new BigDecimal((String) o2)));
      return true;
    } else {
      assertThat("type mismatch at " + path, o1.getClass(), equalTo(o2.getClass()));
      assertThat("mismatch at path " + path, o1, equalTo(o2));
      return true;
    }
  }

  public Object getSpec() {
    return spec;
  }

  @SuppressFBWarnings("HE_EQUALS_USE_HASHCODE")
  // Hack to make work with OutputVerifier.
  @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
  @Override
  public boolean equals(final Object o) {
    return compare(spec, o, "VALUE-SPEC");
  }

  @Override
  public int hashCode() {
    return Objects.hash(spec);
  }

  @Override
  public String toString() {
    return Objects.toString(spec);
  }
}