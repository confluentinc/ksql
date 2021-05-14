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

package io.confluent.ksql.test.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.easymock.TestSubject;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("UUF_UNUSED_FIELD")
@SuppressWarnings({"FieldMayBeStatic", "unused", "OptionalAssignedToNull"})
public class ImmutableTesterTest {

  private ImmutableTester tester;

  @Before
  public void setUp() {
    tester = new ImmutableTester()
        .withKnownImmutableType(ImmutableTesterTest.class);
  }

  @Test(expected = AssertionError.class)
  public void shouldFailOnImmutableCollectionOfMutableType() {
    // Given:
    @Immutable
    class TestSubject {
      final ImmutableList<MutableType> f = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailOnClassOfMutableType() {
    // Given:
    @Immutable
    class TestSubject {
      final Class<MutableType> f = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailOnGuavaImmutableCollectionTypesOfImmutableObjects() {
    // Given:
    @Immutable
    class TestSubject {
      final ImmutableList<String> f0 = null;
      final ImmutableSet<String > f1 = null;
      final ImmutableMap<String, String> f2 = null;
      final ImmutableMultimap<String, String> f3 = null;
      final ImmutableListMultimap<String, String> f4 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailOnKnownImmutableJdkTypes() {
    // Given:
    @Immutable
    class TestSubject {
      final ThreadLocal<MutableType> f0 = null;
      final Duration f1 = null;
      final Character f2 = null;
      final char f3 = 'i';
      final Void f4 = null;
      final Byte f5 = null;
      final Boolean f6 = null;
      final boolean f7 = true;
      final Integer f8 = null;
      final int f9 = 9;
      final Long f10 = null;
      final long f11 = 11;
      final Float f12 = null;
      final float f13 = 13;
      final Double f14 = null;
      final double f15 = 15;
      final String f16 = null;
      final Optional<String> f17 = null;
      final OptionalInt f18 = null;
      final OptionalLong f19 = null;
      final OptionalDouble f20 = null;
      final URI f21 = null;
      final URL f22 = null;
      final Pattern f23 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test(expected = AssertionError.class)
  public void shouldFailOnOptionalOfMutableType() {
    // Given:
    @Immutable
    class TestSubject {
      final Optional<MutableType> f = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailIfImplementingNonImmutableInterface() {
    // Given:
    @Immutable
    class TestSubject {
      final ImplementsNonImmutableInterface f = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailOnGuavaImmutableTypes() {
    // Given:
    @Immutable
    class TestSubject {
      final Range<Integer> f0 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test(expected = AssertionError.class)
  public void shouldFailOnWildcardType() {
    // Given:
    @Immutable
    class TestSubject {
      final ImmutableList<?> f0 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test(expected = AssertionError.class)
  public void shouldFailOnWildcardTypeWithMutableUpperBound() {
    // Given:
    @Immutable
    class TestSubject {
      final ImmutableList<? extends MutableType> f0 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  @Test
  public void shouldNotFailOnWildcardTypeWithImmutableUpperBound() {
    // Given:
    @Immutable
    class TestSubject {
      final ImmutableList<? extends ImplementsNonImmutableInterface> f0 = null;
    }

    // When:
    tester.test(TestSubject.class);
  }

  private static class MutableType {
    private int i;
  }

  @Immutable
  private static final class ImplementsNonImmutableInterface implements Supplier<TestSubject> {
    @Override
    public TestSubject get() {
      return null;
    }
  }
}