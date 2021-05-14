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

package io.confluent.ksql.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import io.confluent.ksql.services.DefaultServiceContext.MemoizedSupplier;
import org.junit.Test;

public class MemoizedSupplierTest {

  @Test
  public void shouldReturnIsInitializedAfterConstructor() {
    // Given
    final MemoizedSupplier<String> memoizedSupplier = new MemoizedSupplier<>(() -> "");

    // When
    memoizedSupplier.get();

    // Then
    assertThat(memoizedSupplier.isInitialized(), is(true));
  }

  @Test
  public void shouldReturnNotInitializedAfterConstructor() {
    // Given
    final MemoizedSupplier<String> memoizedSupplier = new MemoizedSupplier<>(() -> "");

    // Then
    assertThat(memoizedSupplier.isInitialized(), is(false));
  }

  @Test
  public void shouldReturnSameInstance() {
    // Given
    final MemoizedSupplier<String> memoizedSupplier = new MemoizedSupplier<>(() -> "");

    // When
    final String s1 = memoizedSupplier.get();
    final String s2 = memoizedSupplier.get();

    // Then
    assertThat(s1, sameInstance(s2));
  }
}
