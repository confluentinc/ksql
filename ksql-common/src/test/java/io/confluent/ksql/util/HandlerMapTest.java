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

package io.confluent.ksql.util;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.util.HandlerMap.Handler;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class HandlerMapTest {

  @SuppressWarnings("unused") // This field is a compile time test.
  private static final HandlerMap<BaseType, HandlerMapTest> STATIC_TEST =
      HandlerMap.<BaseType, HandlerMapTest>builder()
          .put(BaseType.class, HandlerMapTest::baseHandler)
          .put(LeafTypeA.class, HandlerMapTest::leafAHandler)
          .put(LeafTypeB.class, HandlerMapTest::anotherBaseHandler)
          .build();

  private static final BaseType BASE = new BaseType();
  private static final LeafTypeA LEAF_A = new LeafTypeA();
  private static final LeafTypeB LEAF_B = new LeafTypeB();

  @Mock
  private Handler<BaseType, String> handler1;
  @Mock
  private Handler<BaseType, String> handler2;
  @Mock
  private Handler<BaseType, String> handler3;
  @Mock
  private Handler<BaseType, String> handler4;

  private HandlerMap<BaseType, String> handlerMap;

  @Before
  public void setUp() {
    handlerMap = HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, handler1)
        .put(LeafTypeB.class, handler2)
        .put(BaseType.class, handler3)
        .build();
  }

  @Test
  public void shouldGetHandlerByType() {
    // Given:
    handler1.handle("a", LEAF_A);
    expectLastCall();

    handler2.handle("b", LEAF_B);
    expectLastCall();

    handler3.handle("c", BASE);

    replay(handler1, handler2, handler3);

    // When:
    handlerMap.get(LeafTypeA.class).handle("a", LEAF_A);
    handlerMap.get(LeafTypeB.class).handle("b", LEAF_B);
    handlerMap.get(BaseType.class).handle("c", BASE);

    // Then:
    verify(handler1, handler2, handler3);
  }

  @Test
  public void shouldReturnNullIfTypeNotFound() {
    assertThat(handlerMap.get(MissingType.class), is(nullValue()));
  }

  @Test
  public void shouldReturnDefaultIfTypeNotFound() {
    assertThat(handlerMap.getOrDefault(MissingType.class, handler4), is(handler4));
  }

  @SuppressWarnings("unused") // Compile-time test
  @Test
  public void shouldReturnedTypedHandler() {
    // When:
    final Handler<LeafTypeA, String> typedHandler = handlerMap.getTyped(LeafTypeA.class);

    // Then:
    // Return value is typed to accept derived type, not base.
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateKey() {
    HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, handler1)
        .put(LeafTypeA.class, handler2);
  }

  @Test
  public void shouldNotThrowOnDuplicateHandler() {
    HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, handler1)
        .put(LeafTypeB.class, handler1);
  }

  @Test(expected = ClassCastException.class)
  public void shouldThrowIfHandlerPassedWrongSubType() {
    // Given:
    final Handler<BaseType, String> handler = handlerMap.get(LeafTypeA.class);

    // When:
    handler.handle("a", BASE);
  }

  @Test
  public void shouldWorkWithSuppliers() {
    // Given:
    handlerMap = HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, () -> handler1)
        .build();

    handler1.handle("a", LEAF_A);
    expectLastCall();

    replay(handler1);

    // When:
    handlerMap.get(LeafTypeA.class).handle("a", LEAF_A);

    // Then:
    verify(handler1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierThrows() {
    HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, () -> {
          throw new RuntimeException("Boom");
        })
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierReturnsNullHandler() {
    HandlerMap.<BaseType, String>builder()
        .put(LeafTypeA.class, () -> null)
        .build();
  }

  @SuppressWarnings("unused")
  private void baseHandler(final BaseType type) {
  }

  @SuppressWarnings("unused")
  private void leafAHandler(final LeafTypeA type) {
  }

  @SuppressWarnings("unused")
  private void anotherBaseHandler(final BaseType type) {
  }

  private static class BaseType {

  }

  private static class LeafTypeA extends BaseType {

  }

  private static class LeafTypeB extends BaseType {

  }

  private static class MissingType extends BaseType {

  }
}