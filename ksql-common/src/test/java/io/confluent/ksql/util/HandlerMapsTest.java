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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap0;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap1;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap2;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMapR2;
import io.confluent.ksql.util.HandlerMaps.Handler0;
import io.confluent.ksql.util.HandlerMaps.Handler1;
import io.confluent.ksql.util.HandlerMaps.Handler2;
import io.confluent.ksql.util.HandlerMaps.HandlerR0;
import io.confluent.ksql.util.HandlerMaps.HandlerR1;
import io.confluent.ksql.util.HandlerMaps.HandlerR2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class HandlerMapsTest {

  @SuppressWarnings("unused") // This field is a compile time test.
  private static final ClassHandlerMap0<BaseType> STATIC_TEST_0 =
      HandlerMaps.forClass(BaseType.class)
          .put(BaseType.class, HandlerMapsTest::staticHandlerBase)  // <-- member function
          .put(LeafTypeB.class, HandlerMapsTest::staticHandlerBase) // <-- super type handler
          .put(LeafTypeC.class, HandlerMapsTest::staticHandlerC)    // <-- static function
          .put(LeafTypeD.class, HandlerD0::new)                     // <-- lambda
          .put(LeafTypeF.class, HandlerBase0::new)                  // <-- super type lambda
          .build();

  @SuppressWarnings("unused") // This field is a compile time test.
  private static final ClassHandlerMap1<BaseType, HandlerMapsTest> STATIC_TEST_1 =
      HandlerMaps.forClass(BaseType.class).withArgType(HandlerMapsTest.class)
          .put(BaseType.class, HandlerMapsTest::baseHandler1)       // <-- member function
          .put(LeafTypeA.class, HandlerMapsTest::leafAHandler1)     // <-- member function
          .put(LeafTypeB.class, HandlerMapsTest::baseHandler1)      // <-- super type handler
          .put(LeafTypeC.class, HandlerMapsTest::staticHandlerC)    // <-- static function
          .put0(LeafTypeD.class, HandlerD0::new)                    // <-- no-arg lambda
          .put(LeafTypeE.class, HandlerE1::new)                     // <-- one-arg lambda
          .put(LeafTypeF.class, HandlerBase1::new)                  // <-- super type lambda
          .build();

  @SuppressWarnings("unused") // This field is a compile time test.
  private static final ClassHandlerMap2<BaseType, HandlerMapsTest, String> STATIC_TEST_2 =
      HandlerMaps.forClass(BaseType.class).withArgTypes(HandlerMapsTest.class, String.class)
          .put(BaseType.class, HandlerMapsTest::baseHandler2)       // <-- member function
          .put(LeafTypeA.class, HandlerMapsTest::leafAHandler1)     // <-- one-arg function
          .put(LeafTypeB.class, HandlerMapsTest::baseHandler2)      // <-- super type handler
          .put(LeafTypeC.class, HandlerMapsTest::staticHandlerC)    // <-- static function
          .put0(LeafTypeD.class, HandlerD0::new)                    // <-- no-arg lambda
          .put1(LeafTypeE.class, HandlerE1::new)                    // <-- one-arg lambda
          .put(LeafTypeF.class, HandlerF2::new)                     // <-- two-arg lambda
          .put(LeafTypeG.class, HandlerBase2::new)                  // <-- super type lambda
          .build();

  @SuppressWarnings("unused") // This field is a compile time test.
  private static final ClassHandlerMapR2<BaseType, TwoArgsWithReturnValue, String, Number>
      STATIC_TEST_R2 = HandlerMaps
      .forClass(BaseType.class)
      .withArgTypes(TwoArgsWithReturnValue.class, String.class)
      .withReturnType(Number.class)
      .put(BaseType.class, TwoArgsWithReturnValue::baseHandler2)       // <-- member function
      .put(LeafTypeA.class, TwoArgsWithReturnValue::leafAHandler1)     // <-- one-arg function
      .put(LeafTypeB.class, TwoArgsWithReturnValue::baseHandler2)      // <-- super type handler
      .put(LeafTypeC.class, TwoArgsWithReturnValue::staticHandlerC)    // <-- static function
      .put0(LeafTypeD.class, HandlerRD0::new)                    // <-- no-arg lambda
      .put1(LeafTypeE.class, HandlerRE1::new)                    // <-- one-arg lambda
      .put(LeafTypeF.class, HandlerRF2::new)                     // <-- two-arg lambda
      .put(LeafTypeG.class, HandlerBaseR2::new)                  // <-- super type lambda
      .build();

  private static final BaseType BASE = new BaseType();
  private static final LeafTypeA LEAF_A = new LeafTypeA();

  @Mock(name = "0_1")
  private Handler0<BaseType> handler0_1;
  @Mock(name = "0_2")
  private Handler0<BaseType> handler0_2;
  @Mock(name = "0_3")
  private Handler0<BaseType> handler0_3;

  @Mock(name = "1_1")
  private Handler1<BaseType, String> handler1_1;
  @Mock(name = "1_2")
  private Handler1<BaseType, String> handler1_2;
  @Mock(name = "1_3")
  private Handler1<BaseType, Object> handler1_3;

  @Mock(name = "2_1")
  private Handler2<BaseType, String, Integer> handler2_1;
  @Mock(name = "2_2")
  private Handler2<BaseType, String, Number> handler2_2;
  @Mock(name = "2_3")
  private Handler2<BaseType, String, Object> handler2_3;

  @Mock(name = "R2_1")
  private HandlerR2<BaseType, String, Integer, Long> handlerR2_1;
  @Mock(name = "R2_2")
  private HandlerR2<BaseType, String, Number, Double> handlerR2_2;
  @Mock(name = "R2_3")
  private HandlerR2<BaseType, String, Object, Number> handlerR2_3;

  private ClassHandlerMap0<BaseType> handlerMap0;
  private ClassHandlerMap1<BaseType, String> handlerMap1;
  private ClassHandlerMap2<BaseType, String, Integer> handlerMap2;
  private ClassHandlerMapR2<BaseType, String, Integer, Number> handlerMapR2;

  @Before
  public void setUp() {
    handlerMap0 = HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, handler0_1)
        .put(BaseType.class, handler0_2)
        .build();

    handlerMap1 = HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, handler1_1)
        .put(BaseType.class, handler1_2)
        .build();

    handlerMap2 = HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, handler2_1)
        .put(BaseType.class, handler2_2)
        .build();

    handlerMapR2 = HandlerMaps
        .forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, handlerR2_1)
        .put(BaseType.class, handlerR2_2)
        .build();

    when(handlerR2_1.handle(any(), any(), any())).thenReturn(1L);
    when(handlerR2_2.handle(any(), any(), any())).thenReturn(2.2d);
  }

  @Test
  public void shouldGetHandlerByType0() {
    // When:
    handlerMap0.get(LeafTypeA.class).handle(LEAF_A);
    handlerMap0.get(BaseType.class).handle(BASE);

    // Then:
    verify(handler0_1).handle(LEAF_A);
    verify(handler0_2).handle(BASE);
  }

  @Test
  public void shouldGetHandlerByType1() {
    // When:
    handlerMap1.get(LeafTypeA.class).handle("a", LEAF_A);
    handlerMap1.get(BaseType.class).handle("b", BASE);

    // Then:
    verify(handler1_1).handle("a", LEAF_A);
    verify(handler1_2).handle("b", BASE);
  }

  @Test
  public void shouldGetHandlerByType2() {
    // When:
    handlerMap2.get(LeafTypeA.class).handle("a", 1, LEAF_A);
    handlerMap2.get(BaseType.class).handle("b", 2, BASE);

    // Then:
    verify(handler2_1).handle("a", 1, LEAF_A);
    verify(handler2_2).handle("b", 2, BASE);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldGetHandlerByTypeR2() {
    // When:
    final Number r1 = handlerMapR2.get(LeafTypeA.class).handle("a", 1, LEAF_A);
    final Number r2 = handlerMapR2.get(BaseType.class).handle("b", 2, BASE);

    // Then:
    verify(handlerR2_1).handle("a", 1, LEAF_A);
    verify(handlerR2_2).handle("b", 2, BASE);

    assertThat(r1, is(1L));
    assertThat(r2, is(2.2d));
  }

  @Test
  public void shouldReturnNullIfTypeNotFound0() {
    assertThat(handlerMap0.get(MissingType.class), is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfTypeNotFound1() {
    assertThat(handlerMap1.get(MissingType.class), is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfTypeNotFound2() {
    assertThat(handlerMap2.get(MissingType.class), is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfTypeNotFoundR2() {
    assertThat(handlerMapR2.get(MissingType.class), is(nullValue()));
  }

  @Test
  public void shouldReturnDefaultIfTypeNotFound0() {
    assertThat(handlerMap0.getOrDefault(MissingType.class, handler0_3),
        is(sameInstance(handler0_3)));
  }

  @Test
  public void shouldReturnDefaultIfTypeNotFound1() {
    assertThat(handlerMap1.getOrDefault(MissingType.class, handler1_3),
        is(sameInstance(handler1_3)));
  }

  @Test
  public void shouldReturnDefaultIfTypeNotFound2() {
    assertThat(handlerMap2.getOrDefault(MissingType.class, handler2_3),
        is(sameInstance(handler2_3)));
  }

  @Test
  public void shouldReturnDefaultIfTypeNotFoundR2() {
    assertThat(handlerMapR2.getOrDefault(MissingType.class, handlerR2_3),
        is(sameInstance(handlerR2_3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateKey0() {
    HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, handler0_1)
        .put(LeafTypeA.class, handler0_2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateKey1() {
    HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, handler1_1)
        .put(LeafTypeA.class, handler1_2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateKey2() {
    HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, handler2_1)
        .put(LeafTypeA.class, handler2_2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateKeyR2() {
    HandlerMaps.forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, handlerR2_1)
        .put(LeafTypeA.class, handlerR2_2);
  }

  @Test
  public void shouldNotThrowOnDuplicateHandler0() {
    HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, handler0_1)
        .put(LeafTypeB.class, handler0_1);
  }

  @Test
  public void shouldNotThrowOnDuplicateHandler1() {
    HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, handler1_1)
        .put(LeafTypeB.class, handler1_1);
  }

  @Test
  public void shouldNotThrowOnDuplicateHandler2() {
    HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, handler2_1)
        .put(LeafTypeB.class, handler2_1);
  }

  @Test
  public void shouldNotThrowOnDuplicateHandlerR2() {
    HandlerMaps.forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, handlerR2_1)
        .put(LeafTypeB.class, handlerR2_1);
  }

  @Test(expected = ClassCastException.class)
  public void shouldThrowIfHandlerPassedWrongSubType0() {
    handlerMap0.get(LeafTypeA.class).handle(BASE);
  }

  @Test(expected = ClassCastException.class)
  public void shouldThrowIfHandlerPassedWrongSubType1() {
    handlerMap1.get(LeafTypeA.class).handle("a", BASE);
  }

  @Test(expected = ClassCastException.class)
  public void shouldThrowIfHandlerPassedWrongSubType2() {
    handlerMap2.get(LeafTypeA.class).handle("a", 1, BASE);
  }

  @Test(expected = ClassCastException.class)
  public void shouldThrowIfHandlerPassedWrongSubTypeR2() {
    handlerMap2.get(LeafTypeA.class).handle("a", 1, BASE);
  }

  @Test
  public void shouldWorkWithSuppliers0() {
    // Given:
    handlerMap0 = HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, () -> handler0_1)
        .build();

    // When:
    handlerMap0.get(LeafTypeA.class).handle(LEAF_A);

    // Then:
    verify(handler0_1).handle(LEAF_A);
  }

  @Test
  public void shouldWorkWithSuppliers1() {
    // Given:
    handlerMap1 = HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, () -> handler1_1)
        .build();

    // When:
    handlerMap1.get(LeafTypeA.class).handle("A", LEAF_A);

    // Then:
    verify(handler1_1).handle("A", LEAF_A);
  }

  @Test
  public void shouldWorkWithSuppliers2() {
    // Given:
    handlerMap2 = HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, () -> handler2_1)
        .build();

    // When:
    handlerMap2.get(LeafTypeA.class).handle("A", 2, LEAF_A);

    // Then:
    verify(handler2_1).handle("A", 2, LEAF_A);
  }

  @Test
  public void shouldWorkWithSuppliersR2() {
    // Given:
    handlerMapR2 = HandlerMaps.forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, () -> handlerR2_1)
        .build();

    // When:
    handlerMapR2.get(LeafTypeA.class).handle("A", 2, LEAF_A);

    // Then:
    verify(handlerR2_1).handle("A", 2, LEAF_A);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierThrows0() {
    HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, () -> {
          throw new RuntimeException("Boom");
        })
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierThrows1() {
    HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, () -> {
          throw new RuntimeException("Boom");
        })
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierThrows2() {
    HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, () -> {
          throw new RuntimeException("Boom");
        })
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierThrowsR2() {
    HandlerMaps.forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, () -> {
          throw new RuntimeException("Boom");
        })
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierReturnsNullHandler0() {
    HandlerMaps.forClass(BaseType.class)
        .put(LeafTypeA.class, () -> null)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierReturnsNullHandler1() {
    HandlerMaps.forClass(BaseType.class).withArgType(String.class)
        .put(LeafTypeA.class, () -> null)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierReturnsNullHandler2() {
    HandlerMaps.forClass(BaseType.class).withArgTypes(String.class, Integer.class)
        .put(LeafTypeA.class, () -> null)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfHandlerSupplierReturnsNullHandlerR2() {
    HandlerMaps.forClass(BaseType.class)
        .withArgTypes(String.class, Integer.class)
        .withReturnType(Number.class)
        .put(LeafTypeA.class, () -> null)
        .build();
  }

  @SuppressFBWarnings({"DLS_DEAD_LOCAL_STORE", "UC_USELESS_VOID_METHOD"}) // Compile-time check
  @SuppressWarnings("unused")
  @Test
  public void shouldReturnedTypedHandler() {
    // When:
    final Handler0<LeafTypeA> typedHandler0 = handlerMap0.getTyped(LeafTypeA.class);
    final Handler1<LeafTypeA, String> typedHandler1 = handlerMap1.getTyped(LeafTypeA.class);
    final Handler2<LeafTypeA, String, Integer> typedHandler2 =
        handlerMap2.getTyped(LeafTypeA.class);
    final HandlerR2<LeafTypeA, String, Integer, Number> typedHandlerR2 =
        handlerMapR2.getTyped(LeafTypeA.class);

    // Then:
    // Return value is typed to accept derived type, not base, and no cast exception was thrown.
  }

  @Test
  public void shouldGetKeySetFrom0() {
    assertThat(STATIC_TEST_0.keySet(), containsInAnyOrder(
        BaseType.class,
        LeafTypeB.class,
        LeafTypeC.class,
        LeafTypeD.class,
        LeafTypeF.class
    ));
  }

  @Test
  public void shouldGetKeySetFrom1() {
    assertThat(STATIC_TEST_1.keySet(), containsInAnyOrder(
        BaseType.class,
        LeafTypeA.class,
        LeafTypeB.class,
        LeafTypeC.class,
        LeafTypeD.class,
        LeafTypeE.class,
        LeafTypeF.class
    ));
  }

  @Test
  public void shouldGetKeySetFrom2() {
    assertThat(STATIC_TEST_2.keySet(), containsInAnyOrder(
        BaseType.class,
        LeafTypeA.class,
        LeafTypeB.class,
        LeafTypeC.class,
        LeafTypeD.class,
        LeafTypeE.class,
        LeafTypeF.class,
        LeafTypeG.class
    ));
  }

  @Test
  public void shouldGetKeySetFromR2() {
    assertThat(STATIC_TEST_R2.keySet(), containsInAnyOrder(
        BaseType.class,
        LeafTypeA.class,
        LeafTypeB.class,
        LeafTypeC.class,
        LeafTypeD.class,
        LeafTypeE.class,
        LeafTypeF.class,
        LeafTypeG.class
    ));
  }

  @SuppressWarnings("unused")
  private void baseHandler1(final BaseType type) {
  }

  @SuppressWarnings("unused")
  private void baseHandler2(final String arg, final BaseType type) {
  }

  @SuppressWarnings("unused")
  private void leafAHandler1(final LeafTypeA type) {
  }

  @SuppressWarnings("unused")
  private void leafAHandler2(final LeafTypeA type, final String arg) {
  }

  @SuppressWarnings("unused")
  private static void staticHandlerBase(final BaseType type) {
  }

  @SuppressWarnings("unused")
  private static void staticHandlerC(final LeafTypeC type) {
  }

  private static class BaseType {

  }

  private static class LeafTypeA extends BaseType {

  }

  private static class LeafTypeB extends BaseType {

  }

  private static class LeafTypeC extends BaseType {

  }

  private static class LeafTypeD extends BaseType {

  }

  private static class LeafTypeE extends BaseType {

  }

  private static class LeafTypeF extends BaseType {

  }

  private static class LeafTypeG extends BaseType {

  }

  private static class MissingType extends BaseType {

  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private static final class TwoArgsWithReturnValue {

    private Integer baseHandler2(final String arg, final BaseType type) {
      return null;
    }

    private Number leafAHandler1(final LeafTypeA type) {
      return null;
    }

    private static Long staticHandlerC(final LeafTypeC type) {
      return null;
    }
  }


  @SuppressWarnings("unused")
  private static final class HandlerBase0 implements Handler0<BaseType> {

    @Override
    public void handle(final BaseType key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerBase1 implements Handler1<BaseType, HandlerMapsTest> {

    @Override
    public void handle(final HandlerMapsTest arg0, final BaseType key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerBase2 implements Handler2<BaseType, HandlerMapsTest, String> {

    @Override
    public void handle(final HandlerMapsTest arg0, final String arg1, final BaseType key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerD0 implements Handler0<LeafTypeD> {

    @Override
    public void handle(final LeafTypeD key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerE1 implements Handler1<LeafTypeE, HandlerMapsTest> {

    @Override
    public void handle(final HandlerMapsTest arg0, final LeafTypeE key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerF2 implements Handler2<LeafTypeF, HandlerMapsTest, String> {

    @Override
    public void handle(final HandlerMapsTest arg0, final String arg1, final LeafTypeF key) {

    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerRD0 implements HandlerR0<LeafTypeD, Number> {

    @Override
    public Number handle(final LeafTypeD key) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerRE1 implements
      HandlerR1<LeafTypeE, TwoArgsWithReturnValue, Long> {

    @Override
    public Long handle(final TwoArgsWithReturnValue arg0, final LeafTypeE key) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerRF2
      implements HandlerR2<LeafTypeF, TwoArgsWithReturnValue, String, Integer> {

    @Override
    public Integer handle(final TwoArgsWithReturnValue arg0, final String arg1,
        final LeafTypeF key) {
      return null;
    }
  }

  @SuppressWarnings("unused")
  private static final class HandlerBaseR2
      implements HandlerR2<BaseType, TwoArgsWithReturnValue, String, Double> {

    @Override
    public Double handle(final TwoArgsWithReturnValue arg0, final String arg1, final BaseType key) {
      return null;
    }
  }
}