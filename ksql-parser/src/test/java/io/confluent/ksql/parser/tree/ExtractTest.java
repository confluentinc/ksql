/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.parser.tree;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link Extract}.
 *
 * @see Extract
 */
public class ExtractTest {

  @Test
  public void testEqualsReturningTrue() {
    QualifiedNameReference qualifiedNameReference = new QualifiedNameReference(null);
    Extract.Field extract_Field = Extract.Field.MONTH;
    Extract extractOne = new Extract(qualifiedNameReference, extract_Field);
    Extract extractTwo = new Extract(qualifiedNameReference, extract_Field);

    assertTrue(extractOne.equals(extractTwo));
    assertTrue(extractOne.equals(extractOne));
  }

  @Test
  public void testEqualsWithNull() {
    NodeLocation nodeLocation = new NodeLocation((-5719), (-5719));
    SymbolReference symbolReference = new SymbolReference("(Q\"'Cq~-X/i[");
    IsNotNullPredicate isNotNullPredicate = new IsNotNullPredicate(nodeLocation, symbolReference);
    ArithmeticUnaryExpression arithmeticUnaryExpression = ArithmeticUnaryExpression.negative(isNotNullPredicate);
    Cast cast = new Cast(nodeLocation, arithmeticUnaryExpression, "(Q\"'Cq~-X/i[", false, false);
    Extract.Field extract_Field = Extract.Field.DAY_OF_WEEK;
    Extract extract = new Extract(cast, extract_Field);

    assertFalse(extract.equals(null));
  }

  @Test
  public void testEqualsReturningFalse() {
    NullLiteral nullLiteral = new NullLiteral();
    NullIfExpression nullIfExpression = new NullIfExpression(nullLiteral, nullLiteral);
    Extract.Field extract_Field = Extract.Field.DAY_OF_WEEK;
    Extract extractOne = new Extract(nullIfExpression, extract_Field);
    NodeLocation nodeLocation = new NodeLocation(1513, 1513);
    Extract extractTwo = new Extract(nodeLocation, nullLiteral, extract_Field);

    assertFalse(extractOne.equals(extractTwo));
    assertFalse(extractOne.equals(new Object()));
  }
}