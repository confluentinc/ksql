/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class JavaToConnectTest {

  @Test
  public void shouldGetBooleanSchemaForBooleanClass() {
    assertThat(JavaToConnect.getSchemaFromType(Boolean.class),
        equalTo(Schema.OPTIONAL_BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanPrimitiveClass() {
    assertThat(JavaToConnect.getSchemaFromType(boolean.class),
        equalTo(Schema.BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldGetIntSchemaForIntegerClass() {
    assertThat(JavaToConnect.getSchemaFromType(Integer.class),
        equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldGetIntegerSchemaForIntPrimitiveClass() {
    assertThat(JavaToConnect.getSchemaFromType(int.class),
        equalTo(Schema.INT32_SCHEMA));
  }

  @Test
  public void shouldGetLongSchemaForLongClass() {
    assertThat(JavaToConnect.getSchemaFromType(Long.class),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldGetLongSchemaForLongPrimitiveClass() {
    assertThat(JavaToConnect.getSchemaFromType(long.class),
        equalTo(Schema.INT64_SCHEMA));
  }

  @Test
  public void shouldGetFloatSchemaForDoubleClass() {
    assertThat(JavaToConnect.getSchemaFromType(Double.class),
        equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetFloatSchemaForDoublePrimitiveClass() {
    assertThat(JavaToConnect.getSchemaFromType(double.class),
        equalTo(Schema.FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetMapSchemaFromMapClass() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("mapType", Map.class)
        .getGenericParameterTypes()[0];
    final Schema schema = JavaToConnect.getSchemaFromType(type);
    assertThat(schema.type(), equalTo(Schema.Type.MAP));
    assertThat(schema.keySchema(), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(schema.valueSchema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldGetArraySchemaFromListClass() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("listType", List.class)
        .getGenericParameterTypes()[0];
    final Schema schema = JavaToConnect.getSchemaFromType(type);
    assertThat(schema.type(), equalTo(Schema.Type.ARRAY));
    assertThat(schema.valueSchema(), equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetStringSchemaFromStringClass() {
    assertThat(JavaToConnect.getSchemaFromType(String.class),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfClassDoesntMapToSchema() {
    JavaToConnect.getSchemaFromType(System.class);
  }

  @Test
  public void shouldDefaultToNoNameOnGetSchemaFromType() {
    assertThat(JavaToConnect.getSchemaFromType(Double.class).name(), is(nullValue()));
  }

  @Test
  public void shouldDefaultToNoDocOnGetSchemaFromType() {
    assertThat(JavaToConnect.getSchemaFromType(Double.class).doc(), is(nullValue()));
  }

  @Test
  public void shouldSetNameOnGetSchemaFromType() {
    assertThat(JavaToConnect.getSchemaFromType(Double.class, "name", "").name(), is("name"));
  }

  @Test
  public void shouldSetDocOnGetSchemaFromType() {
    assertThat(JavaToConnect.getSchemaFromType(Double.class, "", "doc").doc(), is("doc"));
  }

  @Test
  public void shouldGetGenericSchemaFromType() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericType").getGenericReturnType();

    // When:
    final Schema returnType = JavaToConnect.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(GenericsUtil.generic("T").build()));
  }

  @Test
  public void shouldGetGenericSchemaFromParameterizedType() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericMapType").getGenericReturnType();

    // When:
    final Schema returnType = JavaToConnect.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(GenericsUtil.map(Schema.OPTIONAL_STRING_SCHEMA, "T").build()));
  }

  // following methods not invoked but used to test conversion from type -> schema
  @SuppressWarnings({"unused", "WeakerAccess"})
  public <T> T genericType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess"})
  public <T> List<T> genericArrayType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess"})
  public <T> Map<String, T> genericMapType() {
    return null;
  }

  @SuppressWarnings("unused")
  private void mapType(final Map<String, Integer> map) {
  }

  @SuppressWarnings("unused")
  private void listType(final List<Double> list) {
  }
}