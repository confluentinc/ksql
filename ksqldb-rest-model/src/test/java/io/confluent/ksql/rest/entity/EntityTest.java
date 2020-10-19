/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.test.util.ClassFinder;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EntityTest {
  private static final Set<Class<?>> BLACK_LIST = ImmutableSet.<Class<?>>builder()
      .add(KsqlMediaType.class)
      .build();

  private final Class<?> entityClass;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Class<?>> data() {
    return ClassFinder.getClasses(FunctionInfo.class.getPackage().getName()).stream()
        .filter(type -> !type.isEnum())
        .filter(type -> !Modifier.isAbstract(type.getModifiers()))
        .filter(type -> !BLACK_LIST.contains(type))
        .collect(Collectors.toList());
  }

  public EntityTest(final Class<?> entityClass) {
    this.entityClass = entityClass;
  }

  @Test
  public void shouldBeAttributedWithIgnoreUnknownProperties() {
    final JsonIgnoreProperties annotation = entityClass.getAnnotation(JsonIgnoreProperties.class);
    assertThat(entityClass + ": @JsonIgnoreProperties annotation missing", annotation, is(notNullValue()));
    assertThat(entityClass + ": not ignoring unknown properties", annotation.ignoreUnknown(), is(true));
  }
}
