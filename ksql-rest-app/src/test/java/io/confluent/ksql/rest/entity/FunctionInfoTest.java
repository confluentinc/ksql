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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

@SuppressWarnings("SameParameterValue")
public class FunctionInfoTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final FunctionInfo FUNC_INFO = new FunctionInfo(
      ImmutableList.of(
          new ArgumentInfo("arg0", "VARCHAR", "first arg"),
          new ArgumentInfo("arg1", "INT", "last arg")
      ),
      "DOUBLE",
      "Test Func"
  );

  @Test
  public void shouldDeserializeV2AsV1() throws Exception {
    // Given:
    final String json = serialize(FUNC_INFO);

    // When:
    final FunctionInfoV1 result = deserialize(json, FunctionInfoV1.class);

    // Then:
    final FunctionInfoV1 expected = new FunctionInfoV1(
      ImmutableList.of("VARCHAR", "INT"), "DOUBLE", "Test Func"
    );
    assertThat(result, is(expected));
  }

  private static String serialize(final FunctionInfo funcInfo) throws Exception {
    return OBJECT_MAPPER.writeValueAsString(funcInfo);
  }

  private static <T> T deserialize(final String json, final Class<T> type) throws Exception {
    return OBJECT_MAPPER.readValue(json, type);
  }

  @SuppressWarnings("unused") // Invoked via reflection
  @JsonIgnoreProperties(ignoreUnknown = true) // Not actually present on V1, but required to prove JSON compatibility
  private static final class FunctionInfoV1 {

    private final List<String> argumentTypes;
    private final String returnType;
    private final String description;

    @SuppressWarnings("WeakerAccess") // Invoked via reflection.
    @JsonCreator
    public FunctionInfoV1(
        @JsonProperty("argumentTypes") final List<String> argumentTypes,
        @JsonProperty("returnType") final String returnType,
        @JsonProperty("description") final String description
    ) {
      this.argumentTypes = Objects.requireNonNull(argumentTypes, "argumentTypes can't be null");
      this.returnType = Objects.requireNonNull(returnType, "returnType can't be null");
      this.description = Objects.requireNonNull(description, "description can't be null");
    }

    public List<String> getArgumentTypes() {
      return Collections.unmodifiableList(argumentTypes);
    }

    public String getReturnType() {
      return returnType;
    }

    public String getDescription() {
      return description;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FunctionInfoV1 that = (FunctionInfoV1) o;
      return Objects.equals(argumentTypes, that.argumentTypes)
             && Objects.equals(returnType, that.returnType)
             && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(argumentTypes, returnType, description);
    }

    @Override
    public String toString() {
      return "FunctionInfo{"
             + "argumentTypes=" + argumentTypes
             + ", returnType='" + returnType + '\''
             + ", description='" + description + '\''
             + '}';
    }
  }
}