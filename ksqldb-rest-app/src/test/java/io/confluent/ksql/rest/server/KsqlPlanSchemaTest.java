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

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.server.utils.KsqlPlanSchemaGenerator;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Ignore;
import org.junit.Test;

public class KsqlPlanSchemaTest {
  private static final Path BASE_PATH = Paths.get("src/test/resources");
  private static final Path SCHEMA_PATH = Paths.get("ksql-plan-schema/schema.json");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @SuppressWarnings("ConstantConditions")
  @Test
  public void shouldBuildSameSchemaForKsqlPlan() throws IOException {
    // When:
    final JsonNode jsonSchema = KsqlPlanSchemaGenerator.generate();

    // Then:
    final JsonNode expected = MAPPER.readTree(
        KsqlPlanSchemaTest.class.getClassLoader().getResource(SCHEMA_PATH.toString()));
    assertThat(
        "Detected a change to the schema of the KSQL physical plan. This is dangerous. "
            + "It means that KSQL may no longer be able to read and execute older plans. "
            + "If you're sure that your change is backwards-compatible, then please regenerate "
            + "the schema.json by running KsqlPlanSchemaTest.runMeToRegenerateSchemaFile().",
        jsonSchema,
        is(expected)
    );
  }

  @Ignore("Comment me out to regenerate the schema")
  @Test
  public void runMeToRegenerateSchemaFile() throws Exception {
    KsqlPlanSchemaGenerator.generateTo(BASE_PATH.resolve(SCHEMA_PATH));
  }

  @Test
  public void shouldNotCheckInThisClassWithTheAboveTestEnabled() throws Exception {
    final Ignore ignoreAnnotation = this.getClass()
        .getMethod("runMeToRegenerateSchemaFile")
        .getAnnotation(Ignore.class);

    assertThat(
        "Ensure you add back the @Ignore annotation above before committing your change.",
        ignoreAnnotation,
        is(notNullValue())
    );
  }
}
