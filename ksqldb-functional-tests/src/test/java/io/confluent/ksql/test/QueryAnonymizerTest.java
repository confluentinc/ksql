/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.confluent.ksql.engine.rewrite.QueryAnonymizer;
import io.confluent.ksql.test.QueryTranslationTest.QttTestFile;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.util.GrammarTokenExporter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class QueryAnonymizerTest {
  private static final Path QUERIES_TO_ANONYMIZE_PATH =
      Paths.get("src/test/java/io/confluent/ksql/test/QueriesToAnonymizeTest.txt");
  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private final QueryAnonymizer anon = new QueryAnonymizer();

  @Test
  public void queriesAreAnonymizedCorrectly() throws Exception {
    // Given:
    StringBuilder statements = new StringBuilder();

    String line;
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(QUERIES_TO_ANONYMIZE_PATH.toString()), UTF_8))) {
      while ((line = reader.readLine()) != null) {
        statements.append(line);
      }
    }

    // When:
    final String anonStatementSelection = anon.anonymize(statements.toString());

    // Assert:
    Approvals.verify(anonStatementSelection);
  }

  private static JsonTestLoader<TestCase> testFileLoader() {
    return JsonTestLoader.of(QUERY_VALIDATION_TEST_DIR, QttTestFile.class);
  }

  @RunWith(Parameterized.class)
  public static class AnonQuerySetIntersectionTestClass {
    private List<String> sqlTokens;
    private final QueryAnonymizer anon = new QueryAnonymizer();
    private final String statement;

    public AnonQuerySetIntersectionTestClass(final String statement) {
      this.statement = statement;
    }

    @Before
    public void setUp() {
      sqlTokens = GrammarTokenExporter.getTokens();
      sqlTokens.addAll(Arrays.asList("INT", "DOUBLE", "VARCHAR", "BOOLEAN", "BIGINT", "BYTES",
          "*"));
    }

    @Parameterized.Parameters
    public static Collection<String> input() {
      return testFileLoader().load()
          .filter(statement -> !statement.expectedException().isPresent())
          .map(TestCase::statements)
          .flatMap(Collection::stream)
          .collect(Collectors.toSet());
    }

    @Test
    public void anonQuerySplitByWordsHasOnlyTokensInSetIntersectionWithQuery() {
      final String anonStatement = anon.anonymize(statement);
      final Set<String> statementWord = new HashSet<>(Arrays.asList(statement.split("[\\s\\(\\)<>,;]")));
      final Set<String> anonStatementWord = new HashSet<>(Arrays.asList(anonStatement.split("[\\s\\(\\)<>,;]")));
      final Set<String> intersection = new HashSet<>(statementWord);
      intersection.retainAll(anonStatementWord);

      // Assert:
      intersection.removeAll(sqlTokens);
      intersection.remove("");
      Assert.assertEquals(Collections.emptySet(), intersection);
    }
  }
}
