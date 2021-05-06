package io.confluent.ksql.test;

import io.confluent.ksql.test.QueryTranslationTest.QttTestFile;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.util.GrammarTokenExporter;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.approvaltests.Approvals;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import io.confluent.ksql.util.QueryAnonymizer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QueryAnonymizerTest {
  private static final Path QUERIES_TO_ANONYMIZE_PATH =
      Paths.get("src/test/java/io/confluent/ksql/test/QueriesToAnonymizeTest.txt");
  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private final QueryAnonymizer anon = new QueryAnonymizer();

  @Test
  public void queriesAreAnonymizedCorrectly() throws Exception {
    // Given:
    StringBuilder statements = new StringBuilder();
    try (Scanner s = new Scanner(new FileReader(QUERIES_TO_ANONYMIZE_PATH.toString()))) {
      while (s.hasNext()) {
        statements.append(s.nextLine());
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
    private static final Stream<TestCase> testCases = testFileLoader().load();
    private List<String> sqlTokens;
    private final QueryAnonymizer anon = new QueryAnonymizer();
    private final String statement;

    public AnonQuerySetIntersectionTestClass(final String statement) {
      this.statement = statement;
    }

    @Before
    public void setUp() {
      sqlTokens = GrammarTokenExporter.getTokens();
      sqlTokens.addAll(Arrays.asList("INT", "DOUBLE", "VARCHAR", "BOOLEAN", "BIGINT"));
    }

    @Parameterized.Parameters
    public static Collection<String> input() {
      return testCases
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
      Assert.assertEquals(0, intersection.size());
    }
  }
}
