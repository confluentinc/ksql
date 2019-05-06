package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.StringLiteral;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WithClauseUtilTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldParseIntLiteralPartitions() {
    // Given:
    final Expression expression = new IntegerLiteral(1);

    // When:
    final int partitions = WithClauseUtil.parsePartitions(expression.toString());

    // Then:
    assertThat(partitions, equalTo(1));
  }

  @Test
  public void shouldParseLongLiteralPartitions() {
    // Given:
    final Expression expression = new LongLiteral(1);

    // When:
    final int partitions = WithClauseUtil.parsePartitions(expression.toString());

    // Then:
    assertThat(partitions, equalTo(1));
  }

  @Test
  public void shouldParseStringLiteralPartitions() {
    // Given:
    final Expression expression = new StringLiteral("1");

    // When:
    final int partitions = WithClauseUtil.parsePartitions(expression.toString());

    // Then:
    assertThat(partitions, equalTo(1));
  }

  @Test
  public void shouldFailParseNonNumericPartitions() {
    // Given:
    final Expression expression = new StringLiteral("not a number");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid number of partitions in WITH clause");

    // When:
    WithClauseUtil.parsePartitions(expression.toString());
  }

  @Test
  public void shouldFailParseFractionPartitions() {
    // Given:
    final Expression expression = new StringLiteral("0.5");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of partitions in WITH clause");

    // When:
    WithClauseUtil.parsePartitions(expression.toString());
  }

  @Test
  public void shouldFailParseNegativePartitions() {
    // Given:
    final Expression expression = new IntegerLiteral(-1);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of partitions in WITH clause (must be positive)");

    // When:
    WithClauseUtil.parsePartitions(expression.toString());
  }

  @Test
  public void shouldFailParseZeroPartitions() {
    // Given:
    final Expression expression = new IntegerLiteral(0);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of partitions in WITH clause (must be positive)");

    // When:
    WithClauseUtil.parsePartitions(expression.toString());
  }

  @Test
  public void shouldFailParsePartitionsOverflow() {
    // Given:
    final Expression expression = new StringLiteral("9999999999999999999999");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of partitions in WITH clause");

    // When:
    WithClauseUtil.parsePartitions(expression.toString());
  }

  @Test
  public void shouldParseIntLiteralReplicas() {
    // Given:
    final Expression expression = new IntegerLiteral(1);

    // When:
    final short replicas = WithClauseUtil.parseReplicas(expression.toString());

    // Then:
    assertThat(replicas, equalTo((short) 1));
  }

  @Test
  public void shouldParseLongLiteralReplicas() {
    // Given:
    final Expression expression = new LongLiteral(1);

    // When:
    final short replicas = WithClauseUtil.parseReplicas(expression.toString());

    // Then:
    assertThat(replicas, equalTo((short) 1));
  }

  @Test
  public void shouldParseStringLiteralReplicas() {
    // Given:
    final Expression expression = new StringLiteral("1");

    // When:
    final short replicas = WithClauseUtil.parseReplicas(expression.toString());

    // Then:
    assertThat(replicas, equalTo((short) 1));
  }

  @Test
  public void shouldFailParseNonNumericReplicas() {
    // Given:
    final Expression expression = new StringLiteral("not a number");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid number of replicas in WITH clause");

    // When:
    WithClauseUtil.parseReplicas(expression.toString());
  }

  @Test
  public void shouldFailParseFractionReplicas() {
    // Given:
    final Expression expression = new StringLiteral("0.5");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of replicas in WITH clause");

    // When:
    WithClauseUtil.parseReplicas(expression.toString());
  }

  @Test
  public void shouldFailParseZeroReplicas() {
    // Given:
    final Expression expression = new IntegerLiteral(0);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of replicas in WITH clause (must be positive)");

    // When:
    WithClauseUtil.parseReplicas(expression.toString());
  }

  @Test
  public void shouldFailParseNegativeReplicas() {
    // Given:
    final Expression expression = new IntegerLiteral(-1);

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of replicas in WITH clause (must be positive)");

    // When:
    WithClauseUtil.parseReplicas(expression.toString());
  }

  @Test
  public void shouldFailParseReplicasOverflow() {
    // Given:
    final Expression expression = new StringLiteral("9999999999999999999999");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Invalid number of replicas in WITH clause");

    // When:
    WithClauseUtil.parseReplicas(expression.toString());
  }

}