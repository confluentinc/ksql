package io.confluent.ksql.test.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.ksql.test.tools.Record;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.Test;

public class RecordNodeTest {

  @Test
  public void shouldUseExactDecimals() {
    // Given:
    final RecordNode node = new RecordNode(
      "topic",
        NullNode.getInstance(),
        new DecimalNode(new BigDecimal("10.000")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    // When:
    final Record result = node.build();

    // Then:
    assertThat(result.value(), is(new BigDecimal("10.000")));
  }
}