package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.execution.codegen.CodeGenTestUtil;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil.Evaluator;
import org.junit.Test;

public class NullSafeTest {

  @Test
  public void shouldGenerateApply() {
    // Given:
    final String mapperCode = LambdaUtil
        .toJavaCode("val", Long.class, "val.longValue() + 1");

    // When:
    final String javaCode = NullSafe
        .generateApply("arguments.get(\"input\")", mapperCode, Long.class);

    // Then:
    final Evaluator evaluator = CodeGenTestUtil.cookCode(javaCode, Long.class);
    assertThat(evaluator.evaluate("input", 10L), is(11L));
    assertThat(evaluator.evaluate("input", null), is(nullValue()));
  }

  @Test
  public void shouldGenerateApplyOrDefault() {
    // Given:
    final String mapperCode = LambdaUtil
        .toJavaCode("val", Long.class, "val.longValue() + 1");

    // When:
    final String javaCode = NullSafe
        .generateApplyOrDefault("arguments.get(\"input\")", mapperCode, "99L", Long.class);

    // Then:
    final Evaluator evaluator = CodeGenTestUtil.cookCode(javaCode, Long.class);
    assertThat(evaluator.evaluate("input", 10L), is(11L));
    assertThat(evaluator.evaluate("input", null), is(99L));
  }
}