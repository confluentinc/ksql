package io.confluent.ksql.planner.plan;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.JoiningNode.RequiredFormat;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class JoiningNodeTest {

  private static final SourceName SOURCE_A = SourceName.of("Src_A");
  private static final SourceName SOURCE_B = SourceName.of("Src_B");
  private static final SourceName SOURCE_C = SourceName.of("Src_C");
  private static final SourceName SOURCE_D = SourceName.of("Src_D");

  private static final FormatInfo FORMAT_1 = FormatInfo
      .of("Fmt_1", ImmutableMap.of("prop", "one"));

  private static final FormatInfo FORMAT_2 = FormatInfo
      .of("Fmt_2", ImmutableMap.of());

  @Test
  public void shouldThrowOnAddOnFormatMismatch() {
    // Given:
    final RequiredFormat requiredFmt = RequiredFormat
        .of(FORMAT_1, SOURCE_A);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> requiredFmt.add(
            FormatInfo.of("Different", FORMAT_1.getProperties()),
            SOURCE_B
        )
    );

    // Then:
    assertThat(e.getMessage(), is(
        "Incompatible key formats. `Src_A` has FMT_1 while `Src_B` has DIFFERENT."
            + System.lineSeparator()
            + "Correct the key format by creating a copy of the table with the correct key format. For example:"
            + System.lineSeparator()
            + "\tCREATE TABLE T_COPY"
            + System.lineSeparator()
            + "\t WITH (KEY_FORMAT = <required format>, <other key format config>)"
            + System.lineSeparator()
            + "\t AS SELECT * FROM T;"));
  }

  @Test
  public void shouldThrowOnAddOnFormatPropsMismatch() {
    // Given:
    final RequiredFormat requiredFmt = RequiredFormat
        .of(FORMAT_1, SOURCE_A)
        .add(FORMAT_1, SOURCE_B);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> requiredFmt.add(
            FormatInfo.of(FORMAT_1.getFormat(), ImmutableMap.of("diff", "erent")),
            SOURCE_B
        )
    );

    // Then:
    assertThat(e.getMessage(), startsWith(
        "Incompatible key format properties. `Src_A` and `Src_B` have {prop=one} while `Src_B` has {diff=erent}."));
  }

  @Test
  public void shouldThrowOnMergeOnFormatPropsMismatch() {
    // Given:
    final RequiredFormat leftFmts = RequiredFormat
        .of(FORMAT_1, SOURCE_A)
        .add(FORMAT_1, SOURCE_B);

    final RequiredFormat rightFmts = RequiredFormat
        .of(FORMAT_2, SOURCE_C)
        .add(FORMAT_2, SOURCE_D);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> leftFmts.merge(rightFmts)
    );

    // Then:
    assertThat(e.getMessage(), startsWith(
        "Incompatible key formats. `Src_A` and `Src_B` have FMT_1 while `Src_C` and `Src_D` have FMT_2."));
  }

  @Test
  public void shouldAddIfSameFormat() {
    // Given:
    final RequiredFormat requiredFmt = RequiredFormat
        .of(FORMAT_1, SOURCE_A);

    // When:
    final RequiredFormat result = requiredFmt.add(FORMAT_1, SOURCE_B);

    // Then (did not throw):
    assertThat(result.format(), is(FORMAT_1));
  }

  @Test
  public void shouldMergeIfSameFormat() {
    // Given:
    final RequiredFormat leftFmts = RequiredFormat
        .of(FORMAT_1, SOURCE_A)
        .add(FORMAT_1, SOURCE_B);

    final RequiredFormat rightFmts = RequiredFormat
        .of(FORMAT_1, SOURCE_C)
        .add(FORMAT_1, SOURCE_D);

    // When:
    final RequiredFormat result = leftFmts.merge(rightFmts);

    // Then (did not throw):
    assertThat(result.format(), is(FORMAT_1));
  }
}