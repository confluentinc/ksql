package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Test;

public class RegexpSplitTest {

  private final RegexpSplit udf = new RegexpSplit();

  @Test
  public void shouldReturnNull() {
    assertThat(udf.regexpSplit(null, "a"), is(nullValue()));
    assertThat(udf.regexpSplit("string", null), is(nullValue()));
    assertThat(udf.regexpSplit(null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnOriginalStringOnNotFoundRegexp() {
    assertThat(udf.regexpSplit("", "z"), contains(""));
    assertThat(udf.regexpSplit("x-y", "z"), contains("x-y"));
  }

  @Test
  public void shouldSplitAllCharactersByGivenAnEmptyRegexp() {
    assertThat(udf.regexpSplit("", ""), contains(""));
    assertThat(udf.regexpSplit("x-y", ""), contains("x", "-", "y"));
    assertThat(udf.regexpSplit("x", ""), contains("x"));
  }

  @Test
  public void shouldSplitStringByGivenRegexp() {
    assertThat(udf.regexpSplit("x-y", "-"), contains("x", "y"));
    assertThat(udf.regexpSplit("x-y", "x"), contains("", "-y"));
    assertThat(udf.regexpSplit("x-y", "y"), contains("x-", ""));
    assertThat(udf.regexpSplit("a-b-c-d", "-"), contains("a", "b", "c", "d"));

    assertThat(udf.regexpSplit("x-y", "."), contains("", "", "", ""));
    assertThat(udf.regexpSplit("a-b-c-b-d", ".b."), contains("a", "c", "d"));
    assertThat(udf.regexpSplit("a-b-c", "^.."), contains("", "b-c"));
    assertThat(udf.regexpSplit("a-b-ccatd-ecatf", "(-|cat)"),
        contains("a", "b", "c", "d", "e", "f"));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfRegexpIsFoundAtTheBeginningOrEnd() {
    assertThat(udf.regexpSplit("-A", "-"), contains("", "A"));
    assertThat(udf.regexpSplit("-A-B", "-"), contains("", "A", "B"));
    assertThat(udf.regexpSplit("A-", "-"), contains("A", ""));
    assertThat(udf.regexpSplit("A-B-", "-"), contains("A", "B", ""));
    assertThat(udf.regexpSplit("-A-B-", "-"), contains("", "A", "B", ""));

    assertThat(udf.regexpSplit("A", "^"), contains("A"));
    assertThat(udf.regexpSplit("A", "$"), contains("A"));
    assertThat(udf.regexpSplit("AB", "^"), contains("AB"));
    assertThat(udf.regexpSplit("AB", "$"), contains("AB"));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfRegexIsFoundInContiguousPositions() {
    assertThat(udf.regexpSplit("A--A", "-"), contains("A", "", "A"));
    assertThat(udf.regexpSplit("z--A--z", "-"), contains("z", "", "A", "", "z"));
    assertThat(udf.regexpSplit("--A--A", "-"), contains("", "", "A", "", "A"));
    assertThat(udf.regexpSplit("A--A--", "-"), contains("A", "", "A", "", ""));

    assertThat(udf.regexpSplit("aababa", "ab"), contains("a", "", "a"));
    assertThat(udf.regexpSplit("aababa", "(ab)+"), contains("a", "a"));
    assertThat(udf.regexpSplit("aabcda", "(ab|cd)"), contains("a", "", "a"));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowOnInvalidPattern() {
    udf.regexpSplit("abcd", "(()");
  }
}
