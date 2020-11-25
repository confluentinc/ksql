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

package io.confluent.ksql.execution.streams.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StringTimestampExtractorTest {

  private static final String FORAMT = "yyyy-MMM-dd";

  @Mock
  public ColumnExtractor columnExtractor;
  @Mock
  private Object key;
  @Mock
  private GenericRow value;
  private StringTimestampExtractor extractor;

  @Before
  public void setUp() {
    extractor = new StringTimestampExtractor(FORAMT, columnExtractor);
    when(columnExtractor.extract(any(), any())).thenReturn("2010-Jan-11");
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullFormat() {
    new StringTimestampExtractor(null, columnExtractor);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldPassRecordToColumnExtractor() {
    // When:
    extractor.extract(key, value);

    // Then:
    verify(columnExtractor).extract(key, value);
  }

  @Test
  public void shouldExtractTimestampFromStringWithFormat() throws ParseException {
    // Given:
    when(columnExtractor.extract(any(), any())).thenReturn("2010-Jan-11");

    // When:
    final long actualTime = extractor.extract(key, value);

    final long expectedTime = new SimpleDateFormat(FORAMT).parse("2010-Jan-11").getTime();
    assertThat(actualTime, equalTo(expectedTime));
  }

  @Test
  public void shouldThrowIfStringDoesNotMatchFormat() {
    // Given:
    when(columnExtractor.extract(any(), any())).thenReturn("11-Jan-2010");

    // When:
    final Exception e = assertThrows(
        DateTimeParseException.class,
        () -> extractor.extract(key, value)
    );

    // Then:
    assertThat(e.getMessage(), containsString("11-Jan-2010"));
  }
}