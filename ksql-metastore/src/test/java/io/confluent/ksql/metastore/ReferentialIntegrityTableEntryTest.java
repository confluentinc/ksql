/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metastore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
public class ReferentialIntegrityTableEntryTest {

  private ReferentialIntegrityTableEntry entry;

  @Before
  public void setUp() {
    entry = new ReferentialIntegrityTableEntry();
  }

  @Test
  public void shouldAddSourceForQuery() {
    // When:
    entry.addSourceForQueries("someId");

    // Then:
    assertThat(entry.getSourceForQueries(), contains("someId"));
  }

  @Test
  public void shouldAddSinkForQuery() {
    // When:
    entry.addSinkForQueries("someId");

    // Then:
    assertThat(entry.getSinkForQueries(), contains("someId"));
  }

  @Test
  public void shouldRemoveQuery() {
    // Given:
    entry.addSourceForQueries("someId");
    entry.addSourceForQueries("otherId");
    entry.addSinkForQueries("someId");
    entry.addSinkForQueries("anotherId");

    // When:
    entry.removeQuery("someId");

    // Then:
    assertThat(entry.getSourceForQueries(), contains("otherId"));
    assertThat(entry.getSinkForQueries(), contains("anotherId"));
  }

  @Test
  public void shouldDeepCopy() {
    // Given:
    entry.addSourceForQueries("sourceId");
    entry.addSinkForQueries("sinkId");
    final ReferentialIntegrityTableEntry copy = entry.copy();

    // When:
    entry.removeQuery("sourceId");
    entry.removeQuery("sinkId");

    // Then:
    assertThat(copy.getSourceForQueries(), contains("sourceId"));
    assertThat(copy.getSinkForQueries(), contains("sinkId"));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 5_000)
        .parallel()
        .forEach(idx -> {
          final String sourceQueryId = "source" + idx;
          final String sinkQueryId = "sink" + idx;
          entry.addSourceForQueries(sourceQueryId);
          entry.addSinkForQueries(sinkQueryId);

          entry.getSourceForQueries();
          entry.getSinkForQueries();

          entry.copy();

          entry.removeQuery(sourceQueryId);
          entry.removeQuery(sinkQueryId);
        });

    assertThat(entry.getSourceForQueries(), is(empty()));
    assertThat(entry.getSinkForQueries(), is(empty()));
  }
}