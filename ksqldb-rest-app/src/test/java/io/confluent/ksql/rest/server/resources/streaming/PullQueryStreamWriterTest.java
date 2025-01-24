package io.confluent.ksql.rest.server.resources.streaming;

import static io.confluent.ksql.rest.server.resources.streaming.PullQueryStreamWriter.QueueWrapper.END_ROW;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.pull.PullQueryRow;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PullQueryStreamWriterTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("a"), SqlTypes.STRING)
      .build();

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Mock
  private PullQueryResult pullQueryResult;
  @Mock
  private PullQueryWriteStream pullQueryQueue;
  @Mock
  private Clock clock;
  @Mock
  private PreparedStatement<Query> statement;

  @Captor
  private ArgumentCaptor<Consumer<Throwable>> throwableConsumerCapture;
  @Captor
  private ArgumentCaptor<Consumer<Void>> completeCapture;

  private ScheduledExecutorService executorService;
  private ByteArrayOutputStream out;
  private PullQueryStreamWriter writer;

  @Before
  public void setUp() {
    when(pullQueryResult.getQueryId()).thenReturn(new QueryId("Query id"));
    when(pullQueryResult.getSchema()).thenReturn(SCHEMA);
    doNothing().when(pullQueryResult).onCompletion(completeCapture.capture());
    writer = new PullQueryStreamWriter(pullQueryResult, 1000, ApiJsonMapper.INSTANCE.get(),
        pullQueryQueue, clock, new CompletableFuture<>(), statement);

    executorService = Executors.newSingleThreadScheduledExecutor();

    out = new ByteArrayOutputStream();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void shouldWriteAnyPendingRowsBeforeReportingException() throws IOException {
    // Given:
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(pullQueryQueue).drainRowsTo(any());

    givenUncaughtException(new KsqlException("Server went Boom"));

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3"),
        containsString("Server went Boom")
    ));
  }

  @Test
  public void shouldWriteNoRows() throws IOException {
    // Given:
    completeCapture.getValue().accept(null);
    doAnswer(streamRows())
        .when(pullQueryQueue).drainRowsTo(any());

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header")
    ));
  }

  @Test
  public void shouldExitAndDrainIfAlreadyComplete() throws IOException {
    // Given:
    completeCapture.getValue().accept(null);
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(pullQueryQueue).drainRowsTo(any());

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }

  @Test
  public void shouldExitAndDrainIfLimitReached() throws IOException {
    // Given:
    doAnswer(streamRows("Row1", "Row2", "Row3"))
        .when(pullQueryQueue).drainRowsTo(any());

    completeCapture.getValue().accept(null);

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, hasItems(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")));
  }


  @Test
  public void shouldWriteOneAndClose() throws InterruptedException, IOException {
    // Given:
    when(pullQueryQueue.pollRow(anyLong(), any()))
        .thenReturn(new PullQueryRow(ImmutableList.of("Row1"), SCHEMA, Optional.empty(), Optional.empty()))
        .thenAnswer(inv -> {
          completeCapture.getValue().accept(null);
          return END_ROW;
        });

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1")
    ));
  }

  @Test
  public void shouldWriteTwoAndClose() throws InterruptedException, IOException {
    // Given:
    when(pullQueryQueue.pollRow(anyLong(), any()))
        .thenReturn(streamRow("Row1"))
        .thenReturn(streamRow("Row2"))
        .thenAnswer(inv -> {
          completeCapture.getValue().accept(null);
          return END_ROW;
        });

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2")
    ));
  }

  @Test
  public void shouldWriteTwoAndCloseWithOneMoreQueue() throws InterruptedException, IOException {
    // Given:
    when(pullQueryQueue.pollRow(anyLong(), any()))
        .thenReturn(streamRow("Row1"))
        .thenReturn(streamRow("Row2"))
        .thenAnswer(inv -> {
          completeCapture.getValue().accept(null);
          return END_ROW;
        });
    doAnswer(streamRows("Row3"))
        .when(pullQueryQueue).drainRowsTo(any());

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    verify(pullQueryQueue).end();
    final List<String> lines = getOutput(out);
    assertThat(lines, contains(
        containsString("header"),
        containsString("Row1"),
        containsString("Row2"),
        containsString("Row3")
    ));
  }

  @Test
  public void shouldProperlyEscapeJSON() throws InterruptedException, IOException {
    // Given:
    when(pullQueryQueue.pollRow(anyLong(), any()))
        .thenReturn(streamRow("foo\nbar"))
        .thenAnswer(inv -> {
          completeCapture.getValue().accept(null);
          return null;
        });

    // When:
    writer.write(out);

    // Then:
    assertThat(writer.isClosed(), is (true));
    final List<String> lines = getOutput(out);
    assertThat(lines.size(), is(2));
    assertThat(lines, contains(
        containsString("header"),
        containsString("foo\\nbar")
    ));
    String lastRow = lines.get(1).replaceFirst("]$", "");
    StreamedRow row = ApiJsonMapper.INSTANCE.get().readValue(lastRow, StreamedRow.class);
    assertThat(row.getRow().isPresent(), is(true));
    assertThat(row.getRow().get().getColumns().get(0), is("foo\nbar"));
  }

  private void givenUncaughtException(final KsqlException e) {
    verify(pullQueryResult).onException(throwableConsumerCapture.capture());
    throwableConsumerCapture.getValue().accept(e);
  }

  private static Answer<Void> streamRows(final Object... rows) {
    return inv -> {
      final Collection<PullQueryRow> output = inv.getArgument(0);

      Arrays.stream(rows)
          .map(row -> new PullQueryRow(ImmutableList.of(row), SCHEMA, Optional.empty(), Optional.empty()))
          .forEach(output::add);

      return null;
    };
  }

  private static PullQueryRow streamRow(final Object row) {
    return new PullQueryRow(ImmutableList.of(row), SCHEMA, Optional.empty(), Optional.empty());
  }

  private static List<String> getOutput(final ByteArrayOutputStream out) throws IOException {
    // Make sure it's parsable as valid JSON
    ApiJsonMapper.INSTANCE.get().readTree(out.toByteArray());
    final String[] lines = new String(out.toByteArray(), StandardCharsets.UTF_8).split("\n");
    return Arrays.stream(lines)
        .filter(line -> !line.isEmpty())
        .collect(Collectors.toList());
  }
}
