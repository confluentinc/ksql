package io.confluent.ksql.ddl.commands;

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import java.util.Set;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class DdlCommandExecTest {
  private static final String SQL_TEXT = "some ksql";
  private static final SourceName STREAM_NAME = SourceName.of("s1");
  private static final SourceName TABLE_NAME = SourceName.of("t1");
  private static final String TOPIC_NAME = "topic";
  private static final LogicalSchema SCHEMA = new LogicalSchema.Builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F1"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F2"), SqlTypes.STRING)
      .build();
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));
  private static final KeyFormat KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private static final Set<SerdeOption> SERDE_OPTIONS = SerdeOption.none();

  private CreateStreamCommand createStream;
  private CreateTableCommand createTable;
  private DropSourceCommand dropSource;
  private DropTypeCommand dropType;

  private final MutableMetaStore metaStore
      = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  @Mock
  private TimestampColumn timestampColumn;
  @Mock
  private KsqlStream source;
  @Mock
  private WindowInfo windowInfo;

  private DdlCommandExec cmdExec;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() {
    //when(metaStore.getSource(STREAM_NAME)).thenReturn(source);
    when(source.getName()).thenReturn(STREAM_NAME);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getKafkaTopicName()).thenReturn(TOPIC_NAME);

    cmdExec = new DdlCommandExec(metaStore);
    dropType = new DropTypeCommand("type");
  }

  @Test
  public void shouldAddStreamWithKeyField() {
    // Given:
    givenCreateStreamWithKey(Optional.of("F1"));

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName("F1"));
  }

  @Test
  public void shouldAddStreamWithCorrectSql() {
    // Given:
    givenCreateStreamWithKey(Optional.of("F1"));

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).getSqlExpression(), is(SQL_TEXT));
  }

  @Test
  public void shouldAddSinkStream() {
    // Given:
    givenCreateStreamWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createStream, true);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).isCasTarget(), is(true));
  }

  @Test
  public void shouldAddStreamWithNoKeyField() {
    // Given:
    givenCreateStreamWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName(Optional.empty()));
  }

  @Test
  public void shouldAddStreamWithCorrectKsqlTopic() {
    // Given:
    givenCreateStreamWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false);

    // Then:
    assertThat(
        metaStore.getSource(STREAM_NAME).getKsqlTopic(),
        equalTo(new KsqlTopic(TOPIC_NAME, KEY_FORMAT, VALUE_FORMAT))
    );
  }

  @Test
  public void shouldAddStreamWithCorrectWindowInfo() {
    // Given:
    givenCreateWindowedStream();

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false);

    // Then:
    assertThat(
        metaStore.getSource(STREAM_NAME).getKsqlTopic().getKeyFormat().getWindowInfo().get(),
        equalTo(windowInfo)
    );
  }

  @Test
  public void shouldAddTableWithKeyField() {
    // Given:
    givenCreateTableWithKey(Optional.of("F1"));

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName("F1"));
  }

  @Test
  public void shouldAddTableWithNoKeyField() {
    // Given:
    givenCreateTableWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName(Optional.empty()));
  }

  @Test
  public void shouldAddTableWithCorrectWindowInfo() {
    // Given:
    givenCreateWindowedTable();

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false);

    // Then:
    assertThat(
        metaStore.getSource(TABLE_NAME).getKsqlTopic().getKeyFormat().getWindowInfo().get(),
        is(windowInfo)
    );
  }

  @Test
  public void shouldAddTableWithCorrectSql() {
    // Given:
    givenCreateTableWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(TABLE_NAME).getSqlExpression(), is(SQL_TEXT));
  }

  @Test
  public void shouldAddTableWithCorrectTopic() {
    // Given:
    givenCreateTableWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false);

    // Then:
    assertThat(
        metaStore.getSource(TABLE_NAME).getKsqlTopic(),
        equalTo(new KsqlTopic(TOPIC_NAME, KEY_FORMAT, VALUE_FORMAT))
    );
  }

  @Test
  public void shouldAddSinkTable() {
    // Given:
    givenCreateTableWithKey(Optional.empty());

    // When:
    cmdExec.execute(SQL_TEXT, createTable, true);

    // Then:
    assertThat(metaStore.getSource(TABLE_NAME).isCasTarget(), is(true));
  }

  @Test
  public void shouldDropMissingSource() {
    // Given:
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropSource, false);

    // Then:
    assertThat(result.isSuccess(), is(true));
    assertThat(result.getMessage(), equalTo("Source " + STREAM_NAME + " does not exist."));
  }

  @Test
  public void shouldDropSource() {
    // Given:
    metaStore.putSource(source);
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropSource, false);

    // Then
    assertThat(result.isSuccess(), is(true));
    assertThat(
        result.getMessage(),
        equalTo(String.format("Source %s (topic: %s) was dropped.",  STREAM_NAME, TOPIC_NAME))
    );
  }

  @Test
  public void shouldDropExistingType() {
    // Given:
    metaStore.registerType("type", SqlTypes.STRING);

    // When:
    final DdlCommandResult result  = cmdExec.execute(SQL_TEXT, dropType, false);

    // Then:
    assertThat(metaStore.resolveType("type").isPresent(), is(false));
    MatcherAssert.assertThat("Expected successful execution", result.isSuccess());
    MatcherAssert.assertThat(result.getMessage(), is("Dropped type 'type'"));
  }

  @Test
  public void shouldDropMissingType() {
    // Given:
    metaStore.deleteType("type");

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropType, false);

    // Then:
    MatcherAssert.assertThat("Expected successful execution", result.isSuccess());
    MatcherAssert.assertThat(result.getMessage(), is("Type 'type' does not exist"));
  }

  private void givenDropSourceCommand(final SourceName name) {
    dropSource = new DropSourceCommand(name);
  }

  private void givenCreateStreamWithKey(final Optional<String> keyField) {
    createStream = new CreateStreamCommand(
        STREAM_NAME,
        SCHEMA,
        keyField.map(ColumnName::of),
        Optional.of(timestampColumn),
        "topic",
        Formats.of(
            KEY_FORMAT,
            VALUE_FORMAT,
            SERDE_OPTIONS),
        Optional.empty()
    );
  }

  private void givenCreateWindowedStream() {
    createStream = new CreateStreamCommand(
        STREAM_NAME,
        SCHEMA,
        Optional.empty(),
        Optional.of(timestampColumn),
        "topic",
        Formats.of(
            KEY_FORMAT,
            VALUE_FORMAT,
            SERDE_OPTIONS),
        Optional.of(windowInfo)
    );
  }

  private void givenCreateWindowedTable() {
    createTable = new CreateTableCommand(
        TABLE_NAME,
        SCHEMA,
        Optional.empty(),
        Optional.of(timestampColumn),
        TOPIC_NAME,
        Formats.of(
            KEY_FORMAT,
            VALUE_FORMAT,
            SERDE_OPTIONS
        ),
        Optional.of(windowInfo)
    );
  }

  private void givenCreateTableWithKey(final Optional<String> keyField) {
    createTable = new CreateTableCommand(
        TABLE_NAME,
        SCHEMA,
        keyField.map(ColumnName::of),
        Optional.of(timestampColumn),
        TOPIC_NAME,
        Formats.of(
            KEY_FORMAT,
            VALUE_FORMAT,
            SERDE_OPTIONS
        ),
        Optional.empty()
    );
  }
}
