package io.confluent.ksql.ddl.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.ddl.commands.AlterSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class DdlCommandExecTest {
  private static final Set<SourceName> NO_QUERY_SOURCES = Collections.emptySet(); 
  private static final String SQL_TEXT = "some ksql";
  private static final SourceName STREAM_NAME = SourceName.of("s1");
  private static final SourceName OTHER_STREAM_NAME = SourceName.of("other-s1");
  private static final SourceName TABLE_NAME = SourceName.of("t1");
  private static final SourceName OTHER_TABLE_NAME = SourceName.of("other-t1");
  private static final String TOPIC_NAME = "topic";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F1"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F2"), SqlTypes.STRING)
      .build();
  private static final LogicalSchema SCHEMA2 = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F1"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("F2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("F3"), SqlTypes.STRING)
      .build();
  private static final ValueFormat VALUE_FORMAT = ValueFormat
      .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());
  private static final KeyFormat KEY_FORMAT = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of());
  private static final SourceName EXISTING_STREAM = SourceName.of("TEST0");
  private static final SourceName EXISTING_TABLE = SourceName.of("TEST2");
  private static final List<Column> NEW_COLUMNS = SCHEMA.columns();

  private CreateStreamCommand createStream;
  private CreateTableCommand createTable;
  private DropSourceCommand dropSource;
  private DropTypeCommand dropType;
  private RegisterTypeCommand registerType;
  private AlterSourceCommand alterSource;

  private final MutableMetaStore metaStore
      = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  @Mock
  private TimestampColumn timestampColumn;
  @Mock
  private KsqlStream source;
  @Mock
  private WindowInfo windowInfo;
  @Mock
  private SqlType type;

  private DdlCommandExec cmdExec;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(source.getName()).thenReturn(STREAM_NAME);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getKafkaTopicName()).thenReturn(TOPIC_NAME);

    cmdExec = new DdlCommandExec(metaStore);
    dropType = new DropTypeCommand("type");
    registerType = new RegisterTypeCommand(type,"type");
  }

  @Test
  public void shouldAddStreamWithCorrectSql() {
    // Given:
    givenCreateStream();

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).getSqlExpression(), is(SQL_TEXT));
  }

  @Test
  public void shouldAddStreamWithReplace() {
    // Given:
    givenCreateStream();
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

    // When:
    givenCreateStream(SCHEMA2, true);
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).getSchema(), is(SCHEMA2));
  }

  @Test
  public void shouldAddNormalStreamWhenNoTypeIsSpecified() {
    // Given:
    final CreateStreamCommand cmd = buildCreateStream(
        SourceName.of("t1"),
        SCHEMA,
        false,
        null
    );

    // When:
    cmdExec.execute(SQL_TEXT, cmd, true, NO_QUERY_SOURCES);

    // Then:
    final KsqlStream ksqlTable = (KsqlStream) metaStore.getSource(SourceName.of("t1"));
    assertThat(ksqlTable.isSource(), is(false));
  }

  @Test
  public void shouldAddSourceStream() {
    // Given:
    final CreateStreamCommand cmd = buildCreateStream(
        SourceName.of("t1"),
        SCHEMA,
        false,
        true
    );

    // When:
    cmdExec.execute(SQL_TEXT, cmd, true, NO_QUERY_SOURCES);

    // Then:
    final KsqlStream ksqlTable = (KsqlStream) metaStore.getSource(SourceName.of("t1"));
    assertThat(ksqlTable.isSource(), is(true));
  }

  @Test
  public void shouldAddSinkStream() {
    // Given:
    givenCreateStream();

    // When:
    cmdExec.execute(SQL_TEXT, createStream, true, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.getSource(STREAM_NAME).isCasTarget(), is(true));
  }

  @Test
  public void shouldThrowOnDropStreamWhenConstraintExist() {
    // Given:
    final CreateStreamCommand stream1 = buildCreateStream(SourceName.of("s1"), SCHEMA, false, false);
    final CreateStreamCommand stream2 = buildCreateStream(SourceName.of("s2"), SCHEMA, false, false);
    final CreateStreamCommand stream3 = buildCreateStream(SourceName.of("s3"), SCHEMA, false, false);
    cmdExec.execute(SQL_TEXT, stream1, true, Collections.emptySet());
    cmdExec.execute(SQL_TEXT, stream2, true, Collections.singleton(SourceName.of("s1")));
    cmdExec.execute(SQL_TEXT, stream3, true, Collections.singleton(SourceName.of("s1")));

    // When:
    final DropSourceCommand dropStream = buildDropSourceCommand(SourceName.of("s1"));
    final Exception e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> cmdExec.execute(SQL_TEXT, dropStream, false, Collections.emptySet())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot drop s1."));
    assertThat(e.getMessage(),
        containsString("The following streams and/or tables read from this source: [s2, s3]."));
    assertThat(e.getMessage(),
        containsString("You need to drop them before dropping s1."));
  }

  @Test
  public void shouldDropStreamIfConstraintExistsAndRestoreIsInProgress() {
    // Given:
    final CreateStreamCommand stream1 = buildCreateStream(SourceName.of("s1"), SCHEMA, false, false);
    final CreateStreamCommand stream2 = buildCreateStream(SourceName.of("s2"), SCHEMA, false, false);
    final CreateStreamCommand stream3 = buildCreateStream(SourceName.of("s3"), SCHEMA, false, false);
    cmdExec.execute(SQL_TEXT, stream1, true, Collections.emptySet());
    cmdExec.execute(SQL_TEXT, stream2, true, Collections.singleton(SourceName.of("s1")));
    cmdExec.execute(SQL_TEXT, stream3, true, Collections.singleton(SourceName.of("s1")));

    // When:
    final DropSourceCommand dropStream = buildDropSourceCommand(SourceName.of("s1"));
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropStream, false,
        Collections.emptySet(), true);

    // Then
    assertThat(result.isSuccess(), is(true));
    assertThat(
        result.getMessage(),
        equalTo(String.format("Source %s (topic: %s) was dropped.", STREAM_NAME, TOPIC_NAME))
    );
  }

  @Test
  public void shouldAddStreamWithCorrectKsqlTopic() {
    // Given:
    givenCreateStream();

    // When:
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

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
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(
        metaStore.getSource(STREAM_NAME).getKsqlTopic().getKeyFormat().getWindowInfo().get(),
        equalTo(windowInfo)
    );
  }

  @Test
  public void shouldAddTableWithCorrectWindowInfo() {
    // Given:
    givenCreateWindowedTable();

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(
        metaStore.getSource(TABLE_NAME).getKsqlTopic().getKeyFormat().getWindowInfo().get(),
        is(windowInfo)
    );
  }

  @Test
  public void shouldAddTableWithCorrectSql() {
    // Given:
    givenCreateTable();

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.getSource(TABLE_NAME).getSqlExpression(), is(SQL_TEXT));
  }

  @Test
  public void shouldAddTableWithCorrectTopic() {
    // Given:
    givenCreateTable();

    // When:
    cmdExec.execute(SQL_TEXT, createTable, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(
        metaStore.getSource(TABLE_NAME).getKsqlTopic(),
        equalTo(new KsqlTopic(TOPIC_NAME, KEY_FORMAT, VALUE_FORMAT))
    );
  }

  @Test
  public void shouldAddSinkTable() {
    // Given:
    givenCreateTable();

    // When:
    cmdExec.execute(SQL_TEXT, createTable, true, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.getSource(TABLE_NAME).isCasTarget(), is(true));
  }

  @Test
  public void shouldAddNormalTableWhenNoTypeIsSpecified() {
    // Given:
    final CreateTableCommand cmd = buildCreateTable(
        SourceName.of("t1"),
        false,
        null
    );

    // When:
    cmdExec.execute(SQL_TEXT, cmd, true, NO_QUERY_SOURCES);

    // Then:
    final KsqlTable ksqlTable = (KsqlTable) metaStore.getSource(SourceName.of("t1"));
    assertThat(ksqlTable.isSource(), is(false));
  }

  @Test
  public void shouldAddSourceTable() {
    // Given:
    final CreateTableCommand cmd = buildCreateTable(TABLE_NAME, false, true);

    // When:
    cmdExec.execute(SQL_TEXT, cmd, true, ImmutableSet.of(TABLE_NAME));

    // Then:
    final KsqlTable ksqlTable = (KsqlTable) metaStore.getSource(TABLE_NAME);
    assertThat(ksqlTable.isSource(), is(true));

    // Check query source was not added as constraints for source tables
    assertThat(metaStore.getSourceConstraints(TABLE_NAME), is(NO_QUERY_SOURCES));
  }

  @Test
  public void shouldThrowOnDropTableWhenConstraintExist() {
    // Given:
    final CreateTableCommand table1 = buildCreateTable(SourceName.of("t1"),
        false, false);
    final CreateTableCommand table2 = buildCreateTable(SourceName.of("t2"),
        false, false);
    final CreateTableCommand table3 = buildCreateTable(SourceName.of("t3"),
        false, false);
    cmdExec.execute(SQL_TEXT, table1, true, Collections.emptySet());
    cmdExec.execute(SQL_TEXT, table2, true, Collections.singleton(SourceName.of("t1")));
    cmdExec.execute(SQL_TEXT, table3, true, Collections.singleton(SourceName.of("t1")));

    // When:
    final DropSourceCommand dropStream = buildDropSourceCommand(SourceName.of("t1"));
    final Exception e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> cmdExec.execute(SQL_TEXT, dropStream, false, Collections.emptySet())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Cannot drop t1."));
    assertThat(e.getMessage(),
        containsString("The following streams and/or tables read from this source: [t2, t3]."));
    assertThat(e.getMessage(),
        containsString("You need to drop them before dropping t1."));
  }

  @Test
  public void shouldAlterStream() {
    // Given:
    alterSource = new AlterSourceCommand(EXISTING_STREAM, DataSourceType.KSTREAM.getKsqlType(), NEW_COLUMNS);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(result.isSuccess(), is(true));
    assertThat(metaStore.getSource(EXISTING_STREAM).getSchema().columns().size(), is(10));
    assertThat(metaStore.getSource(EXISTING_STREAM).getSqlExpression(), is("sqlexpression\nsome ksql"));
  }

  @Test
  public void shouldAlterTable() {
    // Given:
    alterSource = new AlterSourceCommand(EXISTING_TABLE, DataSourceType.KTABLE.getKsqlType(), NEW_COLUMNS);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(result.isSuccess(), is(true));
    assertThat(metaStore.getSource(EXISTING_TABLE).getSchema().columns().size(), is(8));
    assertThat(metaStore.getSource(EXISTING_TABLE).getSqlExpression(), is("sqlexpression\nsome ksql"));
  }

  @Test
  public void shouldThrowOnAlterMissingSource() {
    // Given:
    alterSource = new AlterSourceCommand(STREAM_NAME, DataSourceType.KSTREAM.getKsqlType(), NEW_COLUMNS);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES));

    // Then:
    assertThat(e.getMessage(), is("Source s1 does not exist."));
  }

  @Test
  public void shouldThrowOnMismatchedDatasourceType() {
    // Given:
    alterSource = new AlterSourceCommand(EXISTING_STREAM, DataSourceType.KTABLE.getKsqlType(), NEW_COLUMNS);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES));

    // Then:
    assertThat(e.getMessage(), is("Incompatible data source type is STREAM, but statement was ALTER TABLE"));
  }

  @Test
  public void shouldThrowOnAlterCAS() {
    // Given:
    givenCreateStream();
    cmdExec.execute(SQL_TEXT, createStream, true, NO_QUERY_SOURCES);
    alterSource = new AlterSourceCommand(STREAM_NAME, DataSourceType.KSTREAM.getKsqlType(), NEW_COLUMNS);

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES));

    // Then:
    assertThat(e.getMessage(), is("ALTER command is not supported for CREATE ... AS statements."));
  }

  @Test
  public void shouldThrowOnAddExistingColumn() {
    // Given:
    givenCreateStream();
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);
    alterSource = new AlterSourceCommand(STREAM_NAME, DataSourceType.KSTREAM.getKsqlType(), SCHEMA2.columns());

    // When:
    final KsqlException e = assertThrows(KsqlException.class,
        () -> cmdExec.execute(SQL_TEXT, alterSource, false, NO_QUERY_SOURCES));

    // Then:
    assertThat(e.getMessage(), is("Cannot add column `F1` to schema. A column with the same name already exists."));
  }

  @Test
  public void shouldDropMissingSource() {
    // Given:
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropSource, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(result.isSuccess(), is(true));
    assertThat(result.getMessage(), equalTo("Source " + STREAM_NAME + " does not exist."));
  }

  @Test
  public void shouldDropSource() {
    // Given:
    metaStore.putSource(source, false);
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropSource, false, NO_QUERY_SOURCES);

    // Then
    assertThat(result.isSuccess(), is(true));
    assertThat(
        result.getMessage(),
        equalTo(String.format("Source %s (topic: %s) was dropped.", STREAM_NAME, TOPIC_NAME))
    );
  }

  @Test
  public void shouldRegisterType() {
    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, registerType, false, NO_QUERY_SOURCES);

    // Then:
    assertThat("Expected successful resolution", result.isSuccess());
    assertThat(result.getMessage(), is("Registered custom type with name 'type' and SQL type " + type));
  }

  @Test
  public void shouldNotRegisterExistingType() {
    // Given:
    metaStore.registerType("type", SqlTypes.STRING);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, registerType, false, NO_QUERY_SOURCES);

    // Then:
    assertThat("Expected successful resolution", result.isSuccess());
    assertThat(result.getMessage(), is("type is already registered with type STRING"));
  }

  @Test
  public void shouldDropExistingType() {
    // Given:
    metaStore.registerType("type", SqlTypes.STRING);

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropType, false, NO_QUERY_SOURCES);

    // Then:
    assertThat(metaStore.resolveType("type").isPresent(), is(false));
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), is("Dropped type 'type'"));
  }

  @Test
  public void shouldDropMissingType() {
    // Given:
    metaStore.deleteType("type");

    // When:
    final DdlCommandResult result = cmdExec.execute(SQL_TEXT, dropType, false, NO_QUERY_SOURCES);

    // Then:
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), is("Type 'type' does not exist"));
  }

  @Test
  public void shouldWarnAddDuplicateStreamWithoutReplace() {
    // Given:
    givenCreateStream();
    cmdExec.execute(SQL_TEXT, createStream, false, NO_QUERY_SOURCES);

    // When:
    givenCreateStream(SCHEMA2, false);
    final DdlCommandResult result =cmdExec.execute(SQL_TEXT, createStream,
        false, NO_QUERY_SOURCES);

    // Then:
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), containsString("A stream with the same name already exists"));
  }

  @Test
  public void shouldWarnAddDuplicateTableWithoutReplace() {
    // Given:
    givenCreateTable();
    cmdExec.execute(SQL_TEXT, createTable, false, NO_QUERY_SOURCES);

    // When:
    givenCreateTable();
    final DdlCommandResult result =cmdExec.execute(SQL_TEXT, createTable,
        false, NO_QUERY_SOURCES);

    // Then:
    assertThat("Expected successful execution", result.isSuccess());
    assertThat(result.getMessage(), containsString("A table with the same name already exists"));
  }

  private void givenDropSourceCommand(final SourceName name) {
    dropSource = buildDropSourceCommand(name);
  }

  private DropSourceCommand buildDropSourceCommand(final SourceName name) {
    return new DropSourceCommand(name);
  }

  private void givenCreateStream() {
    givenCreateStream(SCHEMA, false);
  }

  private void givenCreateStream(final LogicalSchema schema, final boolean allowReplace) {
    createStream = buildCreateStream(STREAM_NAME, schema, allowReplace, false);
  }

  private CreateStreamCommand buildCreateStream(
      final SourceName streamName,
      final LogicalSchema schema,
      final boolean allowReplace,
      final Boolean isSource
  ) {
    return new CreateStreamCommand(
        streamName,
        schema,
        Optional.of(timestampColumn),
        "topic",
        Formats.of(
            KEY_FORMAT.getFormatInfo(),
            VALUE_FORMAT.getFormatInfo(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
        ),
        Optional.empty(),
        Optional.of(allowReplace),
        Optional.ofNullable(isSource)
    );
  }

  private void givenCreateWindowedStream() {
    createStream = new CreateStreamCommand(
        STREAM_NAME,
        SCHEMA,
        Optional.of(timestampColumn),
        "topic",
        Formats.of(
            KEY_FORMAT.getFormatInfo(),
            VALUE_FORMAT.getFormatInfo(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
        ),
        Optional.of(windowInfo),
        Optional.of(false),
        Optional.of(false)
    );
  }

  private void givenCreateWindowedTable() {
    createTable = new CreateTableCommand(
        TABLE_NAME,
        SCHEMA,
        Optional.of(timestampColumn),
        TOPIC_NAME,
        Formats.of(
            KEY_FORMAT.getFormatInfo(),
            VALUE_FORMAT.getFormatInfo(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
        ),
        Optional.of(windowInfo),
        Optional.of(false),
        Optional.of(false)
    );
  }

  private void givenCreateTable() {
    createTable = buildCreateTable(TABLE_NAME, false, false);
  }

  private CreateTableCommand buildCreateTable(
      final SourceName sourceName,
      final boolean allowReplace,
      final Boolean isSource
  ) {
    return new CreateTableCommand(
        sourceName,
        SCHEMA,
        Optional.of(timestampColumn),
        TOPIC_NAME,
        Formats.of(
            KEY_FORMAT.getFormatInfo(),
            VALUE_FORMAT.getFormatInfo(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
        ),
        Optional.empty(),
        Optional.of(allowReplace),
        Optional.ofNullable(isSource)
    );
  }
}
