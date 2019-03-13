package io.confluent.ksql.schema.inference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("SameParameterValue")
public class DefaultTopicInjectorTest {

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .field("f1", Schema.OPTIONAL_STRING_SCHEMA).build();

  private MutableMetaStore metaStore;
  private KsqlConfig ksqlConfig;
  private FakeKafkaTopicClient fakeClient;
  private DefaultTopicInjector injector;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    fakeClient = new FakeKafkaTopicClient();
    injector = new DefaultTopicInjector(fakeClient, metaStore);
    ksqlConfig = new KsqlConfig(new HashMap<>());
  }

  @Test
  public void shouldInferDirectlyFromWith() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS(
        "CREATE STREAM x WITH(kafka_topic='topic', partitions=20, replicas=5) "
            + "AS SELECT * FROM SOURCE;"
    );
    final KsqlConfig config = givenConfig(null, null);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(csas, config, ImmutableMap.of());

    // Then:
    assertThat(withTopic, hasProperties("topic", 20, (short) 5));
  }

  @Test
  public void shouldInferPartiallyFromWith() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS(
        "CREATE STREAM x WITH(kafka_topic='topic')AS SELECT * FROM SOURCE;"
    );
    final KsqlConfig config = givenConfig(null, null);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(csas, config, ImmutableMap.of());

    // Then:
    assertThat(withTopic, hasProperties("topic", 4, (short) 2));
  }

  @Test
  public void shouldInferTopicInfoFromSource() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS("source", "sink");
    final KsqlConfig config = givenConfig(null, null);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(csas, config, ImmutableMap.of());

    // Then:
    assertThat(withTopic, hasProperties("SINK", 4, (short) 2));
  }

  @Test
  public void shouldInferTopicInfoFromConfigIfPresent() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS("source", "sink");
    final KsqlConfig config = givenConfig(1, (short) 1);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(csas, config, ImmutableMap.of());

    // Then:
    assertThat(withTopic, hasProperties("SINK", 1, (short) 1));
  }

  @Test
  public void shouldInferTopicInfoFromBothConfigAndSourceIfPartial() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS("source", "sink");
    final KsqlConfig config = givenConfig(1, null);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(csas, config, ImmutableMap.of());

    // Then:
    assertThat(withTopic, hasProperties("SINK", 1, (short) 2));
  }

  @Test
  public void shouldInferTopicInfoFromOverrideIfPresent() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS("source", "sink");
    final KsqlConfig config = givenConfig(1, (short) 1);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(
            csas,
            config,
            ImmutableMap.of(
                KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10,
                KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, 10
            ));

    // Then:
    assertThat(withTopic, hasProperties("SINK", 10, (short) 10));
  }

  @Test
  public void shouldInferTopicInfoFromBothOverrideAndSourceIfPartial() {
    // Given:
    givenSource("SOURCE", "source", DataSourceType.KSTREAM, 4, (short) 2);
    final PreparedStatement<CreateStreamAsSelect> csas = givenCSAS("source", "sink");
    final KsqlConfig config = givenConfig(1, null);

    // When:
    final PreparedStatement<CreateStreamAsSelect> withTopic =
        injector.forStatement(
            csas,
            config,
            ImmutableMap.of(
                KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10
            ));

    // Then:
    assertThat(withTopic, hasProperties("SINK", 10, (short) 2));
  }

  @Test
  public void shouldDoNothingForNonCAS() {
    // Given:
    final PreparedStatement<ListProperties> prepared =
        PreparedStatement.of("", new ListProperties(Optional.empty()));

    // When:
    final PreparedStatement<ListProperties> output =
        injector.forStatement(prepared, ksqlConfig, ImmutableMap.of());

    // Then:
    assertThat(output, is(prepared));
  }

  private Matcher<PreparedStatement<CreateStreamAsSelect>> hasProperties(
      final String name,
      final int partitions,
      final short replicas
  ) {
    return new TypeSafeMatcher<PreparedStatement<CreateStreamAsSelect>>() {
      @Override
      protected boolean matchesSafely(PreparedStatement<CreateStreamAsSelect> item) {
        final Map<?, ?> properties = item.getStatement().getProperties();
        return properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)
              .equals(new StringLiteral(name))
            && properties.get(KsqlConstants.SINK_NUMBER_OF_PARTITIONS)
              .equals(new IntegerLiteral(partitions))
            && properties.get(KsqlConstants.SINK_NUMBER_OF_REPLICAS)
              .equals(new IntegerLiteral(replicas));
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format("Expected (name = %s, partitions = %d, repliacs = %d)",
                name,
                partitions,
                replicas));
      }
    };
  }

  private KsqlConfig givenConfig(
      @Nullable final Integer numPartitions,
      @Nullable final Short numReplicas
  ) {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (numPartitions != null) {
      builder.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, numPartitions);
    }
    if (numReplicas != null) {
      builder.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, numReplicas);
    }
    return new KsqlConfig(builder.build());
  }

  private PreparedStatement<CreateStreamAsSelect> givenCSAS(final String source, final String sink) {
    final String sql = String.format("CREATE STREAM %s AS SELECT * FROM %s;", sink, source);
    return givenCSAS(sql);
  }

  @SuppressWarnings("unchecked")
  private PreparedStatement<CreateStreamAsSelect> givenCSAS(final String sql) {
    final DefaultKsqlParser parser = new DefaultKsqlParser();
    final List<ParsedStatement> statements = parser
        .parse(sql);
    return (PreparedStatement<CreateStreamAsSelect>) parser.prepare(statements.get(0), metaStore);
  }

  private void givenSource(
      final String name,
      final String topicName,
      final DataSourceType type,
      final int numPartitions,
      final short numReplicas
  ) {
    final KsqlTopic ksqlTopic =
        new KsqlTopic(topicName, topicName, new KsqlJsonTopicSerDe(), false);
    fakeClient.createTopic(topicName, numPartitions, numReplicas);

    final StructuredDataSource dataSource;
    switch (type) {
      case KSTREAM:
        dataSource =
            new KsqlStream<>(
                "",
                name,
                SCHEMA,
                SCHEMA.fields().get(0),
                new MetadataTimestampExtractionPolicy(),
                ksqlTopic,
                Serdes.String()
            );
        break;
      case KTABLE:
        dataSource =
            new KsqlTable<>(
                "",
                name,
                SCHEMA,
                SCHEMA.fields().get(0),
                new MetadataTimestampExtractionPolicy(),
                ksqlTopic,
                Serdes.String()
            );
        break;
      default:
        throw new IllegalArgumentException("Unexpected type: " + type);
    }
    metaStore.putSource(dataSource);
  }

}