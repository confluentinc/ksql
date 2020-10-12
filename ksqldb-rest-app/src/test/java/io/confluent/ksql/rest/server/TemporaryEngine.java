/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.function.UdtfLoader;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.rules.ExternalResource;

public class TemporaryEngine extends ExternalResource {

  public static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("val"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("val2"), SqlTypes.decimal(2, 1))
      .valueColumn(ColumnName.of("ADDRESS"), SqlTypes.struct()
          .field("STREET", SqlTypes.STRING)
          .field("STATE", SqlTypes.STRING)
          .build())
      .build();

  private MutableMetaStore metaStore;

  private KsqlConfig ksqlConfig;
  private KsqlEngine engine;
  private ServiceContext serviceContext;
  private Map<String, Object> configs = ImmutableMap.of();

  @Override
  protected void before() {
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    metaStore = new MetaStoreImpl(functionRegistry);

    serviceContext = TestServiceContext.create();
    engine = (KsqlEngineTestUtil.createKsqlEngine(getServiceContext(), metaStore));

    ksqlConfig = KsqlConfigTestUtil.create(
        "localhost:9092",
        ImmutableMap.<String, Object>builder()
            .putAll(configs)
            .put("ksql.command.topic.suffix", "commands")
            .put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
            .build()
    );

    final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);
    final Optional<Metrics> noMetrics = Optional.empty();
    final UdfLoader udfLoader = new UdfLoader(functionRegistry, noMetrics, typeParser, true);
    udfLoader.loadUdfFromClass(TestUdf1.class, "test");
    final UdtfLoader udtfLoader = new UdtfLoader(functionRegistry, noMetrics, typeParser, true);
    udtfLoader.loadUdtfFromClass(TestUdtf1.class, "whatever");
    udtfLoader.loadUdtfFromClass(TestUdtf2.class, "whatever");
  }

  public TemporaryEngine withConfigs(final Map<String, Object> configs) {
    this.configs = configs;
    return this;
  }

  @Override
  protected void after() {
    engine.close();
    serviceContext.close();
  }

  @SuppressWarnings("unchecked")
  public <T extends DataSource> T givenSource(
      final DataSourceType type,
      final String name
  ) {
    givenKafkaTopic(name);

    final KsqlTopic topic = new KsqlTopic(
        name,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
    );

    final DataSource source;
    switch (type) {
      case KSTREAM:
        source =
            new KsqlStream<>(
                "statement",
                SourceName.of(name),
                SCHEMA,
                SerdeOptions.of(),
                Optional.empty(),
                false,
                topic
            );
        break;
      case KTABLE:
        source =
            new KsqlTable<>(
                "statement",
                SourceName.of(name),
                SCHEMA,
                SerdeOptions.of(),
                Optional.empty(),
                false,
                topic
            );
        break;
      default:
        throw new IllegalArgumentException(type.toString());
    }
    metaStore.putSource(source, false);

    return (T) source;
  }

  public void givenKafkaTopic(final String name) {
    ((FakeKafkaTopicClient) getServiceContext().getTopicClient())
        .preconditionTopicExists(name, 1, (short) 1, Collections.emptyMap());
  }

  public ConfiguredStatement<?> configure(final String sql) {
    return ConfiguredStatement.of(
        getEngine().prepare(new DefaultKsqlParser().parse(sql).get(0)),
        new HashMap<>(),
        ksqlConfig);
  }

  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public KsqlEngine getEngine() {
    return engine;
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  @SuppressWarnings({"MethodMayBeStatic", "unused"}) // Invoked via reflection
  @UdtfDescription(name = "test_udtf1", description = "test_udtf1 description")
  public static class TestUdtf1 {

    @Udtf(description = "test_udtf1 int")
    public List<Integer> foo1(@UdfParameter(value = "foo") final int foo) {
      return ImmutableList.of(1);
    }

    @Udtf(description = "test_udtf1 double")
    public List<Double> foo2(@UdfParameter(value = "foo") final double foo) {
      return ImmutableList.of(1.0d);
    }
  }

  @SuppressWarnings({"MethodMayBeStatic", "unused"}) // Invoked via reflection
  @UdtfDescription(name = "test_udtf2", description = "test_udtf2 description")
  public static class TestUdtf2 {

    @Udtf
    public List<Integer> foo1(@UdfParameter(value = "foo") final int foo) {
      return ImmutableList.of(1);
    }

    @Udtf
    public List<Double> foo2(@UdfParameter(value = "foo") final double foo) {
      return ImmutableList.of(1.0d);
    }
  }

  @UdfDescription(
      name = "test_udf_1",
      description = "description for test_udf_1")
  public static class TestUdf1 {

    @Udf
    public Integer doUdf1Int(
        @UdfParameter(value = "foo", description = "the int param") final int foo) {
      return 1;
    }

    @Udtf
    public Double doUdf1Double(
        @UdfParameter(value = "foo", description = "the double param") final double foo) {
      return 1.0d;
    }
  }


}
