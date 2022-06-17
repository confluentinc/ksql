package io.confluent.ksql.test.tools;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import java.io.PrintWriter;
import java.util.Optional;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;

public class CalcitePlannerTest {

  private String schemaName;
  private ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables;
  private Planner planner;


  public static class Pantalones {

    public final String id;
    public final String size;
    public final Integer waist;

    public Pantalones(final String id, final String size, final Integer waist) {
      this.id = id;
      this.size = size;
      this.waist = waist;
    }

    public String getId() {
      return id;
    }

    public String getSize() {
      return size;
    }

    public Integer getWaist() {
      return waist;
    }
  }

  public static class Abrigos {

    public final String id;
    public final Integer sleeve;

    public Abrigos(final String id, final Integer sleeve) {
      this.id = id;
      this.sleeve = sleeve;
    }

    public String getId() {
      return id;
    }

    public Integer getSleeve() {
      return sleeve;
    }
  }

  public static class TestSchema {

    public final Pantalones[] pantalones = {
        new Pantalones("asdf", "m", 3)
    };
    public final Abrigos[] abrigos = {
        new Abrigos("345", 12)
    };
  }

  @Before
  public void beforeClass() {
    schemaName = "schema";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    final ReflectiveSchema reflectiveSchema = new ReflectiveSchema(new TestSchema());
    final SchemaPlus schema = rootSchema.add(schemaName, reflectiveSchema);

    final Config parserConfig = SqlParser.config().withCaseSensitive(false);

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .build();

    this.planner = Frameworks.getPlanner(config);

    final LogicalSchema.Builder pantalonesKsqlSchema = LogicalSchema.builder();
    pantalonesKsqlSchema.keyColumn(ColumnName.of("id"), SqlPrimitiveType.of(SqlBaseType.STRING));
    pantalonesKsqlSchema.valueColumn(ColumnName.of("size"),
        SqlPrimitiveType.of(SqlBaseType.STRING));
    pantalonesKsqlSchema.valueColumn(ColumnName.of("waist"),
        SqlPrimitiveType.of(SqlBaseType.INTEGER));
    final KsqlTable<?> pantalonesKsqlTable = new KsqlTable<>(
        "is this the table definition?",
        SourceName.of("pantalones"),
        pantalonesKsqlSchema.build(),
        Optional.empty(),
        false,
        new KsqlTopic(
            "pantalones-topic",
            KeyFormat.nonWindowed(FormatInfo.of("JSON"), SerdeFeatures.of()),
            ValueFormat.of(FormatInfo.of("JSON"), SerdeFeatures.of())
        ),
        true
    );
    ksqlTables = ImmutableMap.of(
        new SchemaTableName(schemaName, "pantalones"), pantalonesKsqlTable
    );
  }

  @Test
  public void selectStarFrom()
      throws SqlParseException, ValidationException, RelConversionException {
    final SqlNode parsed = planner.parse("SELECT * FROM Pantalones");
    System.out.println(parsed);
    final SqlNode validated = planner.validate(parsed);
    System.out.println(validated);
    final RelRoot logicalPlan = planner.rel(validated);
    final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out),
        SqlExplainLevel.ALL_ATTRIBUTES, false);
    logicalPlan.project().explain(relWriter);

//    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
//    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
//    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
//    System.out.println(topology.describe());
  }

  @Test
  public void selectFieldsFrom()
      throws SqlParseException, ValidationException, RelConversionException {
    final SqlNode parsed = planner.parse("SELECT id, waist FROM Pantalones");
    System.out.println(parsed);
    final SqlNode validated = planner.validate(parsed);
    System.out.println(validated);
    final RelRoot logicalPlan = planner.rel(validated);
    final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out),
        SqlExplainLevel.ALL_ATTRIBUTES, false);
    logicalPlan.project().explain(relWriter);

    final PlanBuildContext planBuildContext = CalcitePlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = CalcitePlanner.physicalPlan(logicalPlan.project(), ksqlTables, planBuildContext);
    final Topology topology = CalcitePlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }

}
