package io.confluent.ksql.test.tools;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
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
import java.util.Collection;
import java.util.Optional;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.schema.SchemaPlus;
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
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CalcitePlannerTest {
  public static String[] queries = new String[] {
      "SELECT * FROM Pantalones",
      "SELECT id, waist FROM Pantalones",
      "SELECT * FROM Pantalones WHERE ID = '5'",
      "SELECT * FROM Pantalones WHERE ID IN ('5', 'XYZ')",
      "SELECT * FROM Pantalones WHERE Waist > 32",
      "SELECT * FROM Pantalones JOIN Abrigos A ON Pantalones.ID = A.ID",
      "SELECT Pantalones.ID, Waist, Size, Sleeve FROM Pantalones JOIN Abrigos A ON Pantalones.ID = A.ID",
      "SELECT * FROM Pantalones JOIN Abrigos A ON Pantalones.ID > A.ID",
      "CREATE TABLE asdf AS SELECT * FROM Pantalones",
      "SELECT ID, CONCAT(ID, '+'), Waist * 2 FROM Pantalones",
      "(SELECT ID, Waist AS Num FROM Pantalones) UNION (SELECT ID, Sleeve as Num FROM Abrigos)",
      "SELECT ID1, Waist, Size FROM (SELECT ID AS ID1, Waist FROM Pantalones) JOIN (SELECT ID AS ID2, Size FROM Pantalones) ON ID1 = ID2",
      // algebraic canonicalization
      "SELECT * FROM (SELECT * FROM Pantalones)",
      "SELECT id, size FROM (SELECT id, waist, size FROM (SELECT * FROM Pantalones))",
  };

  private String schemaName;
  private Planner planner;
  private ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables;

  private final String sql;

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
  public void before() {
    schemaName = "schema";

    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    final ReflectiveSchema reflectiveSchema = new ReflectiveSchema(new TestSchema());
    final SchemaPlus schema = rootSchema.add(schemaName, reflectiveSchema);

    final Config parserConfig = SqlParser.config().withCaseSensitive(false);

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(schema)
        .build();

    planner = Frameworks.getPlanner(config);

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
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final Builder<Object[]> builder = ImmutableList.builder();
    for (final String query : queries) {
      builder.add(new Object[]{query});
    }
    return builder.build();
  }

  public CalcitePlannerTest(final String sql){
    this.sql = sql;
  }

  @Test
  public void logical()
      throws SqlParseException, ValidationException, RelConversionException {
    final RelRoot logicalPlan = getLogicalPlan(sql);

    final RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out),
        SqlExplainLevel.ALL_ATTRIBUTES, false);

    System.out.println("{");
    logicalPlan.project().explain(relWriter);
    System.out.println("}");
  }

  @Test
  public void physical()
      throws SqlParseException, ValidationException, RelConversionException {
    final RelRoot logicalPlan = getLogicalPlan(sql);

    final Topology topology = getPhysicalPlan(logicalPlan);
    System.out.println(topology.describe());
  }

  private Topology getPhysicalPlan(final RelRoot logicalPlan) {
    final PlanBuildContext planBuildContext = CalcitePlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = CalcitePlanner.physicalPlan(
        logicalPlan.project(),
        ksqlTables,
        planBuildContext
    );
    final Topology topology = CalcitePlanner.getTopology(planBuildContext, physicalPlan);
    return topology;
  }

  @NotNull
  private RelRoot getLogicalPlan(final String sql)
      throws SqlParseException, ValidationException, RelConversionException {
    final SqlNode parsed = planner.parse(sql);
    System.out.println(parsed);
    final SqlNode validated = planner.validate(parsed);
    System.out.println(validated);
    final RelRoot logicalPlan = planner.rel(validated);
    return logicalPlan;
  }

}
