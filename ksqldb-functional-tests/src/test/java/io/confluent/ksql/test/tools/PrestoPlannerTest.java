package io.confluent.ksql.test.tools;

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableList;
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
import io.confluent.ksql.test.tools.PrestoPlanner.ProtoConnector;
import io.confluent.ksql.test.tools.PrestoPlanner.Table;
import java.util.Optional;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;

public class PrestoPlannerTest {

  private String schemaName;
  private Connector connector;
  private ImmutableMap<SchemaTableName, ? extends KsqlTable<?>> ksqlTables;

  @Before
  public void beforeClass() {
    schemaName = "schema";

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

    final Table pantalonesPrestoTable = new Table(
        "pantalones",
        schemaName,
        ImmutableList.of(
            ColumnMetadata
                .builder()
                .setName("id")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build(),
            ColumnMetadata
                .builder()
                .setName("size")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build(),
            ColumnMetadata
                .builder()
                .setName("waist")
                .setType(IntegerType.INTEGER)
                .setNullable(false)
                .build()
        )
    );
    final Table abrigosTable = new Table(
        "abrigos",
        schemaName,
        ImmutableList.of(
            ColumnMetadata
                .builder()
                .setName("id")
                .setType(VarcharType.createUnboundedVarcharType())
                .setNullable(false)
                .build(),
            ColumnMetadata
                .builder()
                .setName("sleeve")
                .setType(IntegerType.INTEGER)
                .setNullable(false)
                .build()
        )
    );
    connector = new ProtoConnector(
        ImmutableList.of(
            pantalonesPrestoTable, abrigosTable
        )
    );
  }

  @Test public void selectStarFrom() {
    final Plan prestoPlan = PrestoPlanner.logicalPlan(
        "SELECT * FROM Pantalones",
        schemaName,
        connector
    );

    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }

  @Test public void selectFieldsFrom() {
    final Plan prestoPlan = PrestoPlanner.logicalPlan(
        "SELECT ID, Size FROM Pantalones",
        schemaName,
        connector
    );

    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }

  @Test public void selectStarFromWhere() {
    final Plan prestoPlan = PrestoPlanner.logicalPlan(
        "SELECT * FROM Pantalones WHERE ID = '5'",
        schemaName,
        connector
    );

    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }

  @Test public void selectStarFromWhereIn() {
    final Plan prestoPlan = PrestoPlanner.logicalPlan(
        "SELECT * FROM Pantalones WHERE ID IN ('5', 'XYZ')",
        schemaName,
        connector
    );

    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }

  @Test public void selectStarFromWhereGreater() {
    final Plan prestoPlan = PrestoPlanner.logicalPlan(
        "SELECT * FROM Pantalones WHERE Waist > 32",
        schemaName,
        connector
    );

    final PlanBuildContext planBuildContext = PrestoPlanner.getPlanBuildContext();
    final SchemaKStream<?> physicalPlan = PrestoPlanner.physicalPlan(prestoPlan, ksqlTables, planBuildContext);
    final Topology topology = PrestoPlanner.getTopology(planBuildContext, physicalPlan);
    System.out.println(topology.describe());
  }
}
