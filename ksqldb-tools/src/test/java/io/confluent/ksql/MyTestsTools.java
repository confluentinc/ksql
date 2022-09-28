package io.confluent.ksql;

import io.confluent.ksql.tools.migrations.MigrationsParsingTest;
import io.confluent.ksql.tools.migrations.MigrationsTest;
import io.confluent.ksql.tools.migrations.commands.ApplyMigrationCommandTest;
import io.confluent.ksql.tools.migrations.commands.CreateMigrationCommandTest;
import io.confluent.ksql.tools.migrations.commands.DestroyMigrationsCommandTest;
import io.confluent.ksql.tools.migrations.commands.InitializeMigrationCommandTest;
import io.confluent.ksql.tools.migrations.commands.MigrationInfoCommandTest;
import io.confluent.ksql.tools.migrations.commands.NewMigrationCommandTest;
import io.confluent.ksql.tools.migrations.commands.ValidateMigrationsCommandTest;
import io.confluent.ksql.tools.migrations.util.CommandParserTest;
import io.confluent.ksql.tools.migrations.util.MigrationVersionInfoFormatterTest;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtilTest;
import io.confluent.ksql.tools.migrations.util.MigrationsUtilTest;
import io.confluent.ksql.tools.migrations.util.ServerVersionUtilTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ApplyMigrationCommandTest.class,
        CreateMigrationCommandTest.class,
        DestroyMigrationsCommandTest.class,
        InitializeMigrationCommandTest.class,
//        MigrationInfoCommandTest.class,
        NewMigrationCommandTest.class,
        ValidateMigrationsCommandTest.class,
        CommandParserTest.class,
        MigrationsDirectoryUtilTest.class,
        MigrationsUtilTest.class,
        MigrationVersionInfoFormatterTest.class,
        ServerVersionUtilTest.class,
        MigrationsParsingTest.class,
//        Bazel doesn't support custom security managers, so these need to be re-worked.
//        MigrationsTest.class
})
public class MyTestsTools {
}
