package io.confluent.ksql;

import io.confluent.ksql.cli.BasicAuthFunctionalTest;
import io.confluent.ksql.cli.CliTest;
import io.confluent.ksql.cli.ImmutabilityTest;
import io.confluent.ksql.cli.SslClientAuthFunctionalTest;
import io.confluent.ksql.cli.SslFunctionalTest;
import io.confluent.ksql.cli.commands.OptionsTest;
import io.confluent.ksql.cli.console.CommentStripperTest;
import io.confluent.ksql.cli.console.ConsoleTest;
import io.confluent.ksql.cli.console.JLineReaderTest;
import io.confluent.ksql.cli.console.JLineTerminalTest;
import io.confluent.ksql.cli.console.KsqlLineParserTest;
import io.confluent.ksql.cli.console.OutputFormatTest;
import io.confluent.ksql.cli.console.TrimmingParserTest;
import io.confluent.ksql.cli.console.UnclosedQuoteCheckerTest;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommandTest;
import io.confluent.ksql.cli.console.cmd.RequestPipeliningCommandTest;
import io.confluent.ksql.cli.console.cmd.RunScriptTest;
import io.confluent.ksql.cli.console.table.builder.PropertiesListTableBuilderTest;
import io.confluent.ksql.cli.console.table.builder.QueriesTableBuilderTest;
import io.confluent.ksql.util.CmdLineUtilTest;
import io.confluent.ksql.util.TabularRowTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OptionsTest.class,
        RemoteServerSpecificCommandTest.class,
        RequestPipeliningCommandTest.class,
        RunScriptTest.class,
        PropertiesListTableBuilderTest.class,
        QueriesTableBuilderTest.class,
        CommentStripperTest.class,
// approvaltests not running properly
//        ConsoleTest.class,
        JLineReaderTest.class,
        JLineTerminalTest.class,
        KsqlLineParserTest.class,
        OutputFormatTest.class,
        TrimmingParserTest.class,
        UnclosedQuoteCheckerTest.class,
//        Bazel doesn't support custom security managers, so these need to be re-worked.
//        BasicAuthFunctionalTest.class,
//        CliTest.class,
        ImmutabilityTest.class,
//        SslClientAuthFunctionalTest.class,
//        SslFunctionalTest.class,
        CmdLineUtilTest.class,
        TabularRowTest.class,
        KsqlTest.class,
})
public class MyTestsCli {
}
