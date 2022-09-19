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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // todo failure to run a bunch of these tests
        OptionsTest.class,
        RemoteServerSpecificCommandTest.class,
        RequestPipeliningCommandTest.class,
        RunScriptTest.class,
        PropertiesListTableBuilderTest.class,
        QueriesTableBuilderTest.class,
        CommentStripperTest.class,
//        ConsoleTest.class,
        JLineReaderTest.class,
        JLineTerminalTest.class,
        KsqlLineParserTest.class,
        OutputFormatTest.class,
        TrimmingParserTest.class,
        UnclosedQuoteCheckerTest.class,
//        BasicAuthFunctionalTest.class,
//        CliTest.class,
        ImmutabilityTest.class,
//        SslClientAuthFunctionalTest.class,
//        SslFunctionalTest.class,
        KsqlTest.class,
})
public class MyTestsCli {
}
