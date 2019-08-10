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

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.test.model.InputRecordsNode;
import io.confluent.ksql.test.model.OutputRecordsNode;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TestCaseNode;
import io.confluent.ksql.test.tools.command.TestOptions;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class KsqlTestingTool {

  private KsqlTestingTool() {

  }

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  public static void main(final String[] args) throws Exception {

    try {
      final TestOptions testOptions = TestOptions.parse(args);
      if (testOptions == null) {
        return;
      }
      if (testOptions.getStatementsFile() != null
          && testOptions.getOutputFile() != null) {
        runWithTripleFiles(
            testOptions.getStatementsFile(),
            testOptions.getInputFile(),
            testOptions.getOutputFile());
      }
    } catch (final Exception e) {
      System.err.println("Invalid arguments: " + e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private static List<RecordNode> getRecordNodesFromFile(final String filePath) throws IOException {
    return OBJECT_MAPPER.readValue(new File(filePath), List.class);
  }

  private static List<String> getSqlStatements(final String queryFilePath) {
    try {
      final String sqlStatements = new String(java.nio.file.Files.readAllBytes(
          Paths.get(queryFilePath)), StandardCharsets.UTF_8);

      final KsqlParser ksqlParser = new DefaultKsqlParser();
      final List<ParsedStatement> parsedStatements = ksqlParser.parse(sqlStatements);
      return parsedStatements
          .stream()
          .map(ParsedStatement::getStatementText)
          .collect(Collectors.toList());
    } catch (final IOException e) {
      throw new KsqlException(
          String.format("Could not read the query file: %s. Details: %s",
              queryFilePath, e.getMessage()),
          e);
    }
  }


  static void runWithTripleFiles(
      final String statementFile,
      final String inputFile,
      final String outputFile) throws Exception {
    final InputRecordsNode inputRecordNodes;
    final OutputRecordsNode outRecordNodes;
    try {
      inputRecordNodes = (inputFile == null) ? null : OBJECT_MAPPER
          .readValue(new File(inputFile), InputRecordsNode.class);
    } catch (final Exception inputException) {
      throw new Exception("File name: " + inputFile + " Message: " + inputException.getMessage());
    }
    try {
      outRecordNodes = OBJECT_MAPPER
          .readValue(new File(outputFile), OutputRecordsNode.class);
    } catch (final Exception outputException) {
      throw new Exception("File name: " + outputFile
          + " Message: " + outputException.getMessage());
    }

    final List<String> statements = getSqlStatements(statementFile);

    final TestCaseNode testCaseNode = new TestCaseNode(
        "KSQL_Test",
        null,
        (inputFile == null) ? null : inputRecordNodes.getInputRecords(),
        outRecordNodes.getOutputRecords(),
        Collections.emptyList(),
        statements,
        null,
        null,
        null,
        true
    );

    final TestCase testCase = testCaseNode.buildTests(
        new File(statementFile).toPath(),
        TestFunctionRegistry.INSTANCE.get())
        .get(0);

    executeTestCase(
        testCase,
        new TestExecutor());

  }

  static void executeTestCase(
      final TestCase testCase,
      final TestExecutor testExecutor
  ) {
    try {
      testExecutor.buildAndExecuteQuery(testCase);
      System.out.println("\t >>> Test passed!");
    } catch (final Exception e) {
      System.err.println("\t>>>>> Test failed: " + e.getMessage());
    } finally {
      testExecutor.close();
    }
  }
}