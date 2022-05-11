/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.jdbc;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.impl.ColumnTypeImpl;
import io.confluent.ksql.api.client.impl.RowImpl;
import io.vertx.core.json.JsonArray;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KsqlDatabaseMetaData implements DatabaseMetaData {

  private final Client client;

  public KsqlDatabaseMetaData(final Client client) {
    System.out.println("KsqlDatabaseMetaData" + " KsqlDatabaseMetaData(final Client client) {");
    this.client = client;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " allProceduresAreCallable()");
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " allTablesAreSelectable()");
    return false;
  }

  @Override
  public String getURL() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getURL()");
    return null;
  }

  @Override
  public String getUserName() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getUserName()");
    return null;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " isReadOnly()");
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " nullsAreSortedHigh()");
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " nullsAreSortedLow()");
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " nullsAreSortedAtStart()");
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " nullsAreSortedAtEnd()");
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getDatabaseProductName()");
    return "ksqlDB";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getDatabaseProductVersion()");
    return "0.26";
  }

  @Override
  public String getDriverName() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getDriverName()");
    return "ksqldb-jdbc";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getDriverVersion()");
    return "0.1";
  }

  @Override
  public int getDriverMajorVersion() {
    System.out.println("KsqlDatabaseMetaData" + " int getDriverMajorVersion() {");
    return 0;
  }

  @Override
  public int getDriverMinorVersion() {
    System.out.println("KsqlDatabaseMetaData" + " int getDriverMinorVersion() {");
    return 0;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " usesLocalFiles()");
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " usesLocalFilePerTable()");
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMixedCaseIdentifiers()");
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesUpperCaseIdentifiers()");
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesLowerCaseIdentifiers()");
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesMixedCaseIdentifiers()");
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMixedCaseQuotedIdentifiers()");
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesUpperCaseQuotedIdentifiers()");
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesLowerCaseQuotedIdentifiers()");
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " storesMixedCaseQuotedIdentifiers()");
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getIdentifierQuoteString()");
    return null;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getSQLKeywords()");
    return null;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getNumericFunctions()");
    return null;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getStringFunctions()");
    return null;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getSystemFunctions()");
    return null;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getTimeDateFunctions()");
    return null;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getSearchStringEscape()");
    return null;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getExtraNameCharacters()");
    return null;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsAlterTableWithAddColumn()");
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsAlterTableWithDropColumn()");
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsColumnAliasing()");
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " nullPlusNonNullIsNull()");
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsConvert()");
    return false;
  }

  @Override
  public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " supportsConvert(final int fromType, final int toType)");
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsTableCorrelationNames()");
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsDifferentTableCorrelationNames()");
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsExpressionsInOrderBy()");
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOrderByUnrelated()");
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsGroupBy()");
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsGroupByUnrelated()");
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsGroupByBeyondSelect()");
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsLikeEscapeClause()");
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMultipleResultSets()");
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMultipleTransactions()");
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsNonNullableColumns()");
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMinimumSQLGrammar()");
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCoreSQLGrammar()");
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsExtendedSQLGrammar()");
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsANSI92EntryLevelSQL()");
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsANSI92IntermediateSQL()");
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsANSI92FullSQL()");
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsIntegrityEnhancementFacility()");
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOuterJoins()");
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsFullOuterJoins()");
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsLimitedOuterJoins()");
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getSchemaTerm()");
    return null;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getProcedureTerm()");
    return null;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getCatalogTerm()");
    return null;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " isCatalogAtStart()");
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " String getCatalogSeparator()");
    return null;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSchemasInDataManipulation()");
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSchemasInProcedureCalls()");
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSchemasInTableDefinitions()");
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSchemasInIndexDefinitions()");
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSchemasInPrivilegeDefinitions()");
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCatalogsInDataManipulation()");
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCatalogsInProcedureCalls()");
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCatalogsInTableDefinitions()");
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCatalogsInIndexDefinitions()");
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCatalogsInPrivilegeDefinitions()");
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsPositionedDelete()");
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsPositionedUpdate()");
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSelectForUpdate()");
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsStoredProcedures()");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSubqueriesInComparisons()");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSubqueriesInExists()");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSubqueriesInIns()");
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSubqueriesInQuantifieds()");
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsCorrelatedSubqueries()");
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsUnion()");
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsUnionAll()");
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOpenCursorsAcrossCommit()");
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOpenCursorsAcrossRollback()");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOpenStatementsAcrossCommit()");
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsOpenStatementsAcrossRollback()");
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxBinaryLiteralLength()");
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxCharLiteralLength()");
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnNameLength()");
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnsInGroupBy()");
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnsInIndex()");
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnsInOrderBy()");
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnsInSelect()");
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxColumnsInTable()");
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxConnections()");
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxCursorNameLength()");
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxIndexLength()");
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxSchemaNameLength()");
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxProcedureNameLength()");
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxCatalogNameLength()");
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxRowSize()");
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " doesMaxRowSizeIncludeBlobs()");
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxStatementLength()");
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxStatements()");
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxTableNameLength()");
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxTablesInSelect()");
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getMaxUserNameLength()");
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getDefaultTransactionIsolation()");
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsTransactions()");
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " supportsTransactionIsolationLevel(final int level)");
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " supportsDataDefinitionAndDataManipulationTransactions()");
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsDataManipulationTransactionsOnly()");
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " dataDefinitionCausesTransactionCommit()");
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " dataDefinitionIgnoredInTransactions()");
    return false;
  }

  @Override
  public ResultSet getProcedures(final String catalog, final String schemaPattern,
      final String procedureNamePattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getProcedures(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
      final String procedureNamePattern, final String columnNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getProcedureColumns(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getTables(final String catalog, final String schemaPattern,
      final String tableNamePattern,
      final String[] types) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getTables(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ResultSet getSchemas()");

    try {
      final List<String> columnNames = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
      final List<ColumnType> columnTypes = Arrays.asList(new ColumnTypeImpl("STRING"),
          new ColumnTypeImpl("STRING"));
      final Map<String, Integer> columnNameToIndex = new HashMap<>();
      // JNH: I'm not sure about the indexing here.
      columnNameToIndex.put(columnNames.get(0), 1);
      columnNameToIndex.put(columnNames.get(1), 2);

      final List<String> streamNames =
          client.listStreams().get().stream().map(StreamInfo::getName).collect(
              Collectors.toList());
      final List<String> tableNames =
          client.listTables().get().stream().map(TableInfo::getName).collect(
              Collectors.toList());
      final List<Row> rows = Stream.concat(streamNames.stream(), tableNames.stream()).map(s ->
              new RowImpl(columnNames, columnTypes, new JsonArray(Arrays.asList(s, "default")),
                  columnNameToIndex))
          .collect(Collectors.toList());
      for (Row row : rows) {
        System.out.println("\tGet schema returning schema named: " + row.getString(1));
      }
      return new KsqlResultSet(rows);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getSchemas(final String catalog, final String schemaPattern)");
    return null;
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ResultSet getCatalogs()");
    System.out.println("In getCatalogs");
    final Map<String, Integer> columnNameToIndex = new HashMap<>();
    columnNameToIndex.put("catalog", 0);
    final Row row = new RowImpl(Collections.singletonList("catalog"),
        Collections.singletonList(new ColumnTypeImpl("STRING")),
        new JsonArray(Collections.singletonList("default")),
        columnNameToIndex
    );
    return new KsqlResultSet(Collections.singletonList(row));
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ResultSet getTableTypes()");
    return null;
  }

  @Override
  public ResultSet getColumns(final String catalog, final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getColumns(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getColumnPrivileges(final String catalog, final String schema,
      final String table,
      final String columnNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getColumnPrivileges(final String catalog, final String schema,");
    return null;
  }

  @Override
  public ResultSet getTablePrivileges(final String catalog, final String schemaPattern,
      final String tableNamePattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getTablePrivileges(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getBestRowIdentifier(final String catalog, final String schema,
      final String table, final int scope,
      final boolean nullable) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getBestRowIdentifier(final String catalog, final String schema,");
    return null;
  }

  @Override
  public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getVersionColumns(final String catalog, "
        + "final String schema, final String table)");
    return null;
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getPrimaryKeys(final String catalog, "
        + "final String schema, final String table)");
    return null;
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getImportedKeys(final String catalog, "
        + "final String schema, final String table)");
    return null;
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getExportedKeys(final String catalog, "
        + "final String schema, final String table)");
    return null;
  }

  @Override
  public ResultSet getCrossReference(final String parentCatalog, final String parentSchema,
      final String parentTable,
      final String foreignCatalog, final String foreignSchema, final String foreignTable)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getCrossReference(final String parentCatalog, final String parentSchema,");
    return null;
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ResultSet getTypeInfo()");
    return null;
  }

  @Override
  public ResultSet getIndexInfo(final String catalog, final String schema, final String table,
      final boolean unique,
      final boolean approximate) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getIndexInfo(final String catalog, final String schema, final String table,");
    return null;
  }

  @Override
  public boolean supportsResultSetType(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsResultSetType(final int type)");
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(final int type, final int concurrency)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " supportsResultSetConcurrency(final int type, final int concurrency)");
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ownUpdatesAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ownDeletesAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ownInsertsAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " othersUpdatesAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " othersDeletesAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " othersInsertsAreVisible(final int type)");
    return false;
  }

  @Override
  public boolean updatesAreDetected(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " updatesAreDetected(final int type)");
    return false;
  }

  @Override
  public boolean deletesAreDetected(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " deletesAreDetected(final int type)");
    return false;
  }

  @Override
  public boolean insertsAreDetected(final int type) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " insertsAreDetected(final int type)");
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsBatchUpdates()");
    return false;
  }

  @Override
  public ResultSet getUDTs(final String catalog, final String schemaPattern,
      final String typeNamePattern,
      final int[] types) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getUDTs(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " Connection getConnection()");
    return null;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsSavepoints()");
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsNamedParameters()");
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsMultipleOpenResults()");
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsGetGeneratedKeys()");
    return false;
  }

  @Override
  public ResultSet getSuperTypes(final String catalog, final String schemaPattern,
      final String typeNamePattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getSuperTypes(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getSuperTables(final String catalog, final String schemaPattern,
      final String tableNamePattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getSuperTables(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getAttributes(final String catalog, final String schemaPattern,
      final String typeNamePattern,
      final String attributeNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getAttributes(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
    System.out.println(
        "KsqlDatabaseMetaData" + " supportsResultSetHoldability(final int holdability)");
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getResultSetHoldability()");
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getDatabaseMajorVersion()");
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getDatabaseMinorVersion()");
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getJDBCMajorVersion()");
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getJDBCMinorVersion()");
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " int getSQLStateType()");
    return 0;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " locatorsUpdateCopy()");
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsStatementPooling()");
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " RowIdLifetime getRowIdLifetime()");
    return null;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " supportsStoredFunctionsUsingCallSyntax()");
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " autoCommitFailureClosesAllResultSets()");
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " ResultSet getClientInfoProperties()");
    return null;
  }

  @Override
  public ResultSet getFunctions(final String catalog, final String schemaPattern,
      final String functionNamePattern)
      throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getFunctions(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(final String catalog, final String schemaPattern,
      final String functionNamePattern, final String columnNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getFunctionColumns(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(final String catalog, final String schemaPattern,
      final String tableNamePattern,
      final String columnNamePattern) throws SQLException {
    System.out.println("KsqlDatabaseMetaData"
        + " ResultSet getPseudoColumns(final String catalog, final String schemaPattern,");
    return null;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " generatedKeyAlwaysReturned()");
    return false;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " <T> T unwrap(final Class<T> iface)");
    return null;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    System.out.println("KsqlDatabaseMetaData" + " isWrapperFor(final Class<?> iface)");
    return false;
  }
}
