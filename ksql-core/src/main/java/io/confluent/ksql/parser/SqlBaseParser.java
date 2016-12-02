// Generated from SqlBase.g4 by ANTLR 4.5.3

package io.confluent.ksql.parser;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;

import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {

  static {
    RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION);
  }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
      new PredictionContextCache();
  public static final int
      T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, T__7 = 8, T__8 = 9,
      SELECT = 10, FROM = 11, ADD = 12, AS = 13, ALL = 14, SOME = 15, ANY = 16, DISTINCT = 17,
      WHERE = 18, GROUP = 19, BY = 20, GROUPING = 21, SETS = 22, CUBE = 23, ROLLUP = 24, ORDER = 25,
      HAVING = 26, LIMIT = 27, APPROXIMATE = 28, AT = 29, CONFIDENCE = 30, OR = 31, AND = 32,
      IN = 33, NOT = 34, NO = 35, EXISTS = 36, BETWEEN = 37, LIKE = 38, IS = 39, NULL = 40,
      TRUE = 41, FALSE = 42, NULLS = 43, FIRST = 44, LAST = 45, ESCAPE = 46, ASC = 47, DESC = 48,
      SUBSTRING = 49, POSITION = 50, FOR = 51, TINYINT = 52, SMALLINT = 53, INTEGER = 54,
      DATE = 55, TIME = 56, TIMESTAMP = 57, INTERVAL = 58, YEAR = 59, MONTH = 60, DAY = 61,
      HOUR = 62, MINUTE = 63, SECOND = 64, ZONE = 65, CURRENT_DATE = 66, CURRENT_TIME = 67,
      CURRENT_TIMESTAMP = 68, LOCALTIME = 69, LOCALTIMESTAMP = 70, EXTRACT = 71, CASE = 72,
      WHEN = 73, THEN = 74, ELSE = 75, END = 76, JOIN = 77, CROSS = 78, OUTER = 79, INNER = 80,
      LEFT = 81, RIGHT = 82, FULL = 83, NATURAL = 84, USING = 85, ON = 86, OVER = 87,
      PARTITION =
          88,
      RANGE = 89, ROWS = 90, UNBOUNDED = 91, PRECEDING = 92, FOLLOWING = 93, CURRENT = 94,
      ROW = 95, WITH = 96, RECURSIVE = 97, VALUES = 98, CREATE = 99, TABLE = 100, TOPIC = 101,
      STREAM = 102, VIEW = 103, REPLACE = 104, INSERT = 105, DELETE = 106, INTO = 107,
      CONSTRAINT =
          108,
      DESCRIBE = 109, PRINT = 110, GRANT = 111, REVOKE = 112, PRIVILEGES = 113, PUBLIC = 114,
      OPTION = 115, EXPLAIN = 116, ANALYZE = 117, FORMAT = 118, TYPE = 119, TEXT = 120,
      GRAPHVIZ = 121, LOGICAL = 122, DISTRIBUTED = 123, TRY = 124, CAST = 125, TRY_CAST = 126,
      SHOW = 127, TABLES = 128, TOPICS = 129, QUERIES = 130, TERMINATE = 131, LOAD = 132,
      SCHEMAS = 133, CATALOGS = 134, COLUMNS = 135, COLUMN = 136, USE = 137, PARTITIONS = 138,
      FUNCTIONS = 139, DROP = 140, UNION = 141, EXCEPT = 142, INTERSECT = 143, TO = 144,
      SYSTEM = 145, BERNOULLI = 146, POISSONIZED = 147, TABLESAMPLE = 148, RESCALED = 149,
      STRATIFY = 150, ALTER = 151, RENAME = 152, UNNEST = 153, ORDINALITY = 154, ARRAY = 155,
      MAP = 156, SET = 157, RESET = 158, SESSION = 159, DATA = 160, START = 161, TRANSACTION = 162,
      COMMIT = 163, ROLLBACK = 164, WORK = 165, ISOLATION = 166, LEVEL = 167, SERIALIZABLE = 168,
      REPEATABLE = 169, COMMITTED = 170, UNCOMMITTED = 171, READ = 172, WRITE = 173, ONLY = 174,
      CALL = 175, PREPARE = 176, DEALLOCATE = 177, EXECUTE = 178, SAMPLE = 179, NORMALIZE = 180,
      NFD = 181, NFC = 182, NFKD = 183, NFKC = 184, IF = 185, NULLIF = 186, COALESCE = 187,
      EQ = 188, NEQ = 189, LT = 190, LTE = 191, GT = 192, GTE = 193, PLUS = 194, MINUS = 195,
      ASTERISK = 196, SLASH = 197, PERCENT = 198, CONCAT = 199, STRING = 200, BINARY_LITERAL = 201,
      INTEGER_VALUE = 202, DECIMAL_VALUE = 203, IDENTIFIER = 204, DIGIT_IDENTIFIER = 205,
      QUOTED_IDENTIFIER = 206, BACKQUOTED_IDENTIFIER = 207, TIME_WITH_TIME_ZONE = 208,
      TIMESTAMP_WITH_TIME_ZONE = 209, SIMPLE_COMMENT = 210, BRACKETED_COMMENT = 211,
      WS = 212, UNRECOGNIZED = 213, DELIMITER = 214;
  public static final int
      RULE_statements = 0, RULE_singleStatement = 1, RULE_singleExpression = 2,
      RULE_statement = 3, RULE_query = 4, RULE_with = 5, RULE_tableElement = 6,
      RULE_tableProperties = 7, RULE_tableProperty = 8, RULE_queryNoWith = 9,
      RULE_queryTerm = 10, RULE_queryPrimary = 11, RULE_sortItem = 12, RULE_querySpecification = 13,
      RULE_groupBy = 14, RULE_groupingElement = 15, RULE_groupingExpressions = 16,
      RULE_groupingSet = 17, RULE_namedQuery = 18, RULE_setQuantifier = 19,
      RULE_selectItem = 20, RULE_relation = 21, RULE_joinType = 22, RULE_joinCriteria = 23,
      RULE_sampleType = 24, RULE_aliasedRelation = 25, RULE_columnAliases = 26,
      RULE_relationPrimary = 27, RULE_expression = 28, RULE_booleanExpression = 29,
      RULE_predicated = 30, RULE_predicate = 31, RULE_valueExpression = 32,
      RULE_primaryExpression = 33, RULE_timeZoneSpecifier = 34, RULE_comparisonOperator = 35,
      RULE_booleanValue = 36, RULE_interval = 37, RULE_intervalField = 38, RULE_type = 39,
      RULE_typeParameter = 40, RULE_baseType = 41, RULE_whenClause = 42, RULE_over = 43,
      RULE_windowFrame = 44, RULE_frameBound = 45, RULE_explainOption = 46,
      RULE_transactionMode = 47, RULE_levelOfIsolation = 48, RULE_callArgument = 49,
      RULE_privilege = 50, RULE_qualifiedName = 51, RULE_identifier = 52,
      RULE_quotedIdentifier =
          53,
      RULE_number = 54, RULE_nonReserved = 55, RULE_normalForm = 56;
  public static final String[] ruleNames = {
      "statements", "singleStatement", "singleExpression", "statement", "query",
      "with", "tableElement", "tableProperties", "tableProperty", "queryNoWith",
      "queryTerm", "queryPrimary", "sortItem", "querySpecification", "groupBy",
      "groupingElement", "groupingExpressions", "groupingSet", "namedQuery",
      "setQuantifier", "selectItem", "relation", "joinType", "joinCriteria",
      "sampleType", "aliasedRelation", "columnAliases", "relationPrimary", "expression",
      "booleanExpression", "predicated", "predicate", "valueExpression", "primaryExpression",
      "timeZoneSpecifier", "comparisonOperator", "booleanValue", "interval",
      "intervalField", "type", "typeParameter", "baseType", "whenClause", "over",
      "windowFrame", "frameBound", "explainOption", "transactionMode", "levelOfIsolation",
      "callArgument", "privilege", "qualifiedName", "identifier", "quotedIdentifier",
      "number", "nonReserved", "normalForm"
  };

  private static final String[] _LITERAL_NAMES = {
      null, "';'", "'('", "','", "')'", "'.'", "'->'", "'['", "']'", "'=>'",
      "'SELECT'", "'FROM'", "'ADD'", "'AS'", "'ALL'", "'SOME'", "'ANY'", "'DISTINCT'",
      "'WHERE'", "'GROUP'", "'BY'", "'GROUPING'", "'SETS'", "'CUBE'", "'ROLLUP'",
      "'ORDER'", "'HAVING'", "'LIMIT'", "'APPROXIMATE'", "'AT'", "'CONFIDENCE'",
      "'OR'", "'AND'", "'IN'", "'NOT'", "'NO'", "'EXISTS'", "'BETWEEN'", "'LIKE'",
      "'IS'", "'NULL'", "'TRUE'", "'FALSE'", "'NULLS'", "'FIRST'", "'LAST'",
      "'ESCAPE'", "'ASC'", "'DESC'", "'SUBSTRING'", "'POSITION'", "'FOR'", "'TINYINT'",
      "'SMALLINT'", "'INTEGER'", "'DATE'", "'TIME'", "'TIMESTAMP'", "'INTERVAL'",
      "'YEAR'", "'MONTH'", "'DAY'", "'HOUR'", "'MINUTE'", "'SECOND'", "'ZONE'",
      "'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'", "'LOCALTIME'",
      "'LOCALTIMESTAMP'", "'EXTRACT'", "'CASE'", "'WHEN'", "'THEN'", "'ELSE'",
      "'END'", "'JOIN'", "'CROSS'", "'OUTER'", "'INNER'", "'LEFT'", "'RIGHT'",
      "'FULL'", "'NATURAL'", "'USING'", "'ON'", "'OVER'", "'PARTITION'", "'RANGE'",
      "'ROWS'", "'UNBOUNDED'", "'PRECEDING'", "'FOLLOWING'", "'CURRENT'", "'ROW'",
      "'WITH'", "'RECURSIVE'", "'VALUES'", "'CREATE'", "'TABLE'", "'TOPIC'",
      "'STREAM'", "'VIEW'", "'REPLACE'", "'INSERT'", "'DELETE'", "'INTO'", "'CONSTRAINT'",
      "'DESCRIBE'", "'PRINT'", "'GRANT'", "'REVOKE'", "'PRIVILEGES'", "'PUBLIC'",
      "'OPTION'", "'EXPLAIN'", "'ANALYZE'", "'FORMAT'", "'TYPE'", "'TEXT'",
      "'GRAPHVIZ'", "'LOGICAL'", "'DISTRIBUTED'", "'TRY'", "'CAST'", "'TRY_CAST'",
      "'SHOW'", "'TABLES'", "'TOPICS'", "'QUERIES'", "'TERMINATE'", "'LOAD'",
      "'SCHEMAS'", "'CATALOGS'", "'COLUMNS'", "'COLUMN'", "'USE'", "'PARTITIONS'",
      "'FUNCTIONS'", "'DROP'", "'UNION'", "'EXCEPT'", "'INTERSECT'", "'TO'",
      "'SYSTEM'", "'BERNOULLI'", "'POISSONIZED'", "'TABLESAMPLE'", "'RESCALED'",
      "'STRATIFY'", "'ALTER'", "'RENAME'", "'UNNEST'", "'ORDINALITY'", "'ARRAY'",
      "'MAP'", "'SET'", "'RESET'", "'SESSION'", "'DATA'", "'START'", "'TRANSACTION'",
      "'COMMIT'", "'ROLLBACK'", "'WORK'", "'ISOLATION'", "'LEVEL'", "'SERIALIZABLE'",
      "'REPEATABLE'", "'COMMITTED'", "'UNCOMMITTED'", "'READ'", "'WRITE'", "'ONLY'",
      "'CALL'", "'PREPARE'", "'DEALLOCATE'", "'EXECUTE'", "'SAMPLE'", "'NORMALIZE'",
      "'NFD'", "'NFC'", "'NFKD'", "'NFKC'", "'IF'", "'NULLIF'", "'COALESCE'",
      "'='", null, "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'",
      "'%'", "'||'"
  };
  private static final String[] _SYMBOLIC_NAMES = {
      null, null, null, null, null, null, null, null, null, null, "SELECT",
      "FROM", "ADD", "AS", "ALL", "SOME", "ANY", "DISTINCT", "WHERE", "GROUP",
      "BY", "GROUPING", "SETS", "CUBE", "ROLLUP", "ORDER", "HAVING", "LIMIT",
      "APPROXIMATE", "AT", "CONFIDENCE", "OR", "AND", "IN", "NOT", "NO", "EXISTS",
      "BETWEEN", "LIKE", "IS", "NULL", "TRUE", "FALSE", "NULLS", "FIRST", "LAST",
      "ESCAPE", "ASC", "DESC", "SUBSTRING", "POSITION", "FOR", "TINYINT", "SMALLINT",
      "INTEGER", "DATE", "TIME", "TIMESTAMP", "INTERVAL", "YEAR", "MONTH", "DAY",
      "HOUR", "MINUTE", "SECOND", "ZONE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
      "LOCALTIME", "LOCALTIMESTAMP", "EXTRACT", "CASE", "WHEN", "THEN", "ELSE",
      "END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "RIGHT", "FULL", "NATURAL",
      "USING", "ON", "OVER", "PARTITION", "RANGE", "ROWS", "UNBOUNDED", "PRECEDING",
      "FOLLOWING", "CURRENT", "ROW", "WITH", "RECURSIVE", "VALUES", "CREATE",
      "TABLE", "TOPIC", "STREAM", "VIEW", "REPLACE", "INSERT", "DELETE", "INTO",
      "CONSTRAINT", "DESCRIBE", "PRINT", "GRANT", "REVOKE", "PRIVILEGES", "PUBLIC",
      "OPTION", "EXPLAIN", "ANALYZE", "FORMAT", "TYPE", "TEXT", "GRAPHVIZ",
      "LOGICAL", "DISTRIBUTED", "TRY", "CAST", "TRY_CAST", "SHOW", "TABLES",
      "TOPICS", "QUERIES", "TERMINATE", "LOAD", "SCHEMAS", "CATALOGS", "COLUMNS",
      "COLUMN", "USE", "PARTITIONS", "FUNCTIONS", "DROP", "UNION", "EXCEPT",
      "INTERSECT", "TO", "SYSTEM", "BERNOULLI", "POISSONIZED", "TABLESAMPLE",
      "RESCALED", "STRATIFY", "ALTER", "RENAME", "UNNEST", "ORDINALITY", "ARRAY",
      "MAP", "SET", "RESET", "SESSION", "DATA", "START", "TRANSACTION", "COMMIT",
      "ROLLBACK", "WORK", "ISOLATION", "LEVEL", "SERIALIZABLE", "REPEATABLE",
      "COMMITTED", "UNCOMMITTED", "READ", "WRITE", "ONLY", "CALL", "PREPARE",
      "DEALLOCATE", "EXECUTE", "SAMPLE", "NORMALIZE", "NFD", "NFC", "NFKD",
      "NFKC", "IF", "NULLIF", "COALESCE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE",
      "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", "STRING", "BINARY_LITERAL",
      "INTEGER_VALUE", "DECIMAL_VALUE", "IDENTIFIER", "DIGIT_IDENTIFIER", "QUOTED_IDENTIFIER",
      "BACKQUOTED_IDENTIFIER", "TIME_WITH_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE",
      "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
  };
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;

  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() {
    return "SqlBase.g4";
  }

  @Override
  public String[] getRuleNames() {
    return ruleNames;
  }

  @Override
  public String getSerializedATN() {
    return _serializedATN;
  }

  @Override
  public ATN getATN() {
    return _ATN;
  }

  public SqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
  }

  public static class StatementsContext extends ParserRuleContext {

    public List<SingleStatementContext> singleStatement() {
      return getRuleContexts(SingleStatementContext.class);
    }

    public SingleStatementContext singleStatement(int i) {
      return getRuleContext(SingleStatementContext.class, i);
    }

    public TerminalNode EOF() {
      return getToken(SqlBaseParser.EOF, 0);
    }

    public StatementsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_statements;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterStatements(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitStatements(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitStatements(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final StatementsContext statements() throws RecognitionException {
    StatementsContext _localctx = new StatementsContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_statements);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(114);
        singleStatement();
        setState(118);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__1 || _la == SELECT || ((((_la - 96)) & ~0x3f) == 0 &&
                                                ((1L << (_la - 96)) & ((1L << (WITH - 96)) | (1L
                                                                                              << (VALUES
                                                                                                  - 96))
                                                                       | (1L << (CREATE - 96)) | (1L
                                                                                                  << (TABLE
                                                                                                      - 96))
                                                                       | (1L << (DESCRIBE - 96)) | (
                                                                           1L << (PRINT - 96)) | (1L
                                                                                                  << (SHOW
                                                                                                      - 96))
                                                                       | (1L << (TERMINATE - 96))
                                                                       | (1L << (LOAD - 96)) | (1L
                                                                                                << (DROP
                                                                                                    - 96))
                                                                       | (1L << (SET - 96))))
                                                != 0)) {
          {
            {
              setState(115);
              singleStatement();
            }
          }
          setState(120);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(121);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleStatementContext extends ParserRuleContext {

    public StatementContext statement() {
      return getRuleContext(StatementContext.class, 0);
    }

    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleStatement;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSingleStatement(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSingleStatement(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSingleStatement(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(123);
        statement();
        setState(124);
        match(T__0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleExpressionContext extends ParserRuleContext {

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(SqlBaseParser.EOF, 0);
    }

    public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleExpression;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSingleExpression(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSingleExpression(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSingleExpression(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SingleExpressionContext singleExpression() throws RecognitionException {
    SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_singleExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(126);
        expression();
        setState(127);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StatementContext extends ParserRuleContext {

    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_statement;
    }

    public StatementContext() {
    }

    public void copyFrom(StatementContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class CreateTableContext extends StatementContext {

    public TerminalNode CREATE() {
      return getToken(SqlBaseParser.CREATE, 0);
    }

    public TerminalNode TOPIC() {
      return getToken(SqlBaseParser.TOPIC, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public List<TableElementContext> tableElement() {
      return getRuleContexts(TableElementContext.class);
    }

    public TableElementContext tableElement(int i) {
      return getRuleContext(TableElementContext.class, i);
    }

    public TerminalNode IF() {
      return getToken(SqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(SqlBaseParser.EXISTS, 0);
    }

    public TerminalNode WITH() {
      return getToken(SqlBaseParser.WITH, 0);
    }

    public TablePropertiesContext tableProperties() {
      return getRuleContext(TablePropertiesContext.class, 0);
    }

    public CreateTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterCreateTable(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitCreateTable(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitCreateTable(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class QuerystatementContext extends StatementContext {

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public QuerystatementContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQuerystatement(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQuerystatement(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQuerystatement(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ShowQueriesContext extends StatementContext {

    public TerminalNode SHOW() {
      return getToken(SqlBaseParser.SHOW, 0);
    }

    public TerminalNode QUERIES() {
      return getToken(SqlBaseParser.QUERIES, 0);
    }

    public ShowQueriesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterShowQueries(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitShowQueries(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitShowQueries(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SetPropertyContext extends StatementContext {

    public TerminalNode SET() {
      return getToken(SqlBaseParser.SET, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode EQ() {
      return getToken(SqlBaseParser.EQ, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public SetPropertyContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSetProperty(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSetProperty(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSetProperty(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class TerminateQueryContext extends StatementContext {

    public TerminalNode TERMINATE() {
      return getToken(SqlBaseParser.TERMINATE, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminateQueryContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTerminateQuery(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTerminateQuery(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTerminateQuery(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ShowTopicsContext extends StatementContext {

    public TerminalNode SHOW() {
      return getToken(SqlBaseParser.SHOW, 0);
    }

    public TerminalNode TOPICS() {
      return getToken(SqlBaseParser.TOPICS, 0);
    }

    public ShowTopicsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterShowTopics(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitShowTopics(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitShowTopics(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class PrintTopicContext extends StatementContext {

    public TerminalNode PRINT() {
      return getToken(SqlBaseParser.PRINT, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public NumberContext number() {
      return getRuleContext(NumberContext.class, 0);
    }

    public TerminalNode INTERVAL() {
      return getToken(SqlBaseParser.INTERVAL, 0);
    }

    public TerminalNode SAMPLE() {
      return getToken(SqlBaseParser.SAMPLE, 0);
    }

    public PrintTopicContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterPrintTopic(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitPrintTopic(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitPrintTopic(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class LoadPropertiesContext extends StatementContext {

    public TerminalNode LOAD() {
      return getToken(SqlBaseParser.LOAD, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public LoadPropertiesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterLoadProperties(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitLoadProperties(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitLoadProperties(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ShowTablesContext extends StatementContext {

    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(SqlBaseParser.SHOW, 0);
    }

    public TerminalNode TABLES() {
      return getToken(SqlBaseParser.TABLES, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode LIKE() {
      return getToken(SqlBaseParser.LIKE, 0);
    }

    public TerminalNode FROM() {
      return getToken(SqlBaseParser.FROM, 0);
    }

    public TerminalNode IN() {
      return getToken(SqlBaseParser.IN, 0);
    }

    public TerminalNode STRING() {
      return getToken(SqlBaseParser.STRING, 0);
    }

    public ShowTablesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterShowTables(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitShowTables(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitShowTables(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ShowColumnsContext extends StatementContext {

    public TerminalNode DESCRIBE() {
      return getToken(SqlBaseParser.DESCRIBE, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public ShowColumnsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterShowColumns(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitShowColumns(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitShowColumns(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class DropTableContext extends StatementContext {

    public TerminalNode DROP() {
      return getToken(SqlBaseParser.DROP, 0);
    }

    public TerminalNode TOPIC() {
      return getToken(SqlBaseParser.TOPIC, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(SqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(SqlBaseParser.EXISTS, 0);
    }

    public DropTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterDropTable(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitDropTable(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitDropTable(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_statement);
    int _la;
    try {
      setState(190);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 8, _ctx)) {
        case 1:
          _localctx = new QuerystatementContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(129);
          query();
        }
        break;
        case 2:
          _localctx = new ShowTablesContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(130);
          match(SHOW);
          setState(131);
          match(TABLES);
          setState(134);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(132);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                consume();
              }
              setState(133);
              qualifiedName();
            }
          }

          setState(138);
          _la = _input.LA(1);
          if (_la == LIKE) {
            {
              setState(136);
              match(LIKE);
              setState(137);
              ((ShowTablesContext) _localctx).pattern = match(STRING);
            }
          }

        }
        break;
        case 3:
          _localctx = new ShowTopicsContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(140);
          match(SHOW);
          setState(141);
          match(TOPICS);
        }
        break;
        case 4:
          _localctx = new ShowColumnsContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(142);
          match(DESCRIBE);
          setState(143);
          qualifiedName();
        }
        break;
        case 5:
          _localctx = new PrintTopicContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(144);
          match(PRINT);
          setState(145);
          qualifiedName();
          setState(148);
          _la = _input.LA(1);
          if (_la == INTERVAL || _la == SAMPLE) {
            {
              setState(146);
              _la = _input.LA(1);
              if (!(_la == INTERVAL || _la == SAMPLE)) {
                _errHandler.recoverInline(this);
              } else {
                consume();
              }
              setState(147);
              number();
            }
          }

        }
        break;
        case 6:
          _localctx = new ShowQueriesContext(_localctx);
          enterOuterAlt(_localctx, 6);
        {
          setState(150);
          match(SHOW);
          setState(151);
          match(QUERIES);
        }
        break;
        case 7:
          _localctx = new TerminateQueryContext(_localctx);
          enterOuterAlt(_localctx, 7);
        {
          setState(152);
          match(TERMINATE);
          setState(153);
          qualifiedName();
        }
        break;
        case 8:
          _localctx = new SetPropertyContext(_localctx);
          enterOuterAlt(_localctx, 8);
        {
          setState(154);
          match(SET);
          setState(155);
          qualifiedName();
          setState(156);
          match(EQ);
          setState(157);
          expression();
        }
        break;
        case 9:
          _localctx = new LoadPropertiesContext(_localctx);
          enterOuterAlt(_localctx, 9);
        {
          setState(159);
          match(LOAD);
          setState(160);
          expression();
        }
        break;
        case 10:
          _localctx = new CreateTableContext(_localctx);
          enterOuterAlt(_localctx, 10);
        {
          setState(161);
          match(CREATE);
          setState(162);
          match(TOPIC);
          setState(166);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 4, _ctx)) {
            case 1: {
              setState(163);
              match(IF);
              setState(164);
              match(NOT);
              setState(165);
              match(EXISTS);
            }
            break;
          }
          setState(168);
          qualifiedName();
          setState(169);
          match(T__1);
          setState(170);
          tableElement();
          setState(175);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__2) {
            {
              {
                setState(171);
                match(T__2);
                setState(172);
                tableElement();
              }
            }
            setState(177);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(178);
          match(T__3);
          setState(181);
          _la = _input.LA(1);
          if (_la == WITH) {
            {
              setState(179);
              match(WITH);
              setState(180);
              tableProperties();
            }
          }

        }
        break;
        case 11:
          _localctx = new DropTableContext(_localctx);
          enterOuterAlt(_localctx, 11);
        {
          setState(183);
          match(DROP);
          setState(184);
          match(TOPIC);
          setState(187);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 7, _ctx)) {
            case 1: {
              setState(185);
              match(IF);
              setState(186);
              match(EXISTS);
            }
            break;
          }
          setState(189);
          qualifiedName();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryContext extends ParserRuleContext {

    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class, 0);
    }

    public WithContext with() {
      return getRuleContext(WithContext.class, 0);
    }

    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_query;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQuery(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQuery(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQuery(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QueryContext query() throws RecognitionException {
    QueryContext _localctx = new QueryContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_query);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(193);
        _la = _input.LA(1);
        if (_la == WITH) {
          {
            setState(192);
            with();
          }
        }

        setState(195);
        queryNoWith();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WithContext extends ParserRuleContext {

    public TerminalNode WITH() {
      return getToken(SqlBaseParser.WITH, 0);
    }

    public List<NamedQueryContext> namedQuery() {
      return getRuleContexts(NamedQueryContext.class);
    }

    public NamedQueryContext namedQuery(int i) {
      return getRuleContext(NamedQueryContext.class, i);
    }

    public TerminalNode RECURSIVE() {
      return getToken(SqlBaseParser.RECURSIVE, 0);
    }

    public WithContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_with;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterWith(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitWith(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitWith(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final WithContext with() throws RecognitionException {
    WithContext _localctx = new WithContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_with);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(197);
        match(WITH);
        setState(199);
        _la = _input.LA(1);
        if (_la == RECURSIVE) {
          {
            setState(198);
            match(RECURSIVE);
          }
        }

        setState(201);
        namedQuery();
        setState(206);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__2) {
          {
            {
              setState(202);
              match(T__2);
              setState(203);
              namedQuery();
            }
          }
          setState(208);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TableElementContext extends ParserRuleContext {

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TypeContext type() {
      return getRuleContext(TypeContext.class, 0);
    }

    public TableElementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableElement;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTableElement(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTableElement(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTableElement(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TableElementContext tableElement() throws RecognitionException {
    TableElementContext _localctx = new TableElementContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_tableElement);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(209);
        identifier();
        setState(210);
        type(0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertiesContext extends ParserRuleContext {

    public List<TablePropertyContext> tableProperty() {
      return getRuleContexts(TablePropertyContext.class);
    }

    public TablePropertyContext tableProperty(int i) {
      return getRuleContext(TablePropertyContext.class, i);
    }

    public TablePropertiesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableProperties;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTableProperties(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTableProperties(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTableProperties(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TablePropertiesContext tableProperties() throws RecognitionException {
    TablePropertiesContext _localctx = new TablePropertiesContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_tableProperties);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(212);
        match(T__1);
        setState(213);
        tableProperty();
        setState(218);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__2) {
          {
            {
              setState(214);
              match(T__2);
              setState(215);
              tableProperty();
            }
          }
          setState(220);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(221);
        match(T__3);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertyContext extends ParserRuleContext {

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode EQ() {
      return getToken(SqlBaseParser.EQ, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TablePropertyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableProperty;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTableProperty(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTableProperty(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTableProperty(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TablePropertyContext tableProperty() throws RecognitionException {
    TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_tableProperty);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(223);
        identifier();
        setState(224);
        match(EQ);
        setState(225);
        expression();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryNoWithContext extends ParserRuleContext {

    public Token limit;
    public NumberContext confidence;

    public QueryTermContext queryTerm() {
      return getRuleContext(QueryTermContext.class, 0);
    }

    public TerminalNode ORDER() {
      return getToken(SqlBaseParser.ORDER, 0);
    }

    public TerminalNode BY() {
      return getToken(SqlBaseParser.BY, 0);
    }

    public List<SortItemContext> sortItem() {
      return getRuleContexts(SortItemContext.class);
    }

    public SortItemContext sortItem(int i) {
      return getRuleContext(SortItemContext.class, i);
    }

    public TerminalNode LIMIT() {
      return getToken(SqlBaseParser.LIMIT, 0);
    }

    public TerminalNode APPROXIMATE() {
      return getToken(SqlBaseParser.APPROXIMATE, 0);
    }

    public TerminalNode AT() {
      return getToken(SqlBaseParser.AT, 0);
    }

    public TerminalNode CONFIDENCE() {
      return getToken(SqlBaseParser.CONFIDENCE, 0);
    }

    public NumberContext number() {
      return getRuleContext(NumberContext.class, 0);
    }

    public TerminalNode INTEGER_VALUE() {
      return getToken(SqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode ALL() {
      return getToken(SqlBaseParser.ALL, 0);
    }

    public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryNoWith;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQueryNoWith(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQueryNoWith(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQueryNoWith(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QueryNoWithContext queryNoWith() throws RecognitionException {
    QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_queryNoWith);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(227);
        queryTerm(0);
        setState(238);
        _la = _input.LA(1);
        if (_la == ORDER) {
          {
            setState(228);
            match(ORDER);
            setState(229);
            match(BY);
            setState(230);
            sortItem();
            setState(235);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(231);
                  match(T__2);
                  setState(232);
                  sortItem();
                }
              }
              setState(237);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
        }

        setState(242);
        _la = _input.LA(1);
        if (_la == LIMIT) {
          {
            setState(240);
            match(LIMIT);
            setState(241);
            ((QueryNoWithContext) _localctx).limit = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == ALL || _la == INTEGER_VALUE)) {
              ((QueryNoWithContext) _localctx).limit = (Token) _errHandler.recoverInline(this);
            } else {
              consume();
            }
          }
        }

        setState(249);
        _la = _input.LA(1);
        if (_la == APPROXIMATE) {
          {
            setState(244);
            match(APPROXIMATE);
            setState(245);
            match(AT);
            setState(246);
            ((QueryNoWithContext) _localctx).confidence = number();
            setState(247);
            match(CONFIDENCE);
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryTermContext extends ParserRuleContext {

    public QueryTermContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryTerm;
    }

    public QueryTermContext() {
    }

    public void copyFrom(QueryTermContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class QueryTermDefaultContext extends QueryTermContext {

    public QueryPrimaryContext queryPrimary() {
      return getRuleContext(QueryPrimaryContext.class, 0);
    }

    public QueryTermDefaultContext(QueryTermContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQueryTermDefault(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQueryTermDefault(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQueryTermDefault(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SetOperationContext extends QueryTermContext {

    public QueryTermContext left;
    public Token operator;
    public QueryTermContext right;

    public List<QueryTermContext> queryTerm() {
      return getRuleContexts(QueryTermContext.class);
    }

    public QueryTermContext queryTerm(int i) {
      return getRuleContext(QueryTermContext.class, i);
    }

    public TerminalNode INTERSECT() {
      return getToken(SqlBaseParser.INTERSECT, 0);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public TerminalNode UNION() {
      return getToken(SqlBaseParser.UNION, 0);
    }

    public TerminalNode EXCEPT() {
      return getToken(SqlBaseParser.EXCEPT, 0);
    }

    public SetOperationContext(QueryTermContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSetOperation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSetOperation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSetOperation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QueryTermContext queryTerm() throws RecognitionException {
    return queryTerm(0);
  }

  private QueryTermContext queryTerm(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
    QueryTermContext _prevctx = _localctx;
    int _startState = 20;
    enterRecursionRule(_localctx, 20, RULE_queryTerm, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        {
          _localctx = new QueryTermDefaultContext(_localctx);
          _ctx = _localctx;
          _prevctx = _localctx;

          setState(252);
          queryPrimary();
        }
        _ctx.stop = _input.LT(-1);
        setState(268);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 20, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              setState(266);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 19, _ctx)) {
                case 1: {
                  _localctx =
                      new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
                  ((SetOperationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
                  setState(254);
                  if (!(precpred(_ctx, 2))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  }
                  setState(255);
                  ((SetOperationContext) _localctx).operator = match(INTERSECT);
                  setState(257);
                  _la = _input.LA(1);
                  if (_la == ALL || _la == DISTINCT) {
                    {
                      setState(256);
                      setQuantifier();
                    }
                  }

                  setState(259);
                  ((SetOperationContext) _localctx).right = queryTerm(3);
                }
                break;
                case 2: {
                  _localctx =
                      new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
                  ((SetOperationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
                  setState(260);
                  if (!(precpred(_ctx, 1))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  }
                  setState(261);
                  ((SetOperationContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(_la == UNION || _la == EXCEPT)) {
                    ((SetOperationContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    consume();
                  }
                  setState(263);
                  _la = _input.LA(1);
                  if (_la == ALL || _la == DISTINCT) {
                    {
                      setState(262);
                      setQuantifier();
                    }
                  }

                  setState(265);
                  ((SetOperationContext) _localctx).right = queryTerm(2);
                }
                break;
              }
            }
          }
          setState(270);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 20, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class QueryPrimaryContext extends ParserRuleContext {

    public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryPrimary;
    }

    public QueryPrimaryContext() {
    }

    public void copyFrom(QueryPrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SubqueryContext extends QueryPrimaryContext {

    public QueryNoWithContext queryNoWith() {
      return getRuleContext(QueryNoWithContext.class, 0);
    }

    public SubqueryContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSubquery(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSubquery(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSubquery(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {

    public QuerySpecificationContext querySpecification() {
      return getRuleContext(QuerySpecificationContext.class, 0);
    }

    public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQueryPrimaryDefault(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQueryPrimaryDefault(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQueryPrimaryDefault(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class TableContext extends QueryPrimaryContext {

    public TerminalNode TABLE() {
      return getToken(SqlBaseParser.TABLE, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TableContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTable(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTable(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTable(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class InlineTableContext extends QueryPrimaryContext {

    public TerminalNode VALUES() {
      return getToken(SqlBaseParser.VALUES, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public InlineTableContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterInlineTable(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitInlineTable(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitInlineTable(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QueryPrimaryContext queryPrimary() throws RecognitionException {
    QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_queryPrimary);
    try {
      int _alt;
      setState(287);
      switch (_input.LA(1)) {
        case SELECT:
          _localctx = new QueryPrimaryDefaultContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(271);
          querySpecification();
        }
        break;
        case TABLE:
          _localctx = new TableContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(272);
          match(TABLE);
          setState(273);
          qualifiedName();
        }
        break;
        case VALUES:
          _localctx = new InlineTableContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(274);
          match(VALUES);
          setState(275);
          expression();
          setState(280);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
          while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1) {
              {
                {
                  setState(276);
                  match(T__2);
                  setState(277);
                  expression();
                }
              }
            }
            setState(282);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 21, _ctx);
          }
        }
        break;
        case T__1:
          _localctx = new SubqueryContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(283);
          match(T__1);
          setState(284);
          queryNoWith();
          setState(285);
          match(T__3);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SortItemContext extends ParserRuleContext {

    public Token ordering;
    public Token nullOrdering;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode NULLS() {
      return getToken(SqlBaseParser.NULLS, 0);
    }

    public TerminalNode ASC() {
      return getToken(SqlBaseParser.ASC, 0);
    }

    public TerminalNode DESC() {
      return getToken(SqlBaseParser.DESC, 0);
    }

    public TerminalNode FIRST() {
      return getToken(SqlBaseParser.FIRST, 0);
    }

    public TerminalNode LAST() {
      return getToken(SqlBaseParser.LAST, 0);
    }

    public SortItemContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_sortItem;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSortItem(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSortItem(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSortItem(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SortItemContext sortItem() throws RecognitionException {
    SortItemContext _localctx = new SortItemContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_sortItem);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(289);
        expression();
        setState(291);
        _la = _input.LA(1);
        if (_la == ASC || _la == DESC) {
          {
            setState(290);
            ((SortItemContext) _localctx).ordering = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == ASC || _la == DESC)) {
              ((SortItemContext) _localctx).ordering = (Token) _errHandler.recoverInline(this);
            } else {
              consume();
            }
          }
        }

        setState(295);
        _la = _input.LA(1);
        if (_la == NULLS) {
          {
            setState(293);
            match(NULLS);
            setState(294);
            ((SortItemContext) _localctx).nullOrdering = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == FIRST || _la == LAST)) {
              ((SortItemContext) _localctx).nullOrdering = (Token) _errHandler.recoverInline(this);
            } else {
              consume();
            }
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QuerySpecificationContext extends ParserRuleContext {

    public RelationPrimaryContext into;
    public RelationContext from;
    public BooleanExpressionContext where;
    public BooleanExpressionContext having;

    public TerminalNode SELECT() {
      return getToken(SqlBaseParser.SELECT, 0);
    }

    public List<SelectItemContext> selectItem() {
      return getRuleContexts(SelectItemContext.class);
    }

    public SelectItemContext selectItem(int i) {
      return getRuleContext(SelectItemContext.class, i);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public TerminalNode INTO() {
      return getToken(SqlBaseParser.INTO, 0);
    }

    public TerminalNode FROM() {
      return getToken(SqlBaseParser.FROM, 0);
    }

    public TerminalNode WHERE() {
      return getToken(SqlBaseParser.WHERE, 0);
    }

    public TerminalNode GROUP() {
      return getToken(SqlBaseParser.GROUP, 0);
    }

    public TerminalNode BY() {
      return getToken(SqlBaseParser.BY, 0);
    }

    public GroupByContext groupBy() {
      return getRuleContext(GroupByContext.class, 0);
    }

    public TerminalNode HAVING() {
      return getToken(SqlBaseParser.HAVING, 0);
    }

    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class, 0);
    }

    public List<RelationContext> relation() {
      return getRuleContexts(RelationContext.class);
    }

    public RelationContext relation(int i) {
      return getRuleContext(RelationContext.class, i);
    }

    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }

    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class, i);
    }

    public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_querySpecification;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQuerySpecification(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQuerySpecification(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQuerySpecification(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QuerySpecificationContext querySpecification() throws RecognitionException {
    QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_querySpecification);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(297);
        match(SELECT);
        setState(299);
        _la = _input.LA(1);
        if (_la == ALL || _la == DISTINCT) {
          {
            setState(298);
            setQuantifier();
          }
        }

        setState(301);
        selectItem();
        setState(306);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 26, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(302);
                match(T__2);
                setState(303);
                selectItem();
              }
            }
          }
          setState(308);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 26, _ctx);
        }
        setState(311);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 27, _ctx)) {
          case 1: {
            setState(309);
            match(INTO);
            setState(310);
            ((QuerySpecificationContext) _localctx).into = relationPrimary();
          }
          break;
        }
        setState(322);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 29, _ctx)) {
          case 1: {
            setState(313);
            match(FROM);
            setState(314);
            ((QuerySpecificationContext) _localctx).from = relation(0);
            setState(319);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 28, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(315);
                    match(T__2);
                    setState(316);
                    relation(0);
                  }
                }
              }
              setState(321);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 28, _ctx);
            }
          }
          break;
        }
        setState(326);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 30, _ctx)) {
          case 1: {
            setState(324);
            match(WHERE);
            setState(325);
            ((QuerySpecificationContext) _localctx).where = booleanExpression(0);
          }
          break;
        }
        setState(331);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 31, _ctx)) {
          case 1: {
            setState(328);
            match(GROUP);
            setState(329);
            match(BY);
            setState(330);
            groupBy();
          }
          break;
        }
        setState(335);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 32, _ctx)) {
          case 1: {
            setState(333);
            match(HAVING);
            setState(334);
            ((QuerySpecificationContext) _localctx).having = booleanExpression(0);
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupByContext extends ParserRuleContext {

    public List<GroupingElementContext> groupingElement() {
      return getRuleContexts(GroupingElementContext.class);
    }

    public GroupingElementContext groupingElement(int i) {
      return getRuleContext(GroupingElementContext.class, i);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public GroupByContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_groupBy;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterGroupBy(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitGroupBy(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitGroupBy(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final GroupByContext groupBy() throws RecognitionException {
    GroupByContext _localctx = new GroupByContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_groupBy);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(338);
        _la = _input.LA(1);
        if (_la == ALL || _la == DISTINCT) {
          {
            setState(337);
            setQuantifier();
          }
        }

        setState(340);
        groupingElement();
        setState(345);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 34, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(341);
                match(T__2);
                setState(342);
                groupingElement();
              }
            }
          }
          setState(347);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 34, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupingElementContext extends ParserRuleContext {

    public GroupingElementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_groupingElement;
    }

    public GroupingElementContext() {
    }

    public void copyFrom(GroupingElementContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class MultipleGroupingSetsContext extends GroupingElementContext {

    public TerminalNode GROUPING() {
      return getToken(SqlBaseParser.GROUPING, 0);
    }

    public TerminalNode SETS() {
      return getToken(SqlBaseParser.SETS, 0);
    }

    public List<GroupingSetContext> groupingSet() {
      return getRuleContexts(GroupingSetContext.class);
    }

    public GroupingSetContext groupingSet(int i) {
      return getRuleContext(GroupingSetContext.class, i);
    }

    public MultipleGroupingSetsContext(GroupingElementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterMultipleGroupingSets(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitMultipleGroupingSets(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitMultipleGroupingSets(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SingleGroupingSetContext extends GroupingElementContext {

    public GroupingExpressionsContext groupingExpressions() {
      return getRuleContext(GroupingExpressionsContext.class, 0);
    }

    public SingleGroupingSetContext(GroupingElementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSingleGroupingSet(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSingleGroupingSet(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSingleGroupingSet(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class CubeContext extends GroupingElementContext {

    public TerminalNode CUBE() {
      return getToken(SqlBaseParser.CUBE, 0);
    }

    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }

    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class, i);
    }

    public CubeContext(GroupingElementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterCube(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitCube(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitCube(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class RollupContext extends GroupingElementContext {

    public TerminalNode ROLLUP() {
      return getToken(SqlBaseParser.ROLLUP, 0);
    }

    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }

    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class, i);
    }

    public RollupContext(GroupingElementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterRollup(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitRollup(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitRollup(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final GroupingElementContext groupingElement() throws RecognitionException {
    GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_groupingElement);
    int _la;
    try {
      setState(388);
      switch (_input.LA(1)) {
        case T__1:
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NOT:
        case NO:
        case EXISTS:
        case NULL:
        case TRUE:
        case FALSE:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case CURRENT_DATE:
        case CURRENT_TIME:
        case CURRENT_TIMESTAMP:
        case LOCALTIME:
        case LOCALTIMESTAMP:
        case EXTRACT:
        case CASE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case CAST:
        case TRY_CAST:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NORMALIZE:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
        case PLUS:
        case MINUS:
        case STRING:
        case BINARY_LITERAL:
        case INTEGER_VALUE:
        case DECIMAL_VALUE:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          _localctx = new SingleGroupingSetContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(348);
          groupingExpressions();
        }
        break;
        case ROLLUP:
          _localctx = new RollupContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(349);
          match(ROLLUP);
          setState(350);
          match(T__1);
          setState(359);
          _la = _input.LA(1);
          if (((((_la - 12)) & ~0x3f) == 0 &&
               ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT
                                                                                                - 12))
                                      | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (
                   SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (
                   SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME
                                                                                            - 12))
                                      | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L
                                                                                              << (YEAR
                                                                                                  - 12))
                                      | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR
                                                                                            - 12))
                                      | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE
                                                                                                - 12))))
               != 0) || ((((_la - 87)) & ~0x3f) == 0 &&
                         ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L
                                                                                                  << (RANGE
                                                                                                      - 87))
                                                | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (
                                                    1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87))
                                                | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L
                                                                                              << (REPLACE
                                                                                                  - 87))
                                                | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L
                                                                                                  << (PRIVILEGES
                                                                                                      - 87))
                                                | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (
                                                    1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87))
                                                | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L
                                                                                                 << (TEXT
                                                                                                     - 87))
                                                | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87))
                                                | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87))
                                                | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L
                                                                                                 << (SCHEMAS
                                                                                                     - 87))
                                                | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87))
                                                | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L
                                                                                                << (PARTITIONS
                                                                                                    - 87))
                                                | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L
                                                                                                  << (SYSTEM
                                                                                                      - 87))
                                                | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED
                                                                                     - 87)) | (1L
                                                                                               << (TABLESAMPLE
                                                                                                   - 87))
                                                | (1L << (RESCALED - 87)))) != 0) || (
                  (((_la - 155)) & ~0x3f) == 0 &&
                  ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET
                                                                                                - 155))
                                          | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L
                                                                                               << (DATA
                                                                                                   - 155))
                                          | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (
                                              1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L
                                                                                                  << (WORK
                                                                                                      - 155))
                                          | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L
                                                                                                 << (SERIALIZABLE
                                                                                                     - 155))
                                          | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155))
                                          | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L
                                                                                                  << (WRITE
                                                                                                      - 155))
                                          | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (
                      NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC
                                                                                         - 155)) | (
                                              1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (
                      COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER
                                                                              - 155)) | (1L << (
                      QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
            {
              setState(351);
              qualifiedName();
              setState(356);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__2) {
                {
                  {
                    setState(352);
                    match(T__2);
                    setState(353);
                    qualifiedName();
                  }
                }
                setState(358);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
          }

          setState(361);
          match(T__3);
        }
        break;
        case CUBE:
          _localctx = new CubeContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(362);
          match(CUBE);
          setState(363);
          match(T__1);
          setState(372);
          _la = _input.LA(1);
          if (((((_la - 12)) & ~0x3f) == 0 &&
               ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT
                                                                                                - 12))
                                      | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (
                   SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (
                   SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME
                                                                                            - 12))
                                      | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L
                                                                                              << (YEAR
                                                                                                  - 12))
                                      | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR
                                                                                            - 12))
                                      | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE
                                                                                                - 12))))
               != 0) || ((((_la - 87)) & ~0x3f) == 0 &&
                         ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L
                                                                                                  << (RANGE
                                                                                                      - 87))
                                                | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (
                                                    1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87))
                                                | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L
                                                                                              << (REPLACE
                                                                                                  - 87))
                                                | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L
                                                                                                  << (PRIVILEGES
                                                                                                      - 87))
                                                | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (
                                                    1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87))
                                                | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L
                                                                                                 << (TEXT
                                                                                                     - 87))
                                                | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87))
                                                | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87))
                                                | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L
                                                                                                 << (SCHEMAS
                                                                                                     - 87))
                                                | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87))
                                                | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L
                                                                                                << (PARTITIONS
                                                                                                    - 87))
                                                | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L
                                                                                                  << (SYSTEM
                                                                                                      - 87))
                                                | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED
                                                                                     - 87)) | (1L
                                                                                               << (TABLESAMPLE
                                                                                                   - 87))
                                                | (1L << (RESCALED - 87)))) != 0) || (
                  (((_la - 155)) & ~0x3f) == 0 &&
                  ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET
                                                                                                - 155))
                                          | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L
                                                                                               << (DATA
                                                                                                   - 155))
                                          | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (
                                              1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L
                                                                                                  << (WORK
                                                                                                      - 155))
                                          | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L
                                                                                                 << (SERIALIZABLE
                                                                                                     - 155))
                                          | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155))
                                          | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L
                                                                                                  << (WRITE
                                                                                                      - 155))
                                          | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (
                      NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC
                                                                                         - 155)) | (
                                              1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (
                      COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER
                                                                              - 155)) | (1L << (
                      QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
            {
              setState(364);
              qualifiedName();
              setState(369);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__2) {
                {
                  {
                    setState(365);
                    match(T__2);
                    setState(366);
                    qualifiedName();
                  }
                }
                setState(371);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
          }

          setState(374);
          match(T__3);
        }
        break;
        case GROUPING:
          _localctx = new MultipleGroupingSetsContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(375);
          match(GROUPING);
          setState(376);
          match(SETS);
          setState(377);
          match(T__1);
          setState(378);
          groupingSet();
          setState(383);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__2) {
            {
              {
                setState(379);
                match(T__2);
                setState(380);
                groupingSet();
              }
            }
            setState(385);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(386);
          match(T__3);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupingExpressionsContext extends ParserRuleContext {

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public GroupingExpressionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_groupingExpressions;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterGroupingExpressions(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitGroupingExpressions(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitGroupingExpressions(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final GroupingExpressionsContext groupingExpressions() throws RecognitionException {
    GroupingExpressionsContext _localctx = new GroupingExpressionsContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_groupingExpressions);
    int _la;
    try {
      setState(403);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 43, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(390);
          match(T__1);
          setState(399);
          _la = _input.LA(1);
          if ((((_la) & ~0x3f) == 0 &&
               ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << APPROXIMATE) | (1L << AT) | (1L
                                                                                                << CONFIDENCE)
                               | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << NULL) | (1L
                                                                                             << TRUE)
                               | (1L << FALSE) | (1L << SUBSTRING) | (1L << POSITION) | (1L
                                                                                         << TINYINT)
                               | (1L << SMALLINT) | (1L << INTEGER) | (1L << DATE) | (1L << TIME)
                               | (1L << TIMESTAMP) | (1L << INTERVAL) | (1L << YEAR) | (1L << MONTH)
                               | (1L << DAY) | (1L << HOUR) | (1L << MINUTE))) != 0) || (
                  (((_la - 64)) & ~0x3f) == 0 &&
                  ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L << (
                      CURRENT_DATE - 64)) | (1L << (CURRENT_TIME - 64)) | (1L << (CURRENT_TIMESTAMP
                                                                                  - 64)) | (1L << (
                      LOCALTIME - 64)) | (1L << (LOCALTIMESTAMP - 64)) | (1L << (EXTRACT - 64)) | (
                                             1L << (CASE - 64)) | (1L << (OVER - 64)) | (1L << (
                      PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (
                      PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L
                                                                                              << (ROW
                                                                                                  - 64))
                                         | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (
                      GRANT - 64)) | (1L << (REVOKE - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (
                      PUBLIC - 64)) | (1L << (OPTION - 64)) | (1L << (EXPLAIN - 64)) | (1L << (
                      ANALYZE - 64)) | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (1L << (TEXT
                                                                                             - 64))
                                         | (1L << (GRAPHVIZ - 64)) | (1L << (LOGICAL - 64)) | (1L
                                                                                               << (DISTRIBUTED
                                                                                                   - 64))
                                         | (1L << (TRY - 64)) | (1L << (CAST - 64)) | (1L << (
                      TRY_CAST - 64)) | (1L << (SHOW - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0
                                                                        && ((1L << (_la - 128)) & (
              (1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L << (CATALOGS - 128)) | (1L << (
                  COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L << (USE - 128)) | (1L << (
                  PARTITIONS - 128)) | (1L << (FUNCTIONS - 128)) | (1L << (TO - 128)) | (1L << (
                  SYSTEM - 128)) | (1L << (BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L
                                                                                              << (TABLESAMPLE
                                                                                                  - 128))
              | (1L << (RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET
                                                                                                 - 128))
              | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L << (DATA - 128)) | (1L << (
                  START - 128)) | (1L << (TRANSACTION - 128)) | (1L << (COMMIT - 128)) | (1L << (
                  ROLLBACK - 128)) | (1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (1L << (
                  LEVEL - 128)) | (1L << (SERIALIZABLE - 128)) | (1L << (REPEATABLE - 128)) | (1L
                                                                                               << (COMMITTED
                                                                                                   - 128))
              | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (1L << (WRITE - 128)) | (1L
                                                                                              << (ONLY
                                                                                                  - 128))
              | (1L << (CALL - 128)) | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L << (NFC
                                                                                                 - 128))
              | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (IF - 128)) | (1L << (NULLIF
                                                                                           - 128))
              | (1L << (COALESCE - 128)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 &&
                                                     ((1L << (_la - 194)) & ((1L << (PLUS - 194))
                                                                             | (1L << (MINUS - 194))
                                                                             | (1L << (STRING
                                                                                       - 194)) | (1L
                                                                                                  << (BINARY_LITERAL
                                                                                                      - 194))
                                                                             | (1L << (INTEGER_VALUE
                                                                                       - 194)) | (1L
                                                                                                  << (DECIMAL_VALUE
                                                                                                      - 194))
                                                                             | (1L << (IDENTIFIER
                                                                                       - 194)) | (1L
                                                                                                  << (DIGIT_IDENTIFIER
                                                                                                      - 194))
                                                                             | (1L << (
                                                         QUOTED_IDENTIFIER - 194)) | (1L << (
                                                         BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
            {
              setState(391);
              expression();
              setState(396);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__2) {
                {
                  {
                    setState(392);
                    match(T__2);
                    setState(393);
                    expression();
                  }
                }
                setState(398);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
          }

          setState(401);
          match(T__3);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(402);
          expression();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupingSetContext extends ParserRuleContext {

    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }

    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class, i);
    }

    public GroupingSetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_groupingSet;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterGroupingSet(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitGroupingSet(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitGroupingSet(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final GroupingSetContext groupingSet() throws RecognitionException {
    GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_groupingSet);
    int _la;
    try {
      setState(418);
      switch (_input.LA(1)) {
        case T__1:
          enterOuterAlt(_localctx, 1);
        {
          setState(405);
          match(T__1);
          setState(414);
          _la = _input.LA(1);
          if (((((_la - 12)) & ~0x3f) == 0 &&
               ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT
                                                                                                - 12))
                                      | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (
                   SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (
                   SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME
                                                                                            - 12))
                                      | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L
                                                                                              << (YEAR
                                                                                                  - 12))
                                      | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR
                                                                                            - 12))
                                      | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE
                                                                                                - 12))))
               != 0) || ((((_la - 87)) & ~0x3f) == 0 &&
                         ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L
                                                                                                  << (RANGE
                                                                                                      - 87))
                                                | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (
                                                    1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87))
                                                | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L
                                                                                              << (REPLACE
                                                                                                  - 87))
                                                | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L
                                                                                                  << (PRIVILEGES
                                                                                                      - 87))
                                                | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (
                                                    1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87))
                                                | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L
                                                                                                 << (TEXT
                                                                                                     - 87))
                                                | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87))
                                                | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87))
                                                | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L
                                                                                                 << (SCHEMAS
                                                                                                     - 87))
                                                | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87))
                                                | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L
                                                                                                << (PARTITIONS
                                                                                                    - 87))
                                                | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L
                                                                                                  << (SYSTEM
                                                                                                      - 87))
                                                | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED
                                                                                     - 87)) | (1L
                                                                                               << (TABLESAMPLE
                                                                                                   - 87))
                                                | (1L << (RESCALED - 87)))) != 0) || (
                  (((_la - 155)) & ~0x3f) == 0 &&
                  ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET
                                                                                                - 155))
                                          | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L
                                                                                               << (DATA
                                                                                                   - 155))
                                          | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (
                                              1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L
                                                                                                  << (WORK
                                                                                                      - 155))
                                          | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L
                                                                                                 << (SERIALIZABLE
                                                                                                     - 155))
                                          | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155))
                                          | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L
                                                                                                  << (WRITE
                                                                                                      - 155))
                                          | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (
                      NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC
                                                                                         - 155)) | (
                                              1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (
                      COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER
                                                                              - 155)) | (1L << (
                      QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
            {
              setState(406);
              qualifiedName();
              setState(411);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__2) {
                {
                  {
                    setState(407);
                    match(T__2);
                    setState(408);
                    qualifiedName();
                  }
                }
                setState(413);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
          }

          setState(416);
          match(T__3);
        }
        break;
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NO:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          enterOuterAlt(_localctx, 2);
        {
          setState(417);
          qualifiedName();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamedQueryContext extends ParserRuleContext {

    public IdentifierContext name;

    public TerminalNode AS() {
      return getToken(SqlBaseParser.AS, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ColumnAliasesContext columnAliases() {
      return getRuleContext(ColumnAliasesContext.class, 0);
    }

    public NamedQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namedQuery;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNamedQuery(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNamedQuery(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNamedQuery(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final NamedQueryContext namedQuery() throws RecognitionException {
    NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_namedQuery);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(420);
        ((NamedQueryContext) _localctx).name = identifier();
        setState(422);
        _la = _input.LA(1);
        if (_la == T__1) {
          {
            setState(421);
            columnAliases();
          }
        }

        setState(424);
        match(AS);
        setState(425);
        match(T__1);
        setState(426);
        query();
        setState(427);
        match(T__3);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SetQuantifierContext extends ParserRuleContext {

    public TerminalNode DISTINCT() {
      return getToken(SqlBaseParser.DISTINCT, 0);
    }

    public TerminalNode ALL() {
      return getToken(SqlBaseParser.ALL, 0);
    }

    public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_setQuantifier;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSetQuantifier(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSetQuantifier(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSetQuantifier(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SetQuantifierContext setQuantifier() throws RecognitionException {
    SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_setQuantifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(429);
        _la = _input.LA(1);
        if (!(_la == ALL || _la == DISTINCT)) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SelectItemContext extends ParserRuleContext {

    public SelectItemContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_selectItem;
    }

    public SelectItemContext() {
    }

    public void copyFrom(SelectItemContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SelectAllContext extends SelectItemContext {

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode ASTERISK() {
      return getToken(SqlBaseParser.ASTERISK, 0);
    }

    public SelectAllContext(SelectItemContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSelectAll(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSelectAll(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSelectAll(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SelectSingleContext extends SelectItemContext {

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(SqlBaseParser.AS, 0);
    }

    public SelectSingleContext(SelectItemContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSelectSingle(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSelectSingle(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSelectSingle(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SelectItemContext selectItem() throws RecognitionException {
    SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_selectItem);
    int _la;
    try {
      setState(443);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 50, _ctx)) {
        case 1:
          _localctx = new SelectSingleContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(431);
          expression();
          setState(436);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 49, _ctx)) {
            case 1: {
              setState(433);
              _la = _input.LA(1);
              if (_la == AS) {
                {
                  setState(432);
                  match(AS);
                }
              }

              setState(435);
              identifier();
            }
            break;
          }
        }
        break;
        case 2:
          _localctx = new SelectAllContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(438);
          qualifiedName();
          setState(439);
          match(T__4);
          setState(440);
          match(ASTERISK);
        }
        break;
        case 3:
          _localctx = new SelectAllContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(442);
          match(ASTERISK);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class RelationContext extends ParserRuleContext {

    public RelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_relation;
    }

    public RelationContext() {
    }

    public void copyFrom(RelationContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class RelationDefaultContext extends RelationContext {

    public AliasedRelationContext aliasedRelation() {
      return getRuleContext(AliasedRelationContext.class, 0);
    }

    public RelationDefaultContext(RelationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterRelationDefault(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitRelationDefault(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitRelationDefault(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class JoinRelationContext extends RelationContext {

    public RelationContext left;
    public AliasedRelationContext right;
    public RelationContext rightRelation;

    public List<RelationContext> relation() {
      return getRuleContexts(RelationContext.class);
    }

    public RelationContext relation(int i) {
      return getRuleContext(RelationContext.class, i);
    }

    public TerminalNode CROSS() {
      return getToken(SqlBaseParser.CROSS, 0);
    }

    public TerminalNode JOIN() {
      return getToken(SqlBaseParser.JOIN, 0);
    }

    public JoinTypeContext joinType() {
      return getRuleContext(JoinTypeContext.class, 0);
    }

    public JoinCriteriaContext joinCriteria() {
      return getRuleContext(JoinCriteriaContext.class, 0);
    }

    public TerminalNode NATURAL() {
      return getToken(SqlBaseParser.NATURAL, 0);
    }

    public AliasedRelationContext aliasedRelation() {
      return getRuleContext(AliasedRelationContext.class, 0);
    }

    public JoinRelationContext(RelationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterJoinRelation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitJoinRelation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitJoinRelation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final RelationContext relation() throws RecognitionException {
    return relation(0);
  }

  private RelationContext relation(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    RelationContext _localctx = new RelationContext(_ctx, _parentState);
    RelationContext _prevctx = _localctx;
    int _startState = 42;
    enterRecursionRule(_localctx, 42, RULE_relation, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        {
          _localctx = new RelationDefaultContext(_localctx);
          _ctx = _localctx;
          _prevctx = _localctx;

          setState(446);
          aliasedRelation();
        }
        _ctx.stop = _input.LT(-1);
        setState(466);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 52, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              {
                _localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
                ((JoinRelationContext) _localctx).left = _prevctx;
                pushNewRecursionContext(_localctx, _startState, RULE_relation);
                setState(448);
                if (!(precpred(_ctx, 2))) {
                  throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                }
                setState(462);
                switch (_input.LA(1)) {
                  case CROSS: {
                    setState(449);
                    match(CROSS);
                    setState(450);
                    match(JOIN);
                    setState(451);
                    ((JoinRelationContext) _localctx).right = aliasedRelation();
                  }
                  break;
                  case JOIN:
                  case INNER:
                  case LEFT:
                  case RIGHT:
                  case FULL: {
                    setState(452);
                    joinType();
                    setState(453);
                    match(JOIN);
                    setState(454);
                    ((JoinRelationContext) _localctx).rightRelation = relation(0);
                    setState(455);
                    joinCriteria();
                  }
                  break;
                  case NATURAL: {
                    setState(457);
                    match(NATURAL);
                    setState(458);
                    joinType();
                    setState(459);
                    match(JOIN);
                    setState(460);
                    ((JoinRelationContext) _localctx).right = aliasedRelation();
                  }
                  break;
                  default:
                    throw new NoViableAltException(this);
                }
              }
            }
          }
          setState(468);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 52, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class JoinTypeContext extends ParserRuleContext {

    public TerminalNode INNER() {
      return getToken(SqlBaseParser.INNER, 0);
    }

    public TerminalNode LEFT() {
      return getToken(SqlBaseParser.LEFT, 0);
    }

    public TerminalNode OUTER() {
      return getToken(SqlBaseParser.OUTER, 0);
    }

    public TerminalNode RIGHT() {
      return getToken(SqlBaseParser.RIGHT, 0);
    }

    public TerminalNode FULL() {
      return getToken(SqlBaseParser.FULL, 0);
    }

    public JoinTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_joinType;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterJoinType(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitJoinType(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitJoinType(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final JoinTypeContext joinType() throws RecognitionException {
    JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_joinType);
    int _la;
    try {
      setState(484);
      switch (_input.LA(1)) {
        case JOIN:
        case INNER:
          enterOuterAlt(_localctx, 1);
        {
          setState(470);
          _la = _input.LA(1);
          if (_la == INNER) {
            {
              setState(469);
              match(INNER);
            }
          }

        }
        break;
        case LEFT:
          enterOuterAlt(_localctx, 2);
        {
          setState(472);
          match(LEFT);
          setState(474);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(473);
              match(OUTER);
            }
          }

        }
        break;
        case RIGHT:
          enterOuterAlt(_localctx, 3);
        {
          setState(476);
          match(RIGHT);
          setState(478);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(477);
              match(OUTER);
            }
          }

        }
        break;
        case FULL:
          enterOuterAlt(_localctx, 4);
        {
          setState(480);
          match(FULL);
          setState(482);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(481);
              match(OUTER);
            }
          }

        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class JoinCriteriaContext extends ParserRuleContext {

    public TerminalNode ON() {
      return getToken(SqlBaseParser.ON, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public TerminalNode USING() {
      return getToken(SqlBaseParser.USING, 0);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_joinCriteria;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterJoinCriteria(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitJoinCriteria(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitJoinCriteria(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final JoinCriteriaContext joinCriteria() throws RecognitionException {
    JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_joinCriteria);
    int _la;
    try {
      setState(500);
      switch (_input.LA(1)) {
        case ON:
          enterOuterAlt(_localctx, 1);
        {
          setState(486);
          match(ON);
          setState(487);
          booleanExpression(0);
        }
        break;
        case USING:
          enterOuterAlt(_localctx, 2);
        {
          setState(488);
          match(USING);
          setState(489);
          match(T__1);
          setState(490);
          identifier();
          setState(495);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__2) {
            {
              {
                setState(491);
                match(T__2);
                setState(492);
                identifier();
              }
            }
            setState(497);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(498);
          match(T__3);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SampleTypeContext extends ParserRuleContext {

    public TerminalNode BERNOULLI() {
      return getToken(SqlBaseParser.BERNOULLI, 0);
    }

    public TerminalNode SYSTEM() {
      return getToken(SqlBaseParser.SYSTEM, 0);
    }

    public TerminalNode POISSONIZED() {
      return getToken(SqlBaseParser.POISSONIZED, 0);
    }

    public SampleTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_sampleType;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSampleType(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSampleType(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSampleType(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final SampleTypeContext sampleType() throws RecognitionException {
    SampleTypeContext _localctx = new SampleTypeContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_sampleType);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(502);
        _la = _input.LA(1);
        if (!(((((_la - 145)) & ~0x3f) == 0 &&
               ((1L << (_la - 145)) & ((1L << (SYSTEM - 145)) | (1L << (BERNOULLI - 145)) | (1L << (
                   POISSONIZED - 145)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AliasedRelationContext extends ParserRuleContext {

    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(SqlBaseParser.AS, 0);
    }

    public ColumnAliasesContext columnAliases() {
      return getRuleContext(ColumnAliasesContext.class, 0);
    }

    public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_aliasedRelation;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterAliasedRelation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitAliasedRelation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitAliasedRelation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final AliasedRelationContext aliasedRelation() throws RecognitionException {
    AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_aliasedRelation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(504);
        relationPrimary();
        setState(512);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 62, _ctx)) {
          case 1: {
            setState(506);
            _la = _input.LA(1);
            if (_la == AS) {
              {
                setState(505);
                match(AS);
              }
            }

            setState(508);
            identifier();
            setState(510);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 61, _ctx)) {
              case 1: {
                setState(509);
                columnAliases();
              }
              break;
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ColumnAliasesContext extends ParserRuleContext {

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_columnAliases;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterColumnAliases(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitColumnAliases(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitColumnAliases(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final ColumnAliasesContext columnAliases() throws RecognitionException {
    ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_columnAliases);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(514);
        match(T__1);
        setState(515);
        identifier();
        setState(520);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__2) {
          {
            {
              setState(516);
              match(T__2);
              setState(517);
              identifier();
            }
          }
          setState(522);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(523);
        match(T__3);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class RelationPrimaryContext extends ParserRuleContext {

    public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_relationPrimary;
    }

    public RelationPrimaryContext() {
    }

    public void copyFrom(RelationPrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SubqueryRelationContext extends RelationPrimaryContext {

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public SubqueryRelationContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSubqueryRelation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSubqueryRelation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSubqueryRelation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ParenthesizedRelationContext extends RelationPrimaryContext {

    public RelationContext relation() {
      return getRuleContext(RelationContext.class, 0);
    }

    public ParenthesizedRelationContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterParenthesizedRelation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitParenthesizedRelation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitParenthesizedRelation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class UnnestContext extends RelationPrimaryContext {

    public TerminalNode UNNEST() {
      return getToken(SqlBaseParser.UNNEST, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode WITH() {
      return getToken(SqlBaseParser.WITH, 0);
    }

    public TerminalNode ORDINALITY() {
      return getToken(SqlBaseParser.ORDINALITY, 0);
    }

    public UnnestContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterUnnest(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitUnnest(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitUnnest(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class TableNameContext extends RelationPrimaryContext {

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TableNameContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTableName(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTableName(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTableName(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final RelationPrimaryContext relationPrimary() throws RecognitionException {
    RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_relationPrimary);
    int _la;
    try {
      setState(549);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 66, _ctx)) {
        case 1:
          _localctx = new TableNameContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(525);
          qualifiedName();
        }
        break;
        case 2:
          _localctx = new SubqueryRelationContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(526);
          match(T__1);
          setState(527);
          query();
          setState(528);
          match(T__3);
        }
        break;
        case 3:
          _localctx = new UnnestContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(530);
          match(UNNEST);
          setState(531);
          match(T__1);
          setState(532);
          expression();
          setState(537);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__2) {
            {
              {
                setState(533);
                match(T__2);
                setState(534);
                expression();
              }
            }
            setState(539);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(540);
          match(T__3);
          setState(543);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 65, _ctx)) {
            case 1: {
              setState(541);
              match(WITH);
              setState(542);
              match(ORDINALITY);
            }
            break;
          }
        }
        break;
        case 4:
          _localctx = new ParenthesizedRelationContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(545);
          match(T__1);
          setState(546);
          relation(0);
          setState(547);
          match(T__3);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionContext extends ParserRuleContext {

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_expression;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterExpression(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitExpression(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitExpression(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(551);
        booleanExpression(0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BooleanExpressionContext extends ParserRuleContext {

    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_booleanExpression;
    }

    public BooleanExpressionContext() {
    }

    public void copyFrom(BooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class LogicalNotContext extends BooleanExpressionContext {

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public LogicalNotContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterLogicalNot(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitLogicalNot(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitLogicalNot(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class BooleanDefaultContext extends BooleanExpressionContext {

    public PredicatedContext predicated() {
      return getRuleContext(PredicatedContext.class, 0);
    }

    public BooleanDefaultContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBooleanDefault(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBooleanDefault(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBooleanDefault(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class LogicalBinaryContext extends BooleanExpressionContext {

    public BooleanExpressionContext left;
    public Token operator;
    public BooleanExpressionContext right;

    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }

    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class, i);
    }

    public TerminalNode AND() {
      return getToken(SqlBaseParser.AND, 0);
    }

    public TerminalNode OR() {
      return getToken(SqlBaseParser.OR, 0);
    }

    public LogicalBinaryContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterLogicalBinary(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitLogicalBinary(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitLogicalBinary(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final BooleanExpressionContext booleanExpression() throws RecognitionException {
    return booleanExpression(0);
  }

  private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
    BooleanExpressionContext _prevctx = _localctx;
    int _startState = 58;
    enterRecursionRule(_localctx, 58, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(557);
        switch (_input.LA(1)) {
          case T__1:
          case ADD:
          case APPROXIMATE:
          case AT:
          case CONFIDENCE:
          case NO:
          case EXISTS:
          case NULL:
          case TRUE:
          case FALSE:
          case SUBSTRING:
          case POSITION:
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case DATE:
          case TIME:
          case TIMESTAMP:
          case INTERVAL:
          case YEAR:
          case MONTH:
          case DAY:
          case HOUR:
          case MINUTE:
          case SECOND:
          case ZONE:
          case CURRENT_DATE:
          case CURRENT_TIME:
          case CURRENT_TIMESTAMP:
          case LOCALTIME:
          case LOCALTIMESTAMP:
          case EXTRACT:
          case CASE:
          case OVER:
          case PARTITION:
          case RANGE:
          case ROWS:
          case PRECEDING:
          case FOLLOWING:
          case CURRENT:
          case ROW:
          case VIEW:
          case REPLACE:
          case GRANT:
          case REVOKE:
          case PRIVILEGES:
          case PUBLIC:
          case OPTION:
          case EXPLAIN:
          case ANALYZE:
          case FORMAT:
          case TYPE:
          case TEXT:
          case GRAPHVIZ:
          case LOGICAL:
          case DISTRIBUTED:
          case TRY:
          case CAST:
          case TRY_CAST:
          case SHOW:
          case TABLES:
          case SCHEMAS:
          case CATALOGS:
          case COLUMNS:
          case COLUMN:
          case USE:
          case PARTITIONS:
          case FUNCTIONS:
          case TO:
          case SYSTEM:
          case BERNOULLI:
          case POISSONIZED:
          case TABLESAMPLE:
          case RESCALED:
          case ARRAY:
          case MAP:
          case SET:
          case RESET:
          case SESSION:
          case DATA:
          case START:
          case TRANSACTION:
          case COMMIT:
          case ROLLBACK:
          case WORK:
          case ISOLATION:
          case LEVEL:
          case SERIALIZABLE:
          case REPEATABLE:
          case COMMITTED:
          case UNCOMMITTED:
          case READ:
          case WRITE:
          case ONLY:
          case CALL:
          case NORMALIZE:
          case NFD:
          case NFC:
          case NFKD:
          case NFKC:
          case IF:
          case NULLIF:
          case COALESCE:
          case PLUS:
          case MINUS:
          case STRING:
          case BINARY_LITERAL:
          case INTEGER_VALUE:
          case DECIMAL_VALUE:
          case IDENTIFIER:
          case DIGIT_IDENTIFIER:
          case QUOTED_IDENTIFIER:
          case BACKQUOTED_IDENTIFIER: {
            _localctx = new BooleanDefaultContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(554);
            predicated();
          }
          break;
          case NOT: {
            _localctx = new LogicalNotContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(555);
            match(NOT);
            setState(556);
            booleanExpression(3);
          }
          break;
          default:
            throw new NoViableAltException(this);
        }
        _ctx.stop = _input.LT(-1);
        setState(567);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 69, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              setState(565);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 68, _ctx)) {
                case 1: {
                  _localctx =
                      new LogicalBinaryContext(
                          new BooleanExpressionContext(_parentctx, _parentState));
                  ((LogicalBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                  setState(559);
                  if (!(precpred(_ctx, 2))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  }
                  setState(560);
                  ((LogicalBinaryContext) _localctx).operator = match(AND);
                  setState(561);
                  ((LogicalBinaryContext) _localctx).right = booleanExpression(3);
                }
                break;
                case 2: {
                  _localctx =
                      new LogicalBinaryContext(
                          new BooleanExpressionContext(_parentctx, _parentState));
                  ((LogicalBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                  setState(562);
                  if (!(precpred(_ctx, 1))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  }
                  setState(563);
                  ((LogicalBinaryContext) _localctx).operator = match(OR);
                  setState(564);
                  ((LogicalBinaryContext) _localctx).right = booleanExpression(2);
                }
                break;
              }
            }
          }
          setState(569);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 69, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PredicatedContext extends ParserRuleContext {

    public ValueExpressionContext valueExpression;

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public PredicatedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_predicated;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterPredicated(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitPredicated(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitPredicated(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final PredicatedContext predicated() throws RecognitionException {
    PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_predicated);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(570);
        ((PredicatedContext) _localctx).valueExpression = valueExpression(0);
        setState(572);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 70, _ctx)) {
          case 1: {
            setState(571);
            predicate(((PredicatedContext) _localctx).valueExpression);
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PredicateContext extends ParserRuleContext {

    public ParserRuleContext value;

    public PredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
      super(parent, invokingState);
      this.value = value;
    }

    @Override
    public int getRuleIndex() {
      return RULE_predicate;
    }

    public PredicateContext() {
    }

    public void copyFrom(PredicateContext ctx) {
      super.copyFrom(ctx);
      this.value = ctx.value;
    }
  }

  public static class ComparisonContext extends PredicateContext {

    public ValueExpressionContext right;

    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public ComparisonContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterComparison(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitComparison(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitComparison(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class LikeContext extends PredicateContext {

    public ValueExpressionContext pattern;
    public ValueExpressionContext escape;

    public TerminalNode LIKE() {
      return getToken(SqlBaseParser.LIKE, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public TerminalNode ESCAPE() {
      return getToken(SqlBaseParser.ESCAPE, 0);
    }

    public LikeContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterLike(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitLike(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitLike(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class InSubqueryContext extends PredicateContext {

    public TerminalNode IN() {
      return getToken(SqlBaseParser.IN, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public InSubqueryContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterInSubquery(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitInSubquery(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitInSubquery(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class DistinctFromContext extends PredicateContext {

    public ValueExpressionContext right;

    public TerminalNode IS() {
      return getToken(SqlBaseParser.IS, 0);
    }

    public TerminalNode DISTINCT() {
      return getToken(SqlBaseParser.DISTINCT, 0);
    }

    public TerminalNode FROM() {
      return getToken(SqlBaseParser.FROM, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public DistinctFromContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterDistinctFrom(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitDistinctFrom(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitDistinctFrom(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class InListContext extends PredicateContext {

    public TerminalNode IN() {
      return getToken(SqlBaseParser.IN, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public InListContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterInList(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitInList(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitInList(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class NullPredicateContext extends PredicateContext {

    public TerminalNode IS() {
      return getToken(SqlBaseParser.IS, 0);
    }

    public TerminalNode NULL() {
      return getToken(SqlBaseParser.NULL, 0);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public NullPredicateContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNullPredicate(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNullPredicate(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNullPredicate(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class BetweenContext extends PredicateContext {

    public ValueExpressionContext lower;
    public ValueExpressionContext upper;

    public TerminalNode BETWEEN() {
      return getToken(SqlBaseParser.BETWEEN, 0);
    }

    public TerminalNode AND() {
      return getToken(SqlBaseParser.AND, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode NOT() {
      return getToken(SqlBaseParser.NOT, 0);
    }

    public BetweenContext(PredicateContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBetween(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBetween(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBetween(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
    PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
    enterRule(_localctx, 62, RULE_predicate);
    int _la;
    try {
      setState(629);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 79, _ctx)) {
        case 1:
          _localctx = new ComparisonContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(574);
          comparisonOperator();
          setState(575);
          ((ComparisonContext) _localctx).right = valueExpression(0);
        }
        break;
        case 2:
          _localctx = new BetweenContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(578);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(577);
              match(NOT);
            }
          }

          setState(580);
          match(BETWEEN);
          setState(581);
          ((BetweenContext) _localctx).lower = valueExpression(0);
          setState(582);
          match(AND);
          setState(583);
          ((BetweenContext) _localctx).upper = valueExpression(0);
        }
        break;
        case 3:
          _localctx = new InListContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(586);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(585);
              match(NOT);
            }
          }

          setState(588);
          match(IN);
          setState(589);
          match(T__1);
          setState(590);
          expression();
          setState(595);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__2) {
            {
              {
                setState(591);
                match(T__2);
                setState(592);
                expression();
              }
            }
            setState(597);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(598);
          match(T__3);
        }
        break;
        case 4:
          _localctx = new InSubqueryContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(601);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(600);
              match(NOT);
            }
          }

          setState(603);
          match(IN);
          setState(604);
          match(T__1);
          setState(605);
          query();
          setState(606);
          match(T__3);
        }
        break;
        case 5:
          _localctx = new LikeContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(609);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(608);
              match(NOT);
            }
          }

          setState(611);
          match(LIKE);
          setState(612);
          ((LikeContext) _localctx).pattern = valueExpression(0);
          setState(615);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 76, _ctx)) {
            case 1: {
              setState(613);
              match(ESCAPE);
              setState(614);
              ((LikeContext) _localctx).escape = valueExpression(0);
            }
            break;
          }
        }
        break;
        case 6:
          _localctx = new NullPredicateContext(_localctx);
          enterOuterAlt(_localctx, 6);
        {
          setState(617);
          match(IS);
          setState(619);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(618);
              match(NOT);
            }
          }

          setState(621);
          match(NULL);
        }
        break;
        case 7:
          _localctx = new DistinctFromContext(_localctx);
          enterOuterAlt(_localctx, 7);
        {
          setState(622);
          match(IS);
          setState(624);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(623);
              match(NOT);
            }
          }

          setState(626);
          match(DISTINCT);
          setState(627);
          match(FROM);
          setState(628);
          ((DistinctFromContext) _localctx).right = valueExpression(0);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ValueExpressionContext extends ParserRuleContext {

    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_valueExpression;
    }

    public ValueExpressionContext() {
    }

    public void copyFrom(ValueExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ValueExpressionDefaultContext extends ValueExpressionContext {

    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public ValueExpressionDefaultContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterValueExpressionDefault(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitValueExpressionDefault(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitValueExpressionDefault(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ConcatenationContext extends ValueExpressionContext {

    public ValueExpressionContext left;
    public ValueExpressionContext right;

    public TerminalNode CONCAT() {
      return getToken(SqlBaseParser.CONCAT, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public ConcatenationContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterConcatenation(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitConcatenation(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitConcatenation(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ArithmeticBinaryContext extends ValueExpressionContext {

    public ValueExpressionContext left;
    public Token operator;
    public ValueExpressionContext right;

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode ASTERISK() {
      return getToken(SqlBaseParser.ASTERISK, 0);
    }

    public TerminalNode SLASH() {
      return getToken(SqlBaseParser.SLASH, 0);
    }

    public TerminalNode PERCENT() {
      return getToken(SqlBaseParser.PERCENT, 0);
    }

    public TerminalNode PLUS() {
      return getToken(SqlBaseParser.PLUS, 0);
    }

    public TerminalNode MINUS() {
      return getToken(SqlBaseParser.MINUS, 0);
    }

    public ArithmeticBinaryContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterArithmeticBinary(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitArithmeticBinary(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitArithmeticBinary(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ArithmeticUnaryContext extends ValueExpressionContext {

    public Token operator;

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public TerminalNode MINUS() {
      return getToken(SqlBaseParser.MINUS, 0);
    }

    public TerminalNode PLUS() {
      return getToken(SqlBaseParser.PLUS, 0);
    }

    public ArithmeticUnaryContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterArithmeticUnary(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitArithmeticUnary(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitArithmeticUnary(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class AtTimeZoneContext extends ValueExpressionContext {

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public TerminalNode AT() {
      return getToken(SqlBaseParser.AT, 0);
    }

    public TimeZoneSpecifierContext timeZoneSpecifier() {
      return getRuleContext(TimeZoneSpecifierContext.class, 0);
    }

    public AtTimeZoneContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterAtTimeZone(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitAtTimeZone(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitAtTimeZone(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    return valueExpression(0);
  }

  private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
    ValueExpressionContext _prevctx = _localctx;
    int _startState = 64;
    enterRecursionRule(_localctx, 64, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(635);
        switch (_input.LA(1)) {
          case T__1:
          case ADD:
          case APPROXIMATE:
          case AT:
          case CONFIDENCE:
          case NO:
          case EXISTS:
          case NULL:
          case TRUE:
          case FALSE:
          case SUBSTRING:
          case POSITION:
          case TINYINT:
          case SMALLINT:
          case INTEGER:
          case DATE:
          case TIME:
          case TIMESTAMP:
          case INTERVAL:
          case YEAR:
          case MONTH:
          case DAY:
          case HOUR:
          case MINUTE:
          case SECOND:
          case ZONE:
          case CURRENT_DATE:
          case CURRENT_TIME:
          case CURRENT_TIMESTAMP:
          case LOCALTIME:
          case LOCALTIMESTAMP:
          case EXTRACT:
          case CASE:
          case OVER:
          case PARTITION:
          case RANGE:
          case ROWS:
          case PRECEDING:
          case FOLLOWING:
          case CURRENT:
          case ROW:
          case VIEW:
          case REPLACE:
          case GRANT:
          case REVOKE:
          case PRIVILEGES:
          case PUBLIC:
          case OPTION:
          case EXPLAIN:
          case ANALYZE:
          case FORMAT:
          case TYPE:
          case TEXT:
          case GRAPHVIZ:
          case LOGICAL:
          case DISTRIBUTED:
          case TRY:
          case CAST:
          case TRY_CAST:
          case SHOW:
          case TABLES:
          case SCHEMAS:
          case CATALOGS:
          case COLUMNS:
          case COLUMN:
          case USE:
          case PARTITIONS:
          case FUNCTIONS:
          case TO:
          case SYSTEM:
          case BERNOULLI:
          case POISSONIZED:
          case TABLESAMPLE:
          case RESCALED:
          case ARRAY:
          case MAP:
          case SET:
          case RESET:
          case SESSION:
          case DATA:
          case START:
          case TRANSACTION:
          case COMMIT:
          case ROLLBACK:
          case WORK:
          case ISOLATION:
          case LEVEL:
          case SERIALIZABLE:
          case REPEATABLE:
          case COMMITTED:
          case UNCOMMITTED:
          case READ:
          case WRITE:
          case ONLY:
          case CALL:
          case NORMALIZE:
          case NFD:
          case NFC:
          case NFKD:
          case NFKC:
          case IF:
          case NULLIF:
          case COALESCE:
          case STRING:
          case BINARY_LITERAL:
          case INTEGER_VALUE:
          case DECIMAL_VALUE:
          case IDENTIFIER:
          case DIGIT_IDENTIFIER:
          case QUOTED_IDENTIFIER:
          case BACKQUOTED_IDENTIFIER: {
            _localctx = new ValueExpressionDefaultContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(632);
            primaryExpression(0);
          }
          break;
          case PLUS:
          case MINUS: {
            _localctx = new ArithmeticUnaryContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(633);
            ((ArithmeticUnaryContext) _localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == PLUS || _la == MINUS)) {
              ((ArithmeticUnaryContext) _localctx).operator =
                  (Token) _errHandler.recoverInline(this);
            } else {
              consume();
            }
            setState(634);
            valueExpression(4);
          }
          break;
          default:
            throw new NoViableAltException(this);
        }
        _ctx.stop = _input.LT(-1);
        setState(651);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 82, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              setState(649);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 81, _ctx)) {
                case 1: {
                  _localctx =
                      new ArithmeticBinaryContext(
                          new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(637);
                  if (!(precpred(_ctx, 3))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                  }
                  setState(638);
                  ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(((((_la - 196)) & ~0x3f) == 0 &&
                         ((1L << (_la - 196)) & ((1L << (ASTERISK - 196)) | (1L << (SLASH - 196))
                                                 | (1L << (PERCENT - 196)))) != 0))) {
                    ((ArithmeticBinaryContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    consume();
                  }
                  setState(639);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(4);
                }
                break;
                case 2: {
                  _localctx =
                      new ArithmeticBinaryContext(
                          new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(640);
                  if (!(precpred(_ctx, 2))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  }
                  setState(641);
                  ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(_la == PLUS || _la == MINUS)) {
                    ((ArithmeticBinaryContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    consume();
                  }
                  setState(642);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(3);
                }
                break;
                case 3: {
                  _localctx =
                      new ConcatenationContext(
                          new ValueExpressionContext(_parentctx, _parentState));
                  ((ConcatenationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(643);
                  if (!(precpred(_ctx, 1))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  }
                  setState(644);
                  match(CONCAT);
                  setState(645);
                  ((ConcatenationContext) _localctx).right = valueExpression(2);
                }
                break;
                case 4: {
                  _localctx =
                      new AtTimeZoneContext(new ValueExpressionContext(_parentctx, _parentState));
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(646);
                  if (!(precpred(_ctx, 5))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                  }
                  setState(647);
                  match(AT);
                  setState(648);
                  timeZoneSpecifier();
                }
                break;
              }
            }
          }
          setState(653);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 82, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PrimaryExpressionContext extends ParserRuleContext {

    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_primaryExpression;
    }

    public PrimaryExpressionContext() {
    }

    public void copyFrom(PrimaryExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class DereferenceContext extends PrimaryExpressionContext {

    public PrimaryExpressionContext base;
    public IdentifierContext fieldName;

    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public DereferenceContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterDereference(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitDereference(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitDereference(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class TypeConstructorContext extends PrimaryExpressionContext {

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(SqlBaseParser.STRING, 0);
    }

    public TypeConstructorContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTypeConstructor(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTypeConstructor(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTypeConstructor(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SpecialDateTimeFunctionContext extends PrimaryExpressionContext {

    public Token name;
    public Token precision;

    public TerminalNode CURRENT_DATE() {
      return getToken(SqlBaseParser.CURRENT_DATE, 0);
    }

    public TerminalNode CURRENT_TIME() {
      return getToken(SqlBaseParser.CURRENT_TIME, 0);
    }

    public TerminalNode INTEGER_VALUE() {
      return getToken(SqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0);
    }

    public TerminalNode LOCALTIME() {
      return getToken(SqlBaseParser.LOCALTIME, 0);
    }

    public TerminalNode LOCALTIMESTAMP() {
      return getToken(SqlBaseParser.LOCALTIMESTAMP, 0);
    }

    public SpecialDateTimeFunctionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSpecialDateTimeFunction(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSpecialDateTimeFunction(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSpecialDateTimeFunction(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SubstringContext extends PrimaryExpressionContext {

    public TerminalNode SUBSTRING() {
      return getToken(SqlBaseParser.SUBSTRING, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode FROM() {
      return getToken(SqlBaseParser.FROM, 0);
    }

    public TerminalNode FOR() {
      return getToken(SqlBaseParser.FOR, 0);
    }

    public SubstringContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSubstring(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSubstring(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSubstring(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class CastContext extends PrimaryExpressionContext {

    public TerminalNode CAST() {
      return getToken(SqlBaseParser.CAST, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(SqlBaseParser.AS, 0);
    }

    public TypeContext type() {
      return getRuleContext(TypeContext.class, 0);
    }

    public TerminalNode TRY_CAST() {
      return getToken(SqlBaseParser.TRY_CAST, 0);
    }

    public CastContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterCast(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitCast(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitCast(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class LambdaContext extends PrimaryExpressionContext {

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public LambdaContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterLambda(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitLambda(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitLambda(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterParenthesizedExpression(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitParenthesizedExpression(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitParenthesizedExpression(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class NormalizeContext extends PrimaryExpressionContext {

    public TerminalNode NORMALIZE() {
      return getToken(SqlBaseParser.NORMALIZE, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public NormalFormContext normalForm() {
      return getRuleContext(NormalFormContext.class, 0);
    }

    public NormalizeContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNormalize(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNormalize(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNormalize(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class IntervalLiteralContext extends PrimaryExpressionContext {

    public IntervalContext interval() {
      return getRuleContext(IntervalContext.class, 0);
    }

    public IntervalLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterIntervalLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitIntervalLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitIntervalLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class NumericLiteralContext extends PrimaryExpressionContext {

    public NumberContext number() {
      return getRuleContext(NumberContext.class, 0);
    }

    public NumericLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNumericLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNumericLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNumericLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class BooleanLiteralContext extends PrimaryExpressionContext {

    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class, 0);
    }

    public BooleanLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBooleanLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBooleanLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBooleanLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SimpleCaseContext extends PrimaryExpressionContext {

    public ExpressionContext elseExpression;

    public TerminalNode CASE() {
      return getToken(SqlBaseParser.CASE, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public TerminalNode END() {
      return getToken(SqlBaseParser.END, 0);
    }

    public List<WhenClauseContext> whenClause() {
      return getRuleContexts(WhenClauseContext.class);
    }

    public WhenClauseContext whenClause(int i) {
      return getRuleContext(WhenClauseContext.class, i);
    }

    public TerminalNode ELSE() {
      return getToken(SqlBaseParser.ELSE, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public SimpleCaseContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSimpleCase(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSimpleCase(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSimpleCase(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ColumnReferenceContext extends PrimaryExpressionContext {

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ColumnReferenceContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterColumnReference(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitColumnReference(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitColumnReference(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class NullLiteralContext extends PrimaryExpressionContext {

    public TerminalNode NULL() {
      return getToken(SqlBaseParser.NULL, 0);
    }

    public NullLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNullLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNullLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNullLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class RowConstructorContext extends PrimaryExpressionContext {

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode ROW() {
      return getToken(SqlBaseParser.ROW, 0);
    }

    public RowConstructorContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterRowConstructor(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitRowConstructor(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitRowConstructor(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SubscriptContext extends PrimaryExpressionContext {

    public PrimaryExpressionContext value;
    public ValueExpressionContext index;

    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public SubscriptContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSubscript(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSubscript(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSubscript(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SubqueryExpressionContext extends PrimaryExpressionContext {

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public SubqueryExpressionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSubqueryExpression(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSubqueryExpression(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSubqueryExpression(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class BinaryLiteralContext extends PrimaryExpressionContext {

    public TerminalNode BINARY_LITERAL() {
      return getToken(SqlBaseParser.BINARY_LITERAL, 0);
    }

    public BinaryLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBinaryLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBinaryLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBinaryLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ExtractContext extends PrimaryExpressionContext {

    public TerminalNode EXTRACT() {
      return getToken(SqlBaseParser.EXTRACT, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(SqlBaseParser.FROM, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public ExtractContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterExtract(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitExtract(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitExtract(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class StringLiteralContext extends PrimaryExpressionContext {

    public TerminalNode STRING() {
      return getToken(SqlBaseParser.STRING, 0);
    }

    public StringLiteralContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterStringLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitStringLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitStringLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ArrayConstructorContext extends PrimaryExpressionContext {

    public TerminalNode ARRAY() {
      return getToken(SqlBaseParser.ARRAY, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public ArrayConstructorContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterArrayConstructor(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitArrayConstructor(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitArrayConstructor(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class FunctionCallContext extends PrimaryExpressionContext {

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode ASTERISK() {
      return getToken(SqlBaseParser.ASTERISK, 0);
    }

    public OverContext over() {
      return getRuleContext(OverContext.class, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public FunctionCallContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterFunctionCall(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitFunctionCall(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitFunctionCall(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ExistsContext extends PrimaryExpressionContext {

    public TerminalNode EXISTS() {
      return getToken(SqlBaseParser.EXISTS, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public ExistsContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterExists(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitExists(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitExists(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class PositionContext extends PrimaryExpressionContext {

    public TerminalNode POSITION() {
      return getToken(SqlBaseParser.POSITION, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode IN() {
      return getToken(SqlBaseParser.IN, 0);
    }

    public PositionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterPosition(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitPosition(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitPosition(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SearchedCaseContext extends PrimaryExpressionContext {

    public ExpressionContext elseExpression;

    public TerminalNode CASE() {
      return getToken(SqlBaseParser.CASE, 0);
    }

    public TerminalNode END() {
      return getToken(SqlBaseParser.END, 0);
    }

    public List<WhenClauseContext> whenClause() {
      return getRuleContexts(WhenClauseContext.class);
    }

    public WhenClauseContext whenClause(int i) {
      return getRuleContext(WhenClauseContext.class, i);
    }

    public TerminalNode ELSE() {
      return getToken(SqlBaseParser.ELSE, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public SearchedCaseContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSearchedCase(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSearchedCase(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSearchedCase(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    return primaryExpression(0);
  }

  private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
    PrimaryExpressionContext _prevctx = _localctx;
    int _startState = 66;
    enterRecursionRule(_localctx, 66, RULE_primaryExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(854);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 103, _ctx)) {
          case 1: {
            _localctx = new NullLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(655);
            match(NULL);
          }
          break;
          case 2: {
            _localctx = new IntervalLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(656);
            interval();
          }
          break;
          case 3: {
            _localctx = new TypeConstructorContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(657);
            identifier();
            setState(658);
            match(STRING);
          }
          break;
          case 4: {
            _localctx = new NumericLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(660);
            number();
          }
          break;
          case 5: {
            _localctx = new BooleanLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(661);
            booleanValue();
          }
          break;
          case 6: {
            _localctx = new StringLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(662);
            match(STRING);
          }
          break;
          case 7: {
            _localctx = new BinaryLiteralContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(663);
            match(BINARY_LITERAL);
          }
          break;
          case 8: {
            _localctx = new PositionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(664);
            match(POSITION);
            setState(665);
            match(T__1);
            setState(666);
            valueExpression(0);
            setState(667);
            match(IN);
            setState(668);
            valueExpression(0);
            setState(669);
            match(T__3);
          }
          break;
          case 9: {
            _localctx = new RowConstructorContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(671);
            match(T__1);
            setState(672);
            expression();
            setState(675);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(673);
                  match(T__2);
                  setState(674);
                  expression();
                }
              }
              setState(677);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == T__2);
            setState(679);
            match(T__3);
          }
          break;
          case 10: {
            _localctx = new RowConstructorContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(681);
            match(ROW);
            setState(682);
            match(T__1);
            setState(683);
            expression();
            setState(688);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(684);
                  match(T__2);
                  setState(685);
                  expression();
                }
              }
              setState(690);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
            setState(691);
            match(T__3);
          }
          break;
          case 11: {
            _localctx = new FunctionCallContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(693);
            qualifiedName();
            setState(694);
            match(T__1);
            setState(695);
            match(ASTERISK);
            setState(696);
            match(T__3);
            setState(698);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 85, _ctx)) {
              case 1: {
                setState(697);
                over();
              }
              break;
            }
          }
          break;
          case 12: {
            _localctx = new FunctionCallContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(700);
            qualifiedName();
            setState(701);
            match(T__1);
            setState(713);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 &&
                 ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << ALL) | (1L << DISTINCT) | (1L
                                                                                                << APPROXIMATE)
                                 | (1L << AT) | (1L << CONFIDENCE) | (1L << NOT) | (1L << NO) | (1L
                                                                                                 << EXISTS)
                                 | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << SUBSTRING)
                                 | (1L << POSITION) | (1L << TINYINT) | (1L << SMALLINT) | (1L
                                                                                            << INTEGER)
                                 | (1L << DATE) | (1L << TIME) | (1L << TIMESTAMP) | (1L
                                                                                      << INTERVAL)
                                 | (1L << YEAR) | (1L << MONTH) | (1L << DAY) | (1L << HOUR) | (1L
                                                                                                << MINUTE)))
                 != 0) || ((((_la - 64)) & ~0x3f) == 0 &&
                           ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L
                                                                                                 << (CURRENT_DATE
                                                                                                     - 64))
                                                  | (1L << (CURRENT_TIME - 64)) | (1L << (
                               CURRENT_TIMESTAMP - 64)) | (1L << (LOCALTIME - 64)) | (1L << (
                               LOCALTIMESTAMP - 64)) | (1L << (EXTRACT - 64)) | (1L << (CASE - 64))
                                                  | (1L << (OVER - 64)) | (1L << (PARTITION - 64))
                                                  | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L
                                                                                                  << (PRECEDING
                                                                                                      - 64))
                                                  | (1L << (FOLLOWING - 64)) | (1L << (CURRENT
                                                                                       - 64)) | (1L
                                                                                                 << (ROW
                                                                                                     - 64))
                                                  | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (
                                                      1L << (GRANT - 64)) | (1L << (REVOKE - 64))
                                                  | (1L << (PRIVILEGES - 64)) | (1L << (PUBLIC
                                                                                        - 64)) | (1L
                                                                                                  << (OPTION
                                                                                                      - 64))
                                                  | (1L << (EXPLAIN - 64)) | (1L << (ANALYZE - 64))
                                                  | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (
                                                      1L << (TEXT - 64)) | (1L << (GRAPHVIZ - 64))
                                                  | (1L << (LOGICAL - 64)) | (1L << (DISTRIBUTED
                                                                                     - 64)) | (1L
                                                                                               << (TRY
                                                                                                   - 64))
                                                  | (1L << (CAST - 64)) | (1L << (TRY_CAST - 64))
                                                  | (1L << (SHOW - 64)))) != 0) || (
                    (((_la - 128)) & ~0x3f) == 0 &&
                    ((1L << (_la - 128)) & ((1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L
                                                                                                << (CATALOGS
                                                                                                    - 128))
                                            | (1L << (COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L
                                                                                                  << (USE
                                                                                                      - 128))
                                            | (1L << (PARTITIONS - 128)) | (1L << (FUNCTIONS - 128))
                                            | (1L << (TO - 128)) | (1L << (SYSTEM - 128)) | (1L << (
                        BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L << (TABLESAMPLE
                                                                                  - 128)) | (1L << (
                        RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET
                                                                                                 - 128))
                                            | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L
                                                                                                 << (DATA
                                                                                                     - 128))
                                            | (1L << (START - 128)) | (1L << (TRANSACTION - 128))
                                            | (1L << (COMMIT - 128)) | (1L << (ROLLBACK - 128)) | (
                                                1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (
                                                1L << (LEVEL - 128)) | (1L << (SERIALIZABLE - 128))
                                            | (1L << (REPEATABLE - 128)) | (1L << (COMMITTED - 128))
                                            | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (
                                                1L << (WRITE - 128)) | (1L << (ONLY - 128)) | (1L
                                                                                               << (CALL
                                                                                                   - 128))
                                            | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L
                                                                                                 << (NFC
                                                                                                     - 128))
                                            | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (
                        IF - 128)) | (1L << (NULLIF - 128)) | (1L << (COALESCE - 128)))) != 0) || (
                    (((_la - 194)) & ~0x3f) == 0 &&
                    ((1L << (_la - 194)) & ((1L << (PLUS - 194)) | (1L << (MINUS - 194)) | (1L << (
                        STRING - 194)) | (1L << (BINARY_LITERAL - 194)) | (1L << (INTEGER_VALUE
                                                                                  - 194)) | (1L << (
                        DECIMAL_VALUE - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (
                        DIGIT_IDENTIFIER - 194)) | (1L << (QUOTED_IDENTIFIER - 194)) | (1L << (
                        BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
              {
                setState(703);
                _la = _input.LA(1);
                if (_la == ALL || _la == DISTINCT) {
                  {
                    setState(702);
                    setQuantifier();
                  }
                }

                setState(705);
                expression();
                setState(710);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == T__2) {
                  {
                    {
                      setState(706);
                      match(T__2);
                      setState(707);
                      expression();
                    }
                  }
                  setState(712);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                }
              }
            }

            setState(715);
            match(T__3);
            setState(717);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 89, _ctx)) {
              case 1: {
                setState(716);
                over();
              }
              break;
            }
          }
          break;
          case 13: {
            _localctx = new LambdaContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(719);
            identifier();
            setState(720);
            match(T__5);
            setState(721);
            expression();
          }
          break;
          case 14: {
            _localctx = new LambdaContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(723);
            match(T__1);
            setState(724);
            identifier();
            setState(729);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(725);
                  match(T__2);
                  setState(726);
                  identifier();
                }
              }
              setState(731);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
            setState(732);
            match(T__3);
            setState(733);
            match(T__5);
            setState(734);
            expression();
          }
          break;
          case 15: {
            _localctx = new SubqueryExpressionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(736);
            match(T__1);
            setState(737);
            query();
            setState(738);
            match(T__3);
          }
          break;
          case 16: {
            _localctx = new ExistsContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(740);
            match(EXISTS);
            setState(741);
            match(T__1);
            setState(742);
            query();
            setState(743);
            match(T__3);
          }
          break;
          case 17: {
            _localctx = new SimpleCaseContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(745);
            match(CASE);
            setState(746);
            valueExpression(0);
            setState(748);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(747);
                  whenClause();
                }
              }
              setState(750);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(754);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(752);
                match(ELSE);
                setState(753);
                ((SimpleCaseContext) _localctx).elseExpression = expression();
              }
            }

            setState(756);
            match(END);
          }
          break;
          case 18: {
            _localctx = new SearchedCaseContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(758);
            match(CASE);
            setState(760);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(759);
                  whenClause();
                }
              }
              setState(762);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(766);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(764);
                match(ELSE);
                setState(765);
                ((SearchedCaseContext) _localctx).elseExpression = expression();
              }
            }

            setState(768);
            match(END);
          }
          break;
          case 19: {
            _localctx = new CastContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(770);
            match(CAST);
            setState(771);
            match(T__1);
            setState(772);
            expression();
            setState(773);
            match(AS);
            setState(774);
            type(0);
            setState(775);
            match(T__3);
          }
          break;
          case 20: {
            _localctx = new CastContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(777);
            match(TRY_CAST);
            setState(778);
            match(T__1);
            setState(779);
            expression();
            setState(780);
            match(AS);
            setState(781);
            type(0);
            setState(782);
            match(T__3);
          }
          break;
          case 21: {
            _localctx = new ArrayConstructorContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(784);
            match(ARRAY);
            setState(785);
            match(T__6);
            setState(794);
            _la = _input.LA(1);
            if ((((_la) & ~0x3f) == 0 &&
                 ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << APPROXIMATE) | (1L << AT) | (1L
                                                                                                  << CONFIDENCE)
                                 | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << NULL) | (1L
                                                                                               << TRUE)
                                 | (1L << FALSE) | (1L << SUBSTRING) | (1L << POSITION) | (1L
                                                                                           << TINYINT)
                                 | (1L << SMALLINT) | (1L << INTEGER) | (1L << DATE) | (1L << TIME)
                                 | (1L << TIMESTAMP) | (1L << INTERVAL) | (1L << YEAR) | (1L
                                                                                          << MONTH)
                                 | (1L << DAY) | (1L << HOUR) | (1L << MINUTE))) != 0) || (
                    (((_la - 64)) & ~0x3f) == 0 &&
                    ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L << (
                        CURRENT_DATE - 64)) | (1L << (CURRENT_TIME - 64)) | (1L << (
                        CURRENT_TIMESTAMP - 64)) | (1L << (LOCALTIME - 64)) | (1L << (LOCALTIMESTAMP
                                                                                      - 64)) | (1L
                                                                                                << (EXTRACT
                                                                                                    - 64))
                                           | (1L << (CASE - 64)) | (1L << (OVER - 64)) | (1L << (
                        PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (
                        PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L
                                                                                                << (ROW
                                                                                                    - 64))
                                           | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (
                        GRANT - 64)) | (1L << (REVOKE - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (
                        PUBLIC - 64)) | (1L << (OPTION - 64)) | (1L << (EXPLAIN - 64)) | (1L << (
                        ANALYZE - 64)) | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (1L << (TEXT
                                                                                               - 64))
                                           | (1L << (GRAPHVIZ - 64)) | (1L << (LOGICAL - 64)) | (1L
                                                                                                 << (DISTRIBUTED
                                                                                                     - 64))
                                           | (1L << (TRY - 64)) | (1L << (CAST - 64)) | (1L << (
                        TRY_CAST - 64)) | (1L << (SHOW - 64)))) != 0) || (
                    (((_la - 128)) & ~0x3f) == 0 &&
                    ((1L << (_la - 128)) & ((1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L
                                                                                                << (CATALOGS
                                                                                                    - 128))
                                            | (1L << (COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L
                                                                                                  << (USE
                                                                                                      - 128))
                                            | (1L << (PARTITIONS - 128)) | (1L << (FUNCTIONS - 128))
                                            | (1L << (TO - 128)) | (1L << (SYSTEM - 128)) | (1L << (
                        BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L << (TABLESAMPLE
                                                                                  - 128)) | (1L << (
                        RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET
                                                                                                 - 128))
                                            | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L
                                                                                                 << (DATA
                                                                                                     - 128))
                                            | (1L << (START - 128)) | (1L << (TRANSACTION - 128))
                                            | (1L << (COMMIT - 128)) | (1L << (ROLLBACK - 128)) | (
                                                1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (
                                                1L << (LEVEL - 128)) | (1L << (SERIALIZABLE - 128))
                                            | (1L << (REPEATABLE - 128)) | (1L << (COMMITTED - 128))
                                            | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (
                                                1L << (WRITE - 128)) | (1L << (ONLY - 128)) | (1L
                                                                                               << (CALL
                                                                                                   - 128))
                                            | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L
                                                                                                 << (NFC
                                                                                                     - 128))
                                            | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (
                        IF - 128)) | (1L << (NULLIF - 128)) | (1L << (COALESCE - 128)))) != 0) || (
                    (((_la - 194)) & ~0x3f) == 0 &&
                    ((1L << (_la - 194)) & ((1L << (PLUS - 194)) | (1L << (MINUS - 194)) | (1L << (
                        STRING - 194)) | (1L << (BINARY_LITERAL - 194)) | (1L << (INTEGER_VALUE
                                                                                  - 194)) | (1L << (
                        DECIMAL_VALUE - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (
                        DIGIT_IDENTIFIER - 194)) | (1L << (QUOTED_IDENTIFIER - 194)) | (1L << (
                        BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
              {
                setState(786);
                expression();
                setState(791);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == T__2) {
                  {
                    {
                      setState(787);
                      match(T__2);
                      setState(788);
                      expression();
                    }
                  }
                  setState(793);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                }
              }
            }

            setState(796);
            match(T__7);
          }
          break;
          case 22: {
            _localctx = new ColumnReferenceContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(797);
            identifier();
          }
          break;
          case 23: {
            _localctx = new SpecialDateTimeFunctionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(798);
            ((SpecialDateTimeFunctionContext) _localctx).name = match(CURRENT_DATE);
          }
          break;
          case 24: {
            _localctx = new SpecialDateTimeFunctionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(799);
            ((SpecialDateTimeFunctionContext) _localctx).name = match(CURRENT_TIME);
            setState(803);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 97, _ctx)) {
              case 1: {
                setState(800);
                match(T__1);
                setState(801);
                ((SpecialDateTimeFunctionContext) _localctx).precision = match(INTEGER_VALUE);
                setState(802);
                match(T__3);
              }
              break;
            }
          }
          break;
          case 25: {
            _localctx = new SpecialDateTimeFunctionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(805);
            ((SpecialDateTimeFunctionContext) _localctx).name = match(CURRENT_TIMESTAMP);
            setState(809);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 98, _ctx)) {
              case 1: {
                setState(806);
                match(T__1);
                setState(807);
                ((SpecialDateTimeFunctionContext) _localctx).precision = match(INTEGER_VALUE);
                setState(808);
                match(T__3);
              }
              break;
            }
          }
          break;
          case 26: {
            _localctx = new SpecialDateTimeFunctionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(811);
            ((SpecialDateTimeFunctionContext) _localctx).name = match(LOCALTIME);
            setState(815);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 99, _ctx)) {
              case 1: {
                setState(812);
                match(T__1);
                setState(813);
                ((SpecialDateTimeFunctionContext) _localctx).precision = match(INTEGER_VALUE);
                setState(814);
                match(T__3);
              }
              break;
            }
          }
          break;
          case 27: {
            _localctx = new SpecialDateTimeFunctionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(817);
            ((SpecialDateTimeFunctionContext) _localctx).name = match(LOCALTIMESTAMP);
            setState(821);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 100, _ctx)) {
              case 1: {
                setState(818);
                match(T__1);
                setState(819);
                ((SpecialDateTimeFunctionContext) _localctx).precision = match(INTEGER_VALUE);
                setState(820);
                match(T__3);
              }
              break;
            }
          }
          break;
          case 28: {
            _localctx = new SubstringContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(823);
            match(SUBSTRING);
            setState(824);
            match(T__1);
            setState(825);
            valueExpression(0);
            setState(826);
            match(FROM);
            setState(827);
            valueExpression(0);
            setState(830);
            _la = _input.LA(1);
            if (_la == FOR) {
              {
                setState(828);
                match(FOR);
                setState(829);
                valueExpression(0);
              }
            }

            setState(832);
            match(T__3);
          }
          break;
          case 29: {
            _localctx = new NormalizeContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(834);
            match(NORMALIZE);
            setState(835);
            match(T__1);
            setState(836);
            valueExpression(0);
            setState(839);
            _la = _input.LA(1);
            if (_la == T__2) {
              {
                setState(837);
                match(T__2);
                setState(838);
                normalForm();
              }
            }

            setState(841);
            match(T__3);
          }
          break;
          case 30: {
            _localctx = new ExtractContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(843);
            match(EXTRACT);
            setState(844);
            match(T__1);
            setState(845);
            identifier();
            setState(846);
            match(FROM);
            setState(847);
            valueExpression(0);
            setState(848);
            match(T__3);
          }
          break;
          case 31: {
            _localctx = new ParenthesizedExpressionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(850);
            match(T__1);
            setState(851);
            expression();
            setState(852);
            match(T__3);
          }
          break;
        }
        _ctx.stop = _input.LT(-1);
        setState(866);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 105, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              setState(864);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 104, _ctx)) {
                case 1: {
                  _localctx =
                      new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
                  ((SubscriptContext) _localctx).value = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
                  setState(856);
                  if (!(precpred(_ctx, 12))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 12)");
                  }
                  setState(857);
                  match(T__6);
                  setState(858);
                  ((SubscriptContext) _localctx).index = valueExpression(0);
                  setState(859);
                  match(T__7);
                }
                break;
                case 2: {
                  _localctx =
                      new DereferenceContext(
                          new PrimaryExpressionContext(_parentctx, _parentState));
                  ((DereferenceContext) _localctx).base = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
                  setState(861);
                  if (!(precpred(_ctx, 10))) {
                    throw new FailedPredicateException(this, "precpred(_ctx, 10)");
                  }
                  setState(862);
                  match(T__4);
                  setState(863);
                  ((DereferenceContext) _localctx).fieldName = identifier();
                }
                break;
              }
            }
          }
          setState(868);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 105, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class TimeZoneSpecifierContext extends ParserRuleContext {

    public TimeZoneSpecifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_timeZoneSpecifier;
    }

    public TimeZoneSpecifierContext() {
    }

    public void copyFrom(TimeZoneSpecifierContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class TimeZoneIntervalContext extends TimeZoneSpecifierContext {

    public TerminalNode TIME() {
      return getToken(SqlBaseParser.TIME, 0);
    }

    public TerminalNode ZONE() {
      return getToken(SqlBaseParser.ZONE, 0);
    }

    public IntervalContext interval() {
      return getRuleContext(IntervalContext.class, 0);
    }

    public TimeZoneIntervalContext(TimeZoneSpecifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTimeZoneInterval(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTimeZoneInterval(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTimeZoneInterval(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class TimeZoneStringContext extends TimeZoneSpecifierContext {

    public TerminalNode TIME() {
      return getToken(SqlBaseParser.TIME, 0);
    }

    public TerminalNode ZONE() {
      return getToken(SqlBaseParser.ZONE, 0);
    }

    public TerminalNode STRING() {
      return getToken(SqlBaseParser.STRING, 0);
    }

    public TimeZoneStringContext(TimeZoneSpecifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTimeZoneString(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTimeZoneString(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTimeZoneString(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TimeZoneSpecifierContext timeZoneSpecifier() throws RecognitionException {
    TimeZoneSpecifierContext _localctx = new TimeZoneSpecifierContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_timeZoneSpecifier);
    try {
      setState(875);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 106, _ctx)) {
        case 1:
          _localctx = new TimeZoneIntervalContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(869);
          match(TIME);
          setState(870);
          match(ZONE);
          setState(871);
          interval();
        }
        break;
        case 2:
          _localctx = new TimeZoneStringContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(872);
          match(TIME);
          setState(873);
          match(ZONE);
          setState(874);
          match(STRING);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ComparisonOperatorContext extends ParserRuleContext {

    public TerminalNode EQ() {
      return getToken(SqlBaseParser.EQ, 0);
    }

    public TerminalNode NEQ() {
      return getToken(SqlBaseParser.NEQ, 0);
    }

    public TerminalNode LT() {
      return getToken(SqlBaseParser.LT, 0);
    }

    public TerminalNode LTE() {
      return getToken(SqlBaseParser.LTE, 0);
    }

    public TerminalNode GT() {
      return getToken(SqlBaseParser.GT, 0);
    }

    public TerminalNode GTE() {
      return getToken(SqlBaseParser.GTE, 0);
    }

    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_comparisonOperator;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterComparisonOperator(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitComparisonOperator(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitComparisonOperator(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(877);
        _la = _input.LA(1);
        if (!(((((_la - 188)) & ~0x3f) == 0 &&
               ((1L << (_la - 188)) & ((1L << (EQ - 188)) | (1L << (NEQ - 188)) | (1L << (LT - 188))
                                       | (1L << (LTE - 188)) | (1L << (GT - 188)) | (1L << (GTE
                                                                                            - 188))))
               != 0))) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BooleanValueContext extends ParserRuleContext {

    public TerminalNode TRUE() {
      return getToken(SqlBaseParser.TRUE, 0);
    }

    public TerminalNode FALSE() {
      return getToken(SqlBaseParser.FALSE, 0);
    }

    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_booleanValue;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBooleanValue(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBooleanValue(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBooleanValue(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(879);
        _la = _input.LA(1);
        if (!(_la == TRUE || _la == FALSE)) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IntervalContext extends ParserRuleContext {

    public Token sign;
    public IntervalFieldContext from;
    public IntervalFieldContext to;

    public TerminalNode INTERVAL() {
      return getToken(SqlBaseParser.INTERVAL, 0);
    }

    public TerminalNode STRING() {
      return getToken(SqlBaseParser.STRING, 0);
    }

    public List<IntervalFieldContext> intervalField() {
      return getRuleContexts(IntervalFieldContext.class);
    }

    public IntervalFieldContext intervalField(int i) {
      return getRuleContext(IntervalFieldContext.class, i);
    }

    public TerminalNode TO() {
      return getToken(SqlBaseParser.TO, 0);
    }

    public TerminalNode PLUS() {
      return getToken(SqlBaseParser.PLUS, 0);
    }

    public TerminalNode MINUS() {
      return getToken(SqlBaseParser.MINUS, 0);
    }

    public IntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_interval;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterInterval(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitInterval(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitInterval(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final IntervalContext interval() throws RecognitionException {
    IntervalContext _localctx = new IntervalContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_interval);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(881);
        match(INTERVAL);
        setState(883);
        _la = _input.LA(1);
        if (_la == PLUS || _la == MINUS) {
          {
            setState(882);
            ((IntervalContext) _localctx).sign = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == PLUS || _la == MINUS)) {
              ((IntervalContext) _localctx).sign = (Token) _errHandler.recoverInline(this);
            } else {
              consume();
            }
          }
        }

        setState(885);
        match(STRING);
        setState(886);
        ((IntervalContext) _localctx).from = intervalField();
        setState(889);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 108, _ctx)) {
          case 1: {
            setState(887);
            match(TO);
            setState(888);
            ((IntervalContext) _localctx).to = intervalField();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IntervalFieldContext extends ParserRuleContext {

    public TerminalNode YEAR() {
      return getToken(SqlBaseParser.YEAR, 0);
    }

    public TerminalNode MONTH() {
      return getToken(SqlBaseParser.MONTH, 0);
    }

    public TerminalNode DAY() {
      return getToken(SqlBaseParser.DAY, 0);
    }

    public TerminalNode HOUR() {
      return getToken(SqlBaseParser.HOUR, 0);
    }

    public TerminalNode MINUTE() {
      return getToken(SqlBaseParser.MINUTE, 0);
    }

    public TerminalNode SECOND() {
      return getToken(SqlBaseParser.SECOND, 0);
    }

    public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_intervalField;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterIntervalField(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitIntervalField(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitIntervalField(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final IntervalFieldContext intervalField() throws RecognitionException {
    IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_intervalField);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(891);
        _la = _input.LA(1);
        if (!(((((_la - 59)) & ~0x3f) == 0 &&
               ((1L << (_la - 59)) & ((1L << (YEAR - 59)) | (1L << (MONTH - 59)) | (1L << (DAY
                                                                                           - 59))
                                      | (1L << (HOUR - 59)) | (1L << (MINUTE - 59)) | (1L << (SECOND
                                                                                              - 59))))
               != 0))) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TypeContext extends ParserRuleContext {

    public TerminalNode ARRAY() {
      return getToken(SqlBaseParser.ARRAY, 0);
    }

    public List<TypeContext> type() {
      return getRuleContexts(TypeContext.class);
    }

    public TypeContext type(int i) {
      return getRuleContext(TypeContext.class, i);
    }

    public TerminalNode MAP() {
      return getToken(SqlBaseParser.MAP, 0);
    }

    public TerminalNode ROW() {
      return getToken(SqlBaseParser.ROW, 0);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public BaseTypeContext baseType() {
      return getRuleContext(BaseTypeContext.class, 0);
    }

    public List<TypeParameterContext> typeParameter() {
      return getRuleContexts(TypeParameterContext.class);
    }

    public TypeParameterContext typeParameter(int i) {
      return getRuleContext(TypeParameterContext.class, i);
    }

    public TypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_type;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterType(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitType(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitType(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TypeContext type() throws RecognitionException {
    return type(0);
  }

  private TypeContext type(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    TypeContext _localctx = new TypeContext(_ctx, _parentState);
    TypeContext _prevctx = _localctx;
    int _startState = 78;
    enterRecursionRule(_localctx, 78, RULE_type, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(935);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 112, _ctx)) {
          case 1: {
            setState(894);
            match(ARRAY);
            setState(895);
            match(LT);
            setState(896);
            type(0);
            setState(897);
            match(GT);
          }
          break;
          case 2: {
            setState(899);
            match(MAP);
            setState(900);
            match(LT);
            setState(901);
            type(0);
            setState(902);
            match(T__2);
            setState(903);
            type(0);
            setState(904);
            match(GT);
          }
          break;
          case 3: {
            setState(906);
            match(ROW);
            setState(907);
            match(T__1);
            setState(908);
            identifier();
            setState(909);
            type(0);
            setState(916);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(910);
                  match(T__2);
                  setState(911);
                  identifier();
                  setState(912);
                  type(0);
                }
              }
              setState(918);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
            setState(919);
            match(T__3);
          }
          break;
          case 4: {
            setState(921);
            baseType();
            setState(933);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 111, _ctx)) {
              case 1: {
                setState(922);
                match(T__1);
                setState(923);
                typeParameter();
                setState(928);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == T__2) {
                  {
                    {
                      setState(924);
                      match(T__2);
                      setState(925);
                      typeParameter();
                    }
                  }
                  setState(930);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                }
                setState(931);
                match(T__3);
              }
              break;
            }
          }
          break;
        }
        _ctx.stop = _input.LT(-1);
        setState(941);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 113, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) {
              triggerExitRuleEvent();
            }
            _prevctx = _localctx;
            {
              {
                _localctx = new TypeContext(_parentctx, _parentState);
                pushNewRecursionContext(_localctx, _startState, RULE_type);
                setState(937);
                if (!(precpred(_ctx, 5))) {
                  throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                }
                setState(938);
                match(ARRAY);
              }
            }
          }
          setState(943);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 113, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class TypeParameterContext extends ParserRuleContext {

    public TerminalNode INTEGER_VALUE() {
      return getToken(SqlBaseParser.INTEGER_VALUE, 0);
    }

    public TypeContext type() {
      return getRuleContext(TypeContext.class, 0);
    }

    public TypeParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_typeParameter;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTypeParameter(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTypeParameter(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTypeParameter(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TypeParameterContext typeParameter() throws RecognitionException {
    TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_typeParameter);
    try {
      setState(946);
      switch (_input.LA(1)) {
        case INTEGER_VALUE:
          enterOuterAlt(_localctx, 1);
        {
          setState(944);
          match(INTEGER_VALUE);
        }
        break;
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NO:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
        case TIME_WITH_TIME_ZONE:
        case TIMESTAMP_WITH_TIME_ZONE:
          enterOuterAlt(_localctx, 2);
        {
          setState(945);
          type(0);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BaseTypeContext extends ParserRuleContext {

    public TerminalNode TIME_WITH_TIME_ZONE() {
      return getToken(SqlBaseParser.TIME_WITH_TIME_ZONE, 0);
    }

    public TerminalNode TIMESTAMP_WITH_TIME_ZONE() {
      return getToken(SqlBaseParser.TIMESTAMP_WITH_TIME_ZONE, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public BaseTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_baseType;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBaseType(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBaseType(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBaseType(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final BaseTypeContext baseType() throws RecognitionException {
    BaseTypeContext _localctx = new BaseTypeContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_baseType);
    try {
      setState(951);
      switch (_input.LA(1)) {
        case TIME_WITH_TIME_ZONE:
          enterOuterAlt(_localctx, 1);
        {
          setState(948);
          match(TIME_WITH_TIME_ZONE);
        }
        break;
        case TIMESTAMP_WITH_TIME_ZONE:
          enterOuterAlt(_localctx, 2);
        {
          setState(949);
          match(TIMESTAMP_WITH_TIME_ZONE);
        }
        break;
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NO:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          enterOuterAlt(_localctx, 3);
        {
          setState(950);
          identifier();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WhenClauseContext extends ParserRuleContext {

    public ExpressionContext condition;
    public ExpressionContext result;

    public TerminalNode WHEN() {
      return getToken(SqlBaseParser.WHEN, 0);
    }

    public TerminalNode THEN() {
      return getToken(SqlBaseParser.THEN, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public WhenClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_whenClause;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterWhenClause(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitWhenClause(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitWhenClause(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final WhenClauseContext whenClause() throws RecognitionException {
    WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
    enterRule(_localctx, 84, RULE_whenClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(953);
        match(WHEN);
        setState(954);
        ((WhenClauseContext) _localctx).condition = expression();
        setState(955);
        match(THEN);
        setState(956);
        ((WhenClauseContext) _localctx).result = expression();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class OverContext extends ParserRuleContext {

    public ExpressionContext expression;
    public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();

    public TerminalNode OVER() {
      return getToken(SqlBaseParser.OVER, 0);
    }

    public TerminalNode PARTITION() {
      return getToken(SqlBaseParser.PARTITION, 0);
    }

    public List<TerminalNode> BY() {
      return getTokens(SqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(SqlBaseParser.BY, i);
    }

    public TerminalNode ORDER() {
      return getToken(SqlBaseParser.ORDER, 0);
    }

    public List<SortItemContext> sortItem() {
      return getRuleContexts(SortItemContext.class);
    }

    public SortItemContext sortItem(int i) {
      return getRuleContext(SortItemContext.class, i);
    }

    public WindowFrameContext windowFrame() {
      return getRuleContext(WindowFrameContext.class, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public OverContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_over;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterOver(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitOver(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitOver(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final OverContext over() throws RecognitionException {
    OverContext _localctx = new OverContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_over);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(958);
        match(OVER);
        setState(959);
        match(T__1);
        setState(970);
        _la = _input.LA(1);
        if (_la == PARTITION) {
          {
            setState(960);
            match(PARTITION);
            setState(961);
            match(BY);
            setState(962);
            ((OverContext) _localctx).expression = expression();
            ((OverContext) _localctx).partition.add(((OverContext) _localctx).expression);
            setState(967);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(963);
                  match(T__2);
                  setState(964);
                  ((OverContext) _localctx).expression = expression();
                  ((OverContext) _localctx).partition.add(((OverContext) _localctx).expression);
                }
              }
              setState(969);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
        }

        setState(982);
        _la = _input.LA(1);
        if (_la == ORDER) {
          {
            setState(972);
            match(ORDER);
            setState(973);
            match(BY);
            setState(974);
            sortItem();
            setState(979);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__2) {
              {
                {
                  setState(975);
                  match(T__2);
                  setState(976);
                  sortItem();
                }
              }
              setState(981);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
        }

        setState(985);
        _la = _input.LA(1);
        if (_la == RANGE || _la == ROWS) {
          {
            setState(984);
            windowFrame();
          }
        }

        setState(987);
        match(T__3);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WindowFrameContext extends ParserRuleContext {

    public Token frameType;
    public FrameBoundContext start;
    public FrameBoundContext end;

    public TerminalNode RANGE() {
      return getToken(SqlBaseParser.RANGE, 0);
    }

    public List<FrameBoundContext> frameBound() {
      return getRuleContexts(FrameBoundContext.class);
    }

    public FrameBoundContext frameBound(int i) {
      return getRuleContext(FrameBoundContext.class, i);
    }

    public TerminalNode ROWS() {
      return getToken(SqlBaseParser.ROWS, 0);
    }

    public TerminalNode BETWEEN() {
      return getToken(SqlBaseParser.BETWEEN, 0);
    }

    public TerminalNode AND() {
      return getToken(SqlBaseParser.AND, 0);
    }

    public WindowFrameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_windowFrame;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterWindowFrame(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitWindowFrame(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitWindowFrame(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final WindowFrameContext windowFrame() throws RecognitionException {
    WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_windowFrame);
    try {
      setState(1005);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 121, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(989);
          ((WindowFrameContext) _localctx).frameType = match(RANGE);
          setState(990);
          ((WindowFrameContext) _localctx).start = frameBound();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(991);
          ((WindowFrameContext) _localctx).frameType = match(ROWS);
          setState(992);
          ((WindowFrameContext) _localctx).start = frameBound();
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(993);
          ((WindowFrameContext) _localctx).frameType = match(RANGE);
          setState(994);
          match(BETWEEN);
          setState(995);
          ((WindowFrameContext) _localctx).start = frameBound();
          setState(996);
          match(AND);
          setState(997);
          ((WindowFrameContext) _localctx).end = frameBound();
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(999);
          ((WindowFrameContext) _localctx).frameType = match(ROWS);
          setState(1000);
          match(BETWEEN);
          setState(1001);
          ((WindowFrameContext) _localctx).start = frameBound();
          setState(1002);
          match(AND);
          setState(1003);
          ((WindowFrameContext) _localctx).end = frameBound();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FrameBoundContext extends ParserRuleContext {

    public FrameBoundContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_frameBound;
    }

    public FrameBoundContext() {
    }

    public void copyFrom(FrameBoundContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class BoundedFrameContext extends FrameBoundContext {

    public Token boundType;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(SqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(SqlBaseParser.FOLLOWING, 0);
    }

    public BoundedFrameContext(FrameBoundContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBoundedFrame(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBoundedFrame(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBoundedFrame(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class UnboundedFrameContext extends FrameBoundContext {

    public Token boundType;

    public TerminalNode UNBOUNDED() {
      return getToken(SqlBaseParser.UNBOUNDED, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(SqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(SqlBaseParser.FOLLOWING, 0);
    }

    public UnboundedFrameContext(FrameBoundContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterUnboundedFrame(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitUnboundedFrame(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitUnboundedFrame(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class CurrentRowBoundContext extends FrameBoundContext {

    public TerminalNode CURRENT() {
      return getToken(SqlBaseParser.CURRENT, 0);
    }

    public TerminalNode ROW() {
      return getToken(SqlBaseParser.ROW, 0);
    }

    public CurrentRowBoundContext(FrameBoundContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterCurrentRowBound(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitCurrentRowBound(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitCurrentRowBound(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final FrameBoundContext frameBound() throws RecognitionException {
    FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
    enterRule(_localctx, 90, RULE_frameBound);
    int _la;
    try {
      setState(1016);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 122, _ctx)) {
        case 1:
          _localctx = new UnboundedFrameContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1007);
          match(UNBOUNDED);
          setState(1008);
          ((UnboundedFrameContext) _localctx).boundType = match(PRECEDING);
        }
        break;
        case 2:
          _localctx = new UnboundedFrameContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1009);
          match(UNBOUNDED);
          setState(1010);
          ((UnboundedFrameContext) _localctx).boundType = match(FOLLOWING);
        }
        break;
        case 3:
          _localctx = new CurrentRowBoundContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1011);
          match(CURRENT);
          setState(1012);
          match(ROW);
        }
        break;
        case 4:
          _localctx = new BoundedFrameContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1013);
          expression();
          setState(1014);
          ((BoundedFrameContext) _localctx).boundType = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == PRECEDING || _la == FOLLOWING)) {
            ((BoundedFrameContext) _localctx).boundType = (Token) _errHandler.recoverInline(this);
          } else {
            consume();
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExplainOptionContext extends ParserRuleContext {

    public ExplainOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_explainOption;
    }

    public ExplainOptionContext() {
    }

    public void copyFrom(ExplainOptionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ExplainFormatContext extends ExplainOptionContext {

    public Token value;

    public TerminalNode FORMAT() {
      return getToken(SqlBaseParser.FORMAT, 0);
    }

    public TerminalNode TEXT() {
      return getToken(SqlBaseParser.TEXT, 0);
    }

    public TerminalNode GRAPHVIZ() {
      return getToken(SqlBaseParser.GRAPHVIZ, 0);
    }

    public ExplainFormatContext(ExplainOptionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterExplainFormat(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitExplainFormat(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitExplainFormat(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ExplainTypeContext extends ExplainOptionContext {

    public Token value;

    public TerminalNode TYPE() {
      return getToken(SqlBaseParser.TYPE, 0);
    }

    public TerminalNode LOGICAL() {
      return getToken(SqlBaseParser.LOGICAL, 0);
    }

    public TerminalNode DISTRIBUTED() {
      return getToken(SqlBaseParser.DISTRIBUTED, 0);
    }

    public ExplainTypeContext(ExplainOptionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterExplainType(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitExplainType(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitExplainType(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final ExplainOptionContext explainOption() throws RecognitionException {
    ExplainOptionContext _localctx = new ExplainOptionContext(_ctx, getState());
    enterRule(_localctx, 92, RULE_explainOption);
    int _la;
    try {
      setState(1022);
      switch (_input.LA(1)) {
        case FORMAT:
          _localctx = new ExplainFormatContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1018);
          match(FORMAT);
          setState(1019);
          ((ExplainFormatContext) _localctx).value = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == TEXT || _la == GRAPHVIZ)) {
            ((ExplainFormatContext) _localctx).value = (Token) _errHandler.recoverInline(this);
          } else {
            consume();
          }
        }
        break;
        case TYPE:
          _localctx = new ExplainTypeContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1020);
          match(TYPE);
          setState(1021);
          ((ExplainTypeContext) _localctx).value = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == LOGICAL || _la == DISTRIBUTED)) {
            ((ExplainTypeContext) _localctx).value = (Token) _errHandler.recoverInline(this);
          } else {
            consume();
          }
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TransactionModeContext extends ParserRuleContext {

    public TransactionModeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transactionMode;
    }

    public TransactionModeContext() {
    }

    public void copyFrom(TransactionModeContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class TransactionAccessModeContext extends TransactionModeContext {

    public Token accessMode;

    public TerminalNode READ() {
      return getToken(SqlBaseParser.READ, 0);
    }

    public TerminalNode ONLY() {
      return getToken(SqlBaseParser.ONLY, 0);
    }

    public TerminalNode WRITE() {
      return getToken(SqlBaseParser.WRITE, 0);
    }

    public TransactionAccessModeContext(TransactionModeContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterTransactionAccessMode(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitTransactionAccessMode(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitTransactionAccessMode(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class IsolationLevelContext extends TransactionModeContext {

    public TerminalNode ISOLATION() {
      return getToken(SqlBaseParser.ISOLATION, 0);
    }

    public TerminalNode LEVEL() {
      return getToken(SqlBaseParser.LEVEL, 0);
    }

    public LevelOfIsolationContext levelOfIsolation() {
      return getRuleContext(LevelOfIsolationContext.class, 0);
    }

    public IsolationLevelContext(TransactionModeContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterIsolationLevel(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitIsolationLevel(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitIsolationLevel(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final TransactionModeContext transactionMode() throws RecognitionException {
    TransactionModeContext _localctx = new TransactionModeContext(_ctx, getState());
    enterRule(_localctx, 94, RULE_transactionMode);
    int _la;
    try {
      setState(1029);
      switch (_input.LA(1)) {
        case ISOLATION:
          _localctx = new IsolationLevelContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1024);
          match(ISOLATION);
          setState(1025);
          match(LEVEL);
          setState(1026);
          levelOfIsolation();
        }
        break;
        case READ:
          _localctx = new TransactionAccessModeContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1027);
          match(READ);
          setState(1028);
          ((TransactionAccessModeContext) _localctx).accessMode = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == WRITE || _la == ONLY)) {
            ((TransactionAccessModeContext) _localctx).accessMode =
                (Token) _errHandler.recoverInline(this);
          } else {
            consume();
          }
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LevelOfIsolationContext extends ParserRuleContext {

    public LevelOfIsolationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_levelOfIsolation;
    }

    public LevelOfIsolationContext() {
    }

    public void copyFrom(LevelOfIsolationContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ReadUncommittedContext extends LevelOfIsolationContext {

    public TerminalNode READ() {
      return getToken(SqlBaseParser.READ, 0);
    }

    public TerminalNode UNCOMMITTED() {
      return getToken(SqlBaseParser.UNCOMMITTED, 0);
    }

    public ReadUncommittedContext(LevelOfIsolationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterReadUncommitted(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitReadUncommitted(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitReadUncommitted(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class SerializableContext extends LevelOfIsolationContext {

    public TerminalNode SERIALIZABLE() {
      return getToken(SqlBaseParser.SERIALIZABLE, 0);
    }

    public SerializableContext(LevelOfIsolationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterSerializable(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitSerializable(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitSerializable(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class ReadCommittedContext extends LevelOfIsolationContext {

    public TerminalNode READ() {
      return getToken(SqlBaseParser.READ, 0);
    }

    public TerminalNode COMMITTED() {
      return getToken(SqlBaseParser.COMMITTED, 0);
    }

    public ReadCommittedContext(LevelOfIsolationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterReadCommitted(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitReadCommitted(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitReadCommitted(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class RepeatableReadContext extends LevelOfIsolationContext {

    public TerminalNode REPEATABLE() {
      return getToken(SqlBaseParser.REPEATABLE, 0);
    }

    public TerminalNode READ() {
      return getToken(SqlBaseParser.READ, 0);
    }

    public RepeatableReadContext(LevelOfIsolationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterRepeatableRead(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitRepeatableRead(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitRepeatableRead(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final LevelOfIsolationContext levelOfIsolation() throws RecognitionException {
    LevelOfIsolationContext _localctx = new LevelOfIsolationContext(_ctx, getState());
    enterRule(_localctx, 96, RULE_levelOfIsolation);
    try {
      setState(1038);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 125, _ctx)) {
        case 1:
          _localctx = new ReadUncommittedContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1031);
          match(READ);
          setState(1032);
          match(UNCOMMITTED);
        }
        break;
        case 2:
          _localctx = new ReadCommittedContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1033);
          match(READ);
          setState(1034);
          match(COMMITTED);
        }
        break;
        case 3:
          _localctx = new RepeatableReadContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1035);
          match(REPEATABLE);
          setState(1036);
          match(READ);
        }
        break;
        case 4:
          _localctx = new SerializableContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1037);
          match(SERIALIZABLE);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CallArgumentContext extends ParserRuleContext {

    public CallArgumentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_callArgument;
    }

    public CallArgumentContext() {
    }

    public void copyFrom(CallArgumentContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class PositionalArgumentContext extends CallArgumentContext {

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public PositionalArgumentContext(CallArgumentContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterPositionalArgument(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitPositionalArgument(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitPositionalArgument(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class NamedArgumentContext extends CallArgumentContext {

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public NamedArgumentContext(CallArgumentContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNamedArgument(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNamedArgument(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNamedArgument(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final CallArgumentContext callArgument() throws RecognitionException {
    CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_callArgument);
    try {
      setState(1045);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 126, _ctx)) {
        case 1:
          _localctx = new PositionalArgumentContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1040);
          expression();
        }
        break;
        case 2:
          _localctx = new NamedArgumentContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1041);
          identifier();
          setState(1042);
          match(T__8);
          setState(1043);
          expression();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PrivilegeContext extends ParserRuleContext {

    public TerminalNode SELECT() {
      return getToken(SqlBaseParser.SELECT, 0);
    }

    public TerminalNode DELETE() {
      return getToken(SqlBaseParser.DELETE, 0);
    }

    public TerminalNode INSERT() {
      return getToken(SqlBaseParser.INSERT, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public PrivilegeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_privilege;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterPrivilege(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitPrivilege(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitPrivilege(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final PrivilegeContext privilege() throws RecognitionException {
    PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
    enterRule(_localctx, 100, RULE_privilege);
    try {
      setState(1051);
      switch (_input.LA(1)) {
        case SELECT:
          enterOuterAlt(_localctx, 1);
        {
          setState(1047);
          match(SELECT);
        }
        break;
        case DELETE:
          enterOuterAlt(_localctx, 2);
        {
          setState(1048);
          match(DELETE);
        }
        break;
        case INSERT:
          enterOuterAlt(_localctx, 3);
        {
          setState(1049);
          match(INSERT);
        }
        break;
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NO:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
        case IDENTIFIER:
        case DIGIT_IDENTIFIER:
        case QUOTED_IDENTIFIER:
        case BACKQUOTED_IDENTIFIER:
          enterOuterAlt(_localctx, 4);
        {
          setState(1050);
          identifier();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QualifiedNameContext extends ParserRuleContext {

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_qualifiedName;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQualifiedName(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQualifiedName(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQualifiedName(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 102, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1053);
        identifier();
        setState(1058);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 128, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1054);
                match(T__4);
                setState(1055);
                identifier();
              }
            }
          }
          setState(1060);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 128, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierContext extends ParserRuleContext {

    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifier;
    }

    public IdentifierContext() {
    }

    public void copyFrom(IdentifierContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class BackQuotedIdentifierContext extends IdentifierContext {

    public TerminalNode BACKQUOTED_IDENTIFIER() {
      return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0);
    }

    public BackQuotedIdentifierContext(IdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterBackQuotedIdentifier(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitBackQuotedIdentifier(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitBackQuotedIdentifier(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class QuotedIdentifierAlternativeContext extends IdentifierContext {

    public QuotedIdentifierContext quotedIdentifier() {
      return getRuleContext(QuotedIdentifierContext.class, 0);
    }

    public QuotedIdentifierAlternativeContext(IdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQuotedIdentifierAlternative(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQuotedIdentifierAlternative(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQuotedIdentifierAlternative(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class DigitIdentifierContext extends IdentifierContext {

    public TerminalNode DIGIT_IDENTIFIER() {
      return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0);
    }

    public DigitIdentifierContext(IdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterDigitIdentifier(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitDigitIdentifier(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitDigitIdentifier(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class UnquotedIdentifierContext extends IdentifierContext {

    public TerminalNode IDENTIFIER() {
      return getToken(SqlBaseParser.IDENTIFIER, 0);
    }

    public NonReservedContext nonReserved() {
      return getRuleContext(NonReservedContext.class, 0);
    }

    public UnquotedIdentifierContext(IdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterUnquotedIdentifier(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitUnquotedIdentifier(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitUnquotedIdentifier(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 104, RULE_identifier);
    try {
      setState(1066);
      switch (_input.LA(1)) {
        case IDENTIFIER:
          _localctx = new UnquotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1061);
          match(IDENTIFIER);
        }
        break;
        case QUOTED_IDENTIFIER:
          _localctx = new QuotedIdentifierAlternativeContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1062);
          quotedIdentifier();
        }
        break;
        case ADD:
        case APPROXIMATE:
        case AT:
        case CONFIDENCE:
        case NO:
        case SUBSTRING:
        case POSITION:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INTERVAL:
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
        case SECOND:
        case ZONE:
        case OVER:
        case PARTITION:
        case RANGE:
        case ROWS:
        case PRECEDING:
        case FOLLOWING:
        case CURRENT:
        case ROW:
        case VIEW:
        case REPLACE:
        case GRANT:
        case REVOKE:
        case PRIVILEGES:
        case PUBLIC:
        case OPTION:
        case EXPLAIN:
        case ANALYZE:
        case FORMAT:
        case TYPE:
        case TEXT:
        case GRAPHVIZ:
        case LOGICAL:
        case DISTRIBUTED:
        case TRY:
        case SHOW:
        case TABLES:
        case SCHEMAS:
        case CATALOGS:
        case COLUMNS:
        case COLUMN:
        case USE:
        case PARTITIONS:
        case FUNCTIONS:
        case TO:
        case SYSTEM:
        case BERNOULLI:
        case POISSONIZED:
        case TABLESAMPLE:
        case RESCALED:
        case ARRAY:
        case MAP:
        case SET:
        case RESET:
        case SESSION:
        case DATA:
        case START:
        case TRANSACTION:
        case COMMIT:
        case ROLLBACK:
        case WORK:
        case ISOLATION:
        case LEVEL:
        case SERIALIZABLE:
        case REPEATABLE:
        case COMMITTED:
        case UNCOMMITTED:
        case READ:
        case WRITE:
        case ONLY:
        case CALL:
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
        case IF:
        case NULLIF:
        case COALESCE:
          _localctx = new UnquotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1063);
          nonReserved();
        }
        break;
        case BACKQUOTED_IDENTIFIER:
          _localctx = new BackQuotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1064);
          match(BACKQUOTED_IDENTIFIER);
        }
        break;
        case DIGIT_IDENTIFIER:
          _localctx = new DigitIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(1065);
          match(DIGIT_IDENTIFIER);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QuotedIdentifierContext extends ParserRuleContext {

    public TerminalNode QUOTED_IDENTIFIER() {
      return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0);
    }

    public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_quotedIdentifier;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterQuotedIdentifier(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitQuotedIdentifier(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitQuotedIdentifier(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
    QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
    enterRule(_localctx, 106, RULE_quotedIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1068);
        match(QUOTED_IDENTIFIER);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NumberContext extends ParserRuleContext {

    public NumberContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_number;
    }

    public NumberContext() {
    }

    public void copyFrom(NumberContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class DecimalLiteralContext extends NumberContext {

    public TerminalNode DECIMAL_VALUE() {
      return getToken(SqlBaseParser.DECIMAL_VALUE, 0);
    }

    public DecimalLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterDecimalLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitDecimalLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitDecimalLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public static class IntegerLiteralContext extends NumberContext {

    public TerminalNode INTEGER_VALUE() {
      return getToken(SqlBaseParser.INTEGER_VALUE, 0);
    }

    public IntegerLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterIntegerLiteral(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitIntegerLiteral(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitIntegerLiteral(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 108, RULE_number);
    try {
      setState(1072);
      switch (_input.LA(1)) {
        case DECIMAL_VALUE:
          _localctx = new DecimalLiteralContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1070);
          match(DECIMAL_VALUE);
        }
        break;
        case INTEGER_VALUE:
          _localctx = new IntegerLiteralContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1071);
          match(INTEGER_VALUE);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NonReservedContext extends ParserRuleContext {

    public TerminalNode SHOW() {
      return getToken(SqlBaseParser.SHOW, 0);
    }

    public TerminalNode TABLES() {
      return getToken(SqlBaseParser.TABLES, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(SqlBaseParser.COLUMNS, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(SqlBaseParser.COLUMN, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(SqlBaseParser.PARTITIONS, 0);
    }

    public TerminalNode FUNCTIONS() {
      return getToken(SqlBaseParser.FUNCTIONS, 0);
    }

    public TerminalNode SCHEMAS() {
      return getToken(SqlBaseParser.SCHEMAS, 0);
    }

    public TerminalNode CATALOGS() {
      return getToken(SqlBaseParser.CATALOGS, 0);
    }

    public TerminalNode SESSION() {
      return getToken(SqlBaseParser.SESSION, 0);
    }

    public TerminalNode ADD() {
      return getToken(SqlBaseParser.ADD, 0);
    }

    public TerminalNode OVER() {
      return getToken(SqlBaseParser.OVER, 0);
    }

    public TerminalNode PARTITION() {
      return getToken(SqlBaseParser.PARTITION, 0);
    }

    public TerminalNode RANGE() {
      return getToken(SqlBaseParser.RANGE, 0);
    }

    public TerminalNode ROWS() {
      return getToken(SqlBaseParser.ROWS, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(SqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(SqlBaseParser.FOLLOWING, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(SqlBaseParser.CURRENT, 0);
    }

    public TerminalNode ROW() {
      return getToken(SqlBaseParser.ROW, 0);
    }

    public TerminalNode MAP() {
      return getToken(SqlBaseParser.MAP, 0);
    }

    public TerminalNode ARRAY() {
      return getToken(SqlBaseParser.ARRAY, 0);
    }

    public TerminalNode TINYINT() {
      return getToken(SqlBaseParser.TINYINT, 0);
    }

    public TerminalNode SMALLINT() {
      return getToken(SqlBaseParser.SMALLINT, 0);
    }

    public TerminalNode INTEGER() {
      return getToken(SqlBaseParser.INTEGER, 0);
    }

    public TerminalNode DATE() {
      return getToken(SqlBaseParser.DATE, 0);
    }

    public TerminalNode TIME() {
      return getToken(SqlBaseParser.TIME, 0);
    }

    public TerminalNode TIMESTAMP() {
      return getToken(SqlBaseParser.TIMESTAMP, 0);
    }

    public TerminalNode INTERVAL() {
      return getToken(SqlBaseParser.INTERVAL, 0);
    }

    public TerminalNode ZONE() {
      return getToken(SqlBaseParser.ZONE, 0);
    }

    public TerminalNode YEAR() {
      return getToken(SqlBaseParser.YEAR, 0);
    }

    public TerminalNode MONTH() {
      return getToken(SqlBaseParser.MONTH, 0);
    }

    public TerminalNode DAY() {
      return getToken(SqlBaseParser.DAY, 0);
    }

    public TerminalNode HOUR() {
      return getToken(SqlBaseParser.HOUR, 0);
    }

    public TerminalNode MINUTE() {
      return getToken(SqlBaseParser.MINUTE, 0);
    }

    public TerminalNode SECOND() {
      return getToken(SqlBaseParser.SECOND, 0);
    }

    public TerminalNode EXPLAIN() {
      return getToken(SqlBaseParser.EXPLAIN, 0);
    }

    public TerminalNode ANALYZE() {
      return getToken(SqlBaseParser.ANALYZE, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(SqlBaseParser.FORMAT, 0);
    }

    public TerminalNode TYPE() {
      return getToken(SqlBaseParser.TYPE, 0);
    }

    public TerminalNode TEXT() {
      return getToken(SqlBaseParser.TEXT, 0);
    }

    public TerminalNode GRAPHVIZ() {
      return getToken(SqlBaseParser.GRAPHVIZ, 0);
    }

    public TerminalNode LOGICAL() {
      return getToken(SqlBaseParser.LOGICAL, 0);
    }

    public TerminalNode DISTRIBUTED() {
      return getToken(SqlBaseParser.DISTRIBUTED, 0);
    }

    public TerminalNode TABLESAMPLE() {
      return getToken(SqlBaseParser.TABLESAMPLE, 0);
    }

    public TerminalNode SYSTEM() {
      return getToken(SqlBaseParser.SYSTEM, 0);
    }

    public TerminalNode BERNOULLI() {
      return getToken(SqlBaseParser.BERNOULLI, 0);
    }

    public TerminalNode POISSONIZED() {
      return getToken(SqlBaseParser.POISSONIZED, 0);
    }

    public TerminalNode USE() {
      return getToken(SqlBaseParser.USE, 0);
    }

    public TerminalNode TO() {
      return getToken(SqlBaseParser.TO, 0);
    }

    public TerminalNode RESCALED() {
      return getToken(SqlBaseParser.RESCALED, 0);
    }

    public TerminalNode APPROXIMATE() {
      return getToken(SqlBaseParser.APPROXIMATE, 0);
    }

    public TerminalNode AT() {
      return getToken(SqlBaseParser.AT, 0);
    }

    public TerminalNode CONFIDENCE() {
      return getToken(SqlBaseParser.CONFIDENCE, 0);
    }

    public TerminalNode SET() {
      return getToken(SqlBaseParser.SET, 0);
    }

    public TerminalNode RESET() {
      return getToken(SqlBaseParser.RESET, 0);
    }

    public TerminalNode VIEW() {
      return getToken(SqlBaseParser.VIEW, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(SqlBaseParser.REPLACE, 0);
    }

    public TerminalNode IF() {
      return getToken(SqlBaseParser.IF, 0);
    }

    public TerminalNode NULLIF() {
      return getToken(SqlBaseParser.NULLIF, 0);
    }

    public TerminalNode COALESCE() {
      return getToken(SqlBaseParser.COALESCE, 0);
    }

    public TerminalNode TRY() {
      return getToken(SqlBaseParser.TRY, 0);
    }

    public NormalFormContext normalForm() {
      return getRuleContext(NormalFormContext.class, 0);
    }

    public TerminalNode POSITION() {
      return getToken(SqlBaseParser.POSITION, 0);
    }

    public TerminalNode NO() {
      return getToken(SqlBaseParser.NO, 0);
    }

    public TerminalNode DATA() {
      return getToken(SqlBaseParser.DATA, 0);
    }

    public TerminalNode START() {
      return getToken(SqlBaseParser.START, 0);
    }

    public TerminalNode TRANSACTION() {
      return getToken(SqlBaseParser.TRANSACTION, 0);
    }

    public TerminalNode COMMIT() {
      return getToken(SqlBaseParser.COMMIT, 0);
    }

    public TerminalNode ROLLBACK() {
      return getToken(SqlBaseParser.ROLLBACK, 0);
    }

    public TerminalNode WORK() {
      return getToken(SqlBaseParser.WORK, 0);
    }

    public TerminalNode ISOLATION() {
      return getToken(SqlBaseParser.ISOLATION, 0);
    }

    public TerminalNode LEVEL() {
      return getToken(SqlBaseParser.LEVEL, 0);
    }

    public TerminalNode SERIALIZABLE() {
      return getToken(SqlBaseParser.SERIALIZABLE, 0);
    }

    public TerminalNode REPEATABLE() {
      return getToken(SqlBaseParser.REPEATABLE, 0);
    }

    public TerminalNode COMMITTED() {
      return getToken(SqlBaseParser.COMMITTED, 0);
    }

    public TerminalNode UNCOMMITTED() {
      return getToken(SqlBaseParser.UNCOMMITTED, 0);
    }

    public TerminalNode READ() {
      return getToken(SqlBaseParser.READ, 0);
    }

    public TerminalNode WRITE() {
      return getToken(SqlBaseParser.WRITE, 0);
    }

    public TerminalNode ONLY() {
      return getToken(SqlBaseParser.ONLY, 0);
    }

    public TerminalNode CALL() {
      return getToken(SqlBaseParser.CALL, 0);
    }

    public TerminalNode GRANT() {
      return getToken(SqlBaseParser.GRANT, 0);
    }

    public TerminalNode REVOKE() {
      return getToken(SqlBaseParser.REVOKE, 0);
    }

    public TerminalNode PRIVILEGES() {
      return getToken(SqlBaseParser.PRIVILEGES, 0);
    }

    public TerminalNode PUBLIC() {
      return getToken(SqlBaseParser.PUBLIC, 0);
    }

    public TerminalNode OPTION() {
      return getToken(SqlBaseParser.OPTION, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(SqlBaseParser.SUBSTRING, 0);
    }

    public NonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_nonReserved;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNonReserved(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNonReserved(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNonReserved(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final NonReservedContext nonReserved() throws RecognitionException {
    NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
    enterRule(_localctx, 110, RULE_nonReserved);
    try {
      setState(1159);
      switch (_input.LA(1)) {
        case SHOW:
          enterOuterAlt(_localctx, 1);
        {
          setState(1074);
          match(SHOW);
        }
        break;
        case TABLES:
          enterOuterAlt(_localctx, 2);
        {
          setState(1075);
          match(TABLES);
        }
        break;
        case COLUMNS:
          enterOuterAlt(_localctx, 3);
        {
          setState(1076);
          match(COLUMNS);
        }
        break;
        case COLUMN:
          enterOuterAlt(_localctx, 4);
        {
          setState(1077);
          match(COLUMN);
        }
        break;
        case PARTITIONS:
          enterOuterAlt(_localctx, 5);
        {
          setState(1078);
          match(PARTITIONS);
        }
        break;
        case FUNCTIONS:
          enterOuterAlt(_localctx, 6);
        {
          setState(1079);
          match(FUNCTIONS);
        }
        break;
        case SCHEMAS:
          enterOuterAlt(_localctx, 7);
        {
          setState(1080);
          match(SCHEMAS);
        }
        break;
        case CATALOGS:
          enterOuterAlt(_localctx, 8);
        {
          setState(1081);
          match(CATALOGS);
        }
        break;
        case SESSION:
          enterOuterAlt(_localctx, 9);
        {
          setState(1082);
          match(SESSION);
        }
        break;
        case ADD:
          enterOuterAlt(_localctx, 10);
        {
          setState(1083);
          match(ADD);
        }
        break;
        case OVER:
          enterOuterAlt(_localctx, 11);
        {
          setState(1084);
          match(OVER);
        }
        break;
        case PARTITION:
          enterOuterAlt(_localctx, 12);
        {
          setState(1085);
          match(PARTITION);
        }
        break;
        case RANGE:
          enterOuterAlt(_localctx, 13);
        {
          setState(1086);
          match(RANGE);
        }
        break;
        case ROWS:
          enterOuterAlt(_localctx, 14);
        {
          setState(1087);
          match(ROWS);
        }
        break;
        case PRECEDING:
          enterOuterAlt(_localctx, 15);
        {
          setState(1088);
          match(PRECEDING);
        }
        break;
        case FOLLOWING:
          enterOuterAlt(_localctx, 16);
        {
          setState(1089);
          match(FOLLOWING);
        }
        break;
        case CURRENT:
          enterOuterAlt(_localctx, 17);
        {
          setState(1090);
          match(CURRENT);
        }
        break;
        case ROW:
          enterOuterAlt(_localctx, 18);
        {
          setState(1091);
          match(ROW);
        }
        break;
        case MAP:
          enterOuterAlt(_localctx, 19);
        {
          setState(1092);
          match(MAP);
        }
        break;
        case ARRAY:
          enterOuterAlt(_localctx, 20);
        {
          setState(1093);
          match(ARRAY);
        }
        break;
        case TINYINT:
          enterOuterAlt(_localctx, 21);
        {
          setState(1094);
          match(TINYINT);
        }
        break;
        case SMALLINT:
          enterOuterAlt(_localctx, 22);
        {
          setState(1095);
          match(SMALLINT);
        }
        break;
        case INTEGER:
          enterOuterAlt(_localctx, 23);
        {
          setState(1096);
          match(INTEGER);
        }
        break;
        case DATE:
          enterOuterAlt(_localctx, 24);
        {
          setState(1097);
          match(DATE);
        }
        break;
        case TIME:
          enterOuterAlt(_localctx, 25);
        {
          setState(1098);
          match(TIME);
        }
        break;
        case TIMESTAMP:
          enterOuterAlt(_localctx, 26);
        {
          setState(1099);
          match(TIMESTAMP);
        }
        break;
        case INTERVAL:
          enterOuterAlt(_localctx, 27);
        {
          setState(1100);
          match(INTERVAL);
        }
        break;
        case ZONE:
          enterOuterAlt(_localctx, 28);
        {
          setState(1101);
          match(ZONE);
        }
        break;
        case YEAR:
          enterOuterAlt(_localctx, 29);
        {
          setState(1102);
          match(YEAR);
        }
        break;
        case MONTH:
          enterOuterAlt(_localctx, 30);
        {
          setState(1103);
          match(MONTH);
        }
        break;
        case DAY:
          enterOuterAlt(_localctx, 31);
        {
          setState(1104);
          match(DAY);
        }
        break;
        case HOUR:
          enterOuterAlt(_localctx, 32);
        {
          setState(1105);
          match(HOUR);
        }
        break;
        case MINUTE:
          enterOuterAlt(_localctx, 33);
        {
          setState(1106);
          match(MINUTE);
        }
        break;
        case SECOND:
          enterOuterAlt(_localctx, 34);
        {
          setState(1107);
          match(SECOND);
        }
        break;
        case EXPLAIN:
          enterOuterAlt(_localctx, 35);
        {
          setState(1108);
          match(EXPLAIN);
        }
        break;
        case ANALYZE:
          enterOuterAlt(_localctx, 36);
        {
          setState(1109);
          match(ANALYZE);
        }
        break;
        case FORMAT:
          enterOuterAlt(_localctx, 37);
        {
          setState(1110);
          match(FORMAT);
        }
        break;
        case TYPE:
          enterOuterAlt(_localctx, 38);
        {
          setState(1111);
          match(TYPE);
        }
        break;
        case TEXT:
          enterOuterAlt(_localctx, 39);
        {
          setState(1112);
          match(TEXT);
        }
        break;
        case GRAPHVIZ:
          enterOuterAlt(_localctx, 40);
        {
          setState(1113);
          match(GRAPHVIZ);
        }
        break;
        case LOGICAL:
          enterOuterAlt(_localctx, 41);
        {
          setState(1114);
          match(LOGICAL);
        }
        break;
        case DISTRIBUTED:
          enterOuterAlt(_localctx, 42);
        {
          setState(1115);
          match(DISTRIBUTED);
        }
        break;
        case TABLESAMPLE:
          enterOuterAlt(_localctx, 43);
        {
          setState(1116);
          match(TABLESAMPLE);
        }
        break;
        case SYSTEM:
          enterOuterAlt(_localctx, 44);
        {
          setState(1117);
          match(SYSTEM);
        }
        break;
        case BERNOULLI:
          enterOuterAlt(_localctx, 45);
        {
          setState(1118);
          match(BERNOULLI);
        }
        break;
        case POISSONIZED:
          enterOuterAlt(_localctx, 46);
        {
          setState(1119);
          match(POISSONIZED);
        }
        break;
        case USE:
          enterOuterAlt(_localctx, 47);
        {
          setState(1120);
          match(USE);
        }
        break;
        case TO:
          enterOuterAlt(_localctx, 48);
        {
          setState(1121);
          match(TO);
        }
        break;
        case RESCALED:
          enterOuterAlt(_localctx, 49);
        {
          setState(1122);
          match(RESCALED);
        }
        break;
        case APPROXIMATE:
          enterOuterAlt(_localctx, 50);
        {
          setState(1123);
          match(APPROXIMATE);
        }
        break;
        case AT:
          enterOuterAlt(_localctx, 51);
        {
          setState(1124);
          match(AT);
        }
        break;
        case CONFIDENCE:
          enterOuterAlt(_localctx, 52);
        {
          setState(1125);
          match(CONFIDENCE);
        }
        break;
        case SET:
          enterOuterAlt(_localctx, 53);
        {
          setState(1126);
          match(SET);
        }
        break;
        case RESET:
          enterOuterAlt(_localctx, 54);
        {
          setState(1127);
          match(RESET);
        }
        break;
        case VIEW:
          enterOuterAlt(_localctx, 55);
        {
          setState(1128);
          match(VIEW);
        }
        break;
        case REPLACE:
          enterOuterAlt(_localctx, 56);
        {
          setState(1129);
          match(REPLACE);
        }
        break;
        case IF:
          enterOuterAlt(_localctx, 57);
        {
          setState(1130);
          match(IF);
        }
        break;
        case NULLIF:
          enterOuterAlt(_localctx, 58);
        {
          setState(1131);
          match(NULLIF);
        }
        break;
        case COALESCE:
          enterOuterAlt(_localctx, 59);
        {
          setState(1132);
          match(COALESCE);
        }
        break;
        case TRY:
          enterOuterAlt(_localctx, 60);
        {
          setState(1133);
          match(TRY);
        }
        break;
        case NFD:
        case NFC:
        case NFKD:
        case NFKC:
          enterOuterAlt(_localctx, 61);
        {
          setState(1134);
          normalForm();
        }
        break;
        case POSITION:
          enterOuterAlt(_localctx, 62);
        {
          setState(1135);
          match(POSITION);
        }
        break;
        case NO:
          enterOuterAlt(_localctx, 63);
        {
          setState(1136);
          match(NO);
        }
        break;
        case DATA:
          enterOuterAlt(_localctx, 64);
        {
          setState(1137);
          match(DATA);
        }
        break;
        case START:
          enterOuterAlt(_localctx, 65);
        {
          setState(1138);
          match(START);
        }
        break;
        case TRANSACTION:
          enterOuterAlt(_localctx, 66);
        {
          setState(1139);
          match(TRANSACTION);
        }
        break;
        case COMMIT:
          enterOuterAlt(_localctx, 67);
        {
          setState(1140);
          match(COMMIT);
        }
        break;
        case ROLLBACK:
          enterOuterAlt(_localctx, 68);
        {
          setState(1141);
          match(ROLLBACK);
        }
        break;
        case WORK:
          enterOuterAlt(_localctx, 69);
        {
          setState(1142);
          match(WORK);
        }
        break;
        case ISOLATION:
          enterOuterAlt(_localctx, 70);
        {
          setState(1143);
          match(ISOLATION);
        }
        break;
        case LEVEL:
          enterOuterAlt(_localctx, 71);
        {
          setState(1144);
          match(LEVEL);
        }
        break;
        case SERIALIZABLE:
          enterOuterAlt(_localctx, 72);
        {
          setState(1145);
          match(SERIALIZABLE);
        }
        break;
        case REPEATABLE:
          enterOuterAlt(_localctx, 73);
        {
          setState(1146);
          match(REPEATABLE);
        }
        break;
        case COMMITTED:
          enterOuterAlt(_localctx, 74);
        {
          setState(1147);
          match(COMMITTED);
        }
        break;
        case UNCOMMITTED:
          enterOuterAlt(_localctx, 75);
        {
          setState(1148);
          match(UNCOMMITTED);
        }
        break;
        case READ:
          enterOuterAlt(_localctx, 76);
        {
          setState(1149);
          match(READ);
        }
        break;
        case WRITE:
          enterOuterAlt(_localctx, 77);
        {
          setState(1150);
          match(WRITE);
        }
        break;
        case ONLY:
          enterOuterAlt(_localctx, 78);
        {
          setState(1151);
          match(ONLY);
        }
        break;
        case CALL:
          enterOuterAlt(_localctx, 79);
        {
          setState(1152);
          match(CALL);
        }
        break;
        case GRANT:
          enterOuterAlt(_localctx, 80);
        {
          setState(1153);
          match(GRANT);
        }
        break;
        case REVOKE:
          enterOuterAlt(_localctx, 81);
        {
          setState(1154);
          match(REVOKE);
        }
        break;
        case PRIVILEGES:
          enterOuterAlt(_localctx, 82);
        {
          setState(1155);
          match(PRIVILEGES);
        }
        break;
        case PUBLIC:
          enterOuterAlt(_localctx, 83);
        {
          setState(1156);
          match(PUBLIC);
        }
        break;
        case OPTION:
          enterOuterAlt(_localctx, 84);
        {
          setState(1157);
          match(OPTION);
        }
        break;
        case SUBSTRING:
          enterOuterAlt(_localctx, 85);
        {
          setState(1158);
          match(SUBSTRING);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NormalFormContext extends ParserRuleContext {

    public TerminalNode NFD() {
      return getToken(SqlBaseParser.NFD, 0);
    }

    public TerminalNode NFC() {
      return getToken(SqlBaseParser.NFC, 0);
    }

    public TerminalNode NFKD() {
      return getToken(SqlBaseParser.NFKD, 0);
    }

    public TerminalNode NFKC() {
      return getToken(SqlBaseParser.NFKC, 0);
    }

    public NormalFormContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_normalForm;
    }

    @Override
    public void enterRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).enterNormalForm(this);
      }
    }

    @Override
    public void exitRule(ParseTreeListener listener) {
      if (listener instanceof SqlBaseListener) {
        ((SqlBaseListener) listener).exitNormalForm(this);
      }
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof SqlBaseVisitor) {
        return ((SqlBaseVisitor<? extends T>) visitor).visitNormalForm(this);
      } else {
        return visitor.visitChildren(this);
      }
    }
  }

  public final NormalFormContext normalForm() throws RecognitionException {
    NormalFormContext _localctx = new NormalFormContext(_ctx, getState());
    enterRule(_localctx, 112, RULE_normalForm);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1161);
        _la = _input.LA(1);
        if (!(((((_la - 181)) & ~0x3f) == 0 &&
               ((1L << (_la - 181)) & ((1L << (NFD - 181)) | (1L << (NFC - 181)) | (1L << (NFKD
                                                                                           - 181))
                                       | (1L << (NFKC - 181)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
      case 10:
        return queryTerm_sempred((QueryTermContext) _localctx, predIndex);
      case 21:
        return relation_sempred((RelationContext) _localctx, predIndex);
      case 29:
        return booleanExpression_sempred((BooleanExpressionContext) _localctx, predIndex);
      case 32:
        return valueExpression_sempred((ValueExpressionContext) _localctx, predIndex);
      case 33:
        return primaryExpression_sempred((PrimaryExpressionContext) _localctx, predIndex);
      case 39:
        return type_sempred((TypeContext) _localctx, predIndex);
    }
    return true;
  }

  private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
    switch (predIndex) {
      case 0:
        return precpred(_ctx, 2);
      case 1:
        return precpred(_ctx, 1);
    }
    return true;
  }

  private boolean relation_sempred(RelationContext _localctx, int predIndex) {
    switch (predIndex) {
      case 2:
        return precpred(_ctx, 2);
    }
    return true;
  }

  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 3:
        return precpred(_ctx, 2);
      case 4:
        return precpred(_ctx, 1);
    }
    return true;
  }

  private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 5:
        return precpred(_ctx, 3);
      case 6:
        return precpred(_ctx, 2);
      case 7:
        return precpred(_ctx, 1);
      case 8:
        return precpred(_ctx, 5);
    }
    return true;
  }

  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 9:
        return precpred(_ctx, 12);
      case 10:
        return precpred(_ctx, 10);
    }
    return true;
  }

  private boolean type_sempred(TypeContext _localctx, int predIndex) {
    switch (predIndex) {
      case 11:
        return precpred(_ctx, 5);
    }
    return true;
  }

  public static final String _serializedATN =
      "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u00d8\u048e\4\2\t" +
      "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13" +
      "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22" +
      "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31" +
      "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!" +
      "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4" +
      ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t" +
      "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\3\2\3\2\7\2w\n\2\f" +
      "\2\16\2z\13\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\5\5" +
      "\u0089\n\5\3\5\3\5\5\5\u008d\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u0097" +
      "\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5" +
      "\5\u00a9\n\5\3\5\3\5\3\5\3\5\3\5\7\5\u00b0\n\5\f\5\16\5\u00b3\13\5\3\5" +
      "\3\5\3\5\5\5\u00b8\n\5\3\5\3\5\3\5\3\5\5\5\u00be\n\5\3\5\5\5\u00c1\n\5" +
      "\3\6\5\6\u00c4\n\6\3\6\3\6\3\7\3\7\5\7\u00ca\n\7\3\7\3\7\3\7\7\7\u00cf" +
      "\n\7\f\7\16\7\u00d2\13\7\3\b\3\b\3\b\3\t\3\t\3\t\3\t\7\t\u00db\n\t\f\t" +
      "\16\t\u00de\13\t\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13" +
      "\7\13\u00ec\n\13\f\13\16\13\u00ef\13\13\5\13\u00f1\n\13\3\13\3\13\5\13" +
      "\u00f5\n\13\3\13\3\13\3\13\3\13\3\13\5\13\u00fc\n\13\3\f\3\f\3\f\3\f\3" +
      "\f\3\f\5\f\u0104\n\f\3\f\3\f\3\f\3\f\5\f\u010a\n\f\3\f\7\f\u010d\n\f\f" +
      "\f\16\f\u0110\13\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\7\r\u0119\n\r\f\r\16\r" +
      "\u011c\13\r\3\r\3\r\3\r\3\r\5\r\u0122\n\r\3\16\3\16\5\16\u0126\n\16\3" +
      "\16\3\16\5\16\u012a\n\16\3\17\3\17\5\17\u012e\n\17\3\17\3\17\3\17\7\17" +
      "\u0133\n\17\f\17\16\17\u0136\13\17\3\17\3\17\5\17\u013a\n\17\3\17\3\17" +
      "\3\17\3\17\7\17\u0140\n\17\f\17\16\17\u0143\13\17\5\17\u0145\n\17\3\17" +
      "\3\17\5\17\u0149\n\17\3\17\3\17\3\17\5\17\u014e\n\17\3\17\3\17\5\17\u0152" +
      "\n\17\3\20\5\20\u0155\n\20\3\20\3\20\3\20\7\20\u015a\n\20\f\20\16\20\u015d" +
      "\13\20\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u0165\n\21\f\21\16\21\u0168" +
      "\13\21\5\21\u016a\n\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u0172\n\21\f" +
      "\21\16\21\u0175\13\21\5\21\u0177\n\21\3\21\3\21\3\21\3\21\3\21\3\21\3" +
      "\21\7\21\u0180\n\21\f\21\16\21\u0183\13\21\3\21\3\21\5\21\u0187\n\21\3" +
      "\22\3\22\3\22\3\22\7\22\u018d\n\22\f\22\16\22\u0190\13\22\5\22\u0192\n" +
      "\22\3\22\3\22\5\22\u0196\n\22\3\23\3\23\3\23\3\23\7\23\u019c\n\23\f\23" +
      "\16\23\u019f\13\23\5\23\u01a1\n\23\3\23\3\23\5\23\u01a5\n\23\3\24\3\24" +
      "\5\24\u01a9\n\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\26\3\26\5\26\u01b4" +
      "\n\26\3\26\5\26\u01b7\n\26\3\26\3\26\3\26\3\26\3\26\5\26\u01be\n\26\3" +
      "\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3" +
      "\27\3\27\3\27\5\27\u01d1\n\27\7\27\u01d3\n\27\f\27\16\27\u01d6\13\27\3" +
      "\30\5\30\u01d9\n\30\3\30\3\30\5\30\u01dd\n\30\3\30\3\30\5\30\u01e1\n\30" +
      "\3\30\3\30\5\30\u01e5\n\30\5\30\u01e7\n\30\3\31\3\31\3\31\3\31\3\31\3" +
      "\31\3\31\7\31\u01f0\n\31\f\31\16\31\u01f3\13\31\3\31\3\31\5\31\u01f7\n" +
      "\31\3\32\3\32\3\33\3\33\5\33\u01fd\n\33\3\33\3\33\5\33\u0201\n\33\5\33" +
      "\u0203\n\33\3\34\3\34\3\34\3\34\7\34\u0209\n\34\f\34\16\34\u020c\13\34" +
      "\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\35\7\35\u021a" +
      "\n\35\f\35\16\35\u021d\13\35\3\35\3\35\3\35\5\35\u0222\n\35\3\35\3\35" +
      "\3\35\3\35\5\35\u0228\n\35\3\36\3\36\3\37\3\37\3\37\3\37\5\37\u0230\n" +
      "\37\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u0238\n\37\f\37\16\37\u023b\13" +
      "\37\3 \3 \5 \u023f\n \3!\3!\3!\3!\5!\u0245\n!\3!\3!\3!\3!\3!\3!\5!\u024d" +
      "\n!\3!\3!\3!\3!\3!\7!\u0254\n!\f!\16!\u0257\13!\3!\3!\3!\5!\u025c\n!\3" +
      "!\3!\3!\3!\3!\3!\5!\u0264\n!\3!\3!\3!\3!\5!\u026a\n!\3!\3!\5!\u026e\n" +
      "!\3!\3!\3!\5!\u0273\n!\3!\3!\3!\5!\u0278\n!\3\"\3\"\3\"\3\"\5\"\u027e" +
      "\n\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\7\"\u028c\n\"\f\"" +
      "\16\"\u028f\13\"\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3" +
      "#\3#\3#\3#\6#\u02a6\n#\r#\16#\u02a7\3#\3#\3#\3#\3#\3#\3#\7#\u02b1\n#\f" +
      "#\16#\u02b4\13#\3#\3#\3#\3#\3#\3#\3#\5#\u02bd\n#\3#\3#\3#\5#\u02c2\n#" +
      "\3#\3#\3#\7#\u02c7\n#\f#\16#\u02ca\13#\5#\u02cc\n#\3#\3#\5#\u02d0\n#\3" +
      "#\3#\3#\3#\3#\3#\3#\3#\7#\u02da\n#\f#\16#\u02dd\13#\3#\3#\3#\3#\3#\3#" +
      "\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\6#\u02ef\n#\r#\16#\u02f0\3#\3#\5#\u02f5" +
      "\n#\3#\3#\3#\3#\6#\u02fb\n#\r#\16#\u02fc\3#\3#\5#\u0301\n#\3#\3#\3#\3" +
      "#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\7#\u0318\n#\f#\16" +
      "#\u031b\13#\5#\u031d\n#\3#\3#\3#\3#\3#\3#\3#\5#\u0326\n#\3#\3#\3#\3#\5" +
      "#\u032c\n#\3#\3#\3#\3#\5#\u0332\n#\3#\3#\3#\3#\5#\u0338\n#\3#\3#\3#\3" +
      "#\3#\3#\3#\5#\u0341\n#\3#\3#\3#\3#\3#\3#\3#\5#\u034a\n#\3#\3#\3#\3#\3" +
      "#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u0359\n#\3#\3#\3#\3#\3#\3#\3#\3#\7#\u0363" +
      "\n#\f#\16#\u0366\13#\3$\3$\3$\3$\3$\3$\5$\u036e\n$\3%\3%\3&\3&\3\'\3\'" +
      "\5\'\u0376\n\'\3\'\3\'\3\'\3\'\5\'\u037c\n\'\3(\3(\3)\3)\3)\3)\3)\3)\3" +
      ")\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\7)\u0395\n)\f)\16)\u0398\13" +
      ")\3)\3)\3)\3)\3)\3)\3)\7)\u03a1\n)\f)\16)\u03a4\13)\3)\3)\5)\u03a8\n)" +
      "\5)\u03aa\n)\3)\3)\7)\u03ae\n)\f)\16)\u03b1\13)\3*\3*\5*\u03b5\n*\3+\3" +
      "+\3+\5+\u03ba\n+\3,\3,\3,\3,\3,\3-\3-\3-\3-\3-\3-\3-\7-\u03c8\n-\f-\16" +
      "-\u03cb\13-\5-\u03cd\n-\3-\3-\3-\3-\3-\7-\u03d4\n-\f-\16-\u03d7\13-\5" +
      "-\u03d9\n-\3-\5-\u03dc\n-\3-\3-\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3.\3" +
      ".\3.\3.\3.\5.\u03f0\n.\3/\3/\3/\3/\3/\3/\3/\3/\3/\5/\u03fb\n/\3\60\3\60" +
      "\3\60\3\60\5\60\u0401\n\60\3\61\3\61\3\61\3\61\3\61\5\61\u0408\n\61\3" +
      "\62\3\62\3\62\3\62\3\62\3\62\3\62\5\62\u0411\n\62\3\63\3\63\3\63\3\63" +
      "\3\63\5\63\u0418\n\63\3\64\3\64\3\64\3\64\5\64\u041e\n\64\3\65\3\65\3" +
      "\65\7\65\u0423\n\65\f\65\16\65\u0426\13\65\3\66\3\66\3\66\3\66\3\66\5" +
      "\66\u042d\n\66\3\67\3\67\38\38\58\u0433\n8\39\39\39\39\39\39\39\39\39" +
      "\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39" +
      "\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39" +
      "\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39\39" +
      "\39\39\39\39\39\39\39\59\u048a\n9\3:\3:\3:\2\b\26,<BDP;\2\4\6\b\n\f\16" +
      "\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bd" +
      "fhjlnpr\2\24\4\2\r\r##\4\2<<\u00b5\u00b5\4\2\20\20\u00cc\u00cc\3\2\u008f" +
      "\u0090\3\2\61\62\3\2./\4\2\20\20\23\23\3\2\u0093\u0095\3\2\u00c4\u00c5" +
      "\3\2\u00c6\u00c8\3\2\u00be\u00c3\3\2+,\3\2=B\3\2^_\3\2z{\3\2|}\3\2\u00af" +
      "\u00b0\3\2\u00b7\u00ba\u0570\2t\3\2\2\2\4}\3\2\2\2\6\u0080\3\2\2\2\b\u00c0" +
      "\3\2\2\2\n\u00c3\3\2\2\2\f\u00c7\3\2\2\2\16\u00d3\3\2\2\2\20\u00d6\3\2" +
      "\2\2\22\u00e1\3\2\2\2\24\u00e5\3\2\2\2\26\u00fd\3\2\2\2\30\u0121\3\2\2" +
      "\2\32\u0123\3\2\2\2\34\u012b\3\2\2\2\36\u0154\3\2\2\2 \u0186\3\2\2\2\"" +
      "\u0195\3\2\2\2$\u01a4\3\2\2\2&\u01a6\3\2\2\2(\u01af\3\2\2\2*\u01bd\3\2" +
      "\2\2,\u01bf\3\2\2\2.\u01e6\3\2\2\2\60\u01f6\3\2\2\2\62\u01f8\3\2\2\2\64" +
      "\u01fa\3\2\2\2\66\u0204\3\2\2\28\u0227\3\2\2\2:\u0229\3\2\2\2<\u022f\3" +
      "\2\2\2>\u023c\3\2\2\2@\u0277\3\2\2\2B\u027d\3\2\2\2D\u0358\3\2\2\2F\u036d" +
      "\3\2\2\2H\u036f\3\2\2\2J\u0371\3\2\2\2L\u0373\3\2\2\2N\u037d\3\2\2\2P" +
      "\u03a9\3\2\2\2R\u03b4\3\2\2\2T\u03b9\3\2\2\2V\u03bb\3\2\2\2X\u03c0\3\2" +
      "\2\2Z\u03ef\3\2\2\2\\\u03fa\3\2\2\2^\u0400\3\2\2\2`\u0407\3\2\2\2b\u0410" +
      "\3\2\2\2d\u0417\3\2\2\2f\u041d\3\2\2\2h\u041f\3\2\2\2j\u042c\3\2\2\2l" +
      "\u042e\3\2\2\2n\u0432\3\2\2\2p\u0489\3\2\2\2r\u048b\3\2\2\2tx\5\4\3\2" +
      "uw\5\4\3\2vu\3\2\2\2wz\3\2\2\2xv\3\2\2\2xy\3\2\2\2y{\3\2\2\2zx\3\2\2\2" +
      "{|\7\2\2\3|\3\3\2\2\2}~\5\b\5\2~\177\7\3\2\2\177\5\3\2\2\2\u0080\u0081" +
      "\5:\36\2\u0081\u0082\7\2\2\3\u0082\7\3\2\2\2\u0083\u00c1\5\n\6\2\u0084" +
      "\u0085\7\u0081\2\2\u0085\u0088\7\u0082\2\2\u0086\u0087\t\2\2\2\u0087\u0089" +
      "\5h\65\2\u0088\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089\u008c\3\2\2\2\u008a" +
      "\u008b\7(\2\2\u008b\u008d\7\u00ca\2\2\u008c\u008a\3\2\2\2\u008c\u008d" +
      "\3\2\2\2\u008d\u00c1\3\2\2\2\u008e\u008f\7\u0081\2\2\u008f\u00c1\7\u0083" +
      "\2\2\u0090\u0091\7o\2\2\u0091\u00c1\5h\65\2\u0092\u0093\7p\2\2\u0093\u0096" +
      "\5h\65\2\u0094\u0095\t\3\2\2\u0095\u0097\5n8\2\u0096\u0094\3\2\2\2\u0096" +
      "\u0097\3\2\2\2\u0097\u00c1\3\2\2\2\u0098\u0099\7\u0081\2\2\u0099\u00c1" +
      "\7\u0084\2\2\u009a\u009b\7\u0085\2\2\u009b\u00c1\5h\65\2\u009c\u009d\7" +
      "\u009f\2\2\u009d\u009e\5h\65\2\u009e\u009f\7\u00be\2\2\u009f\u00a0\5:" +
      "\36\2\u00a0\u00c1\3\2\2\2\u00a1\u00a2\7\u0086\2\2\u00a2\u00c1\5:\36\2" +
      "\u00a3\u00a4\7e\2\2\u00a4\u00a8\7g\2\2\u00a5\u00a6\7\u00bb\2\2\u00a6\u00a7" +
      "\7$\2\2\u00a7\u00a9\7&\2\2\u00a8\u00a5\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9" +
      "\u00aa\3\2\2\2\u00aa\u00ab\5h\65\2\u00ab\u00ac\7\4\2\2\u00ac\u00b1\5\16" +
      "\b\2\u00ad\u00ae\7\5\2\2\u00ae\u00b0\5\16\b\2\u00af\u00ad\3\2\2\2\u00b0" +
      "\u00b3\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b4\3\2" +
      "\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00b7\7\6\2\2\u00b5\u00b6\7b\2\2\u00b6" +
      "\u00b8\5\20\t\2\u00b7\u00b5\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00c1\3" +
      "\2\2\2\u00b9\u00ba\7\u008e\2\2\u00ba\u00bd\7g\2\2\u00bb\u00bc\7\u00bb" +
      "\2\2\u00bc\u00be\7&\2\2\u00bd\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be" +
      "\u00bf\3\2\2\2\u00bf\u00c1\5h\65\2\u00c0\u0083\3\2\2\2\u00c0\u0084\3\2" +
      "\2\2\u00c0\u008e\3\2\2\2\u00c0\u0090\3\2\2\2\u00c0\u0092\3\2\2\2\u00c0" +
      "\u0098\3\2\2\2\u00c0\u009a\3\2\2\2\u00c0\u009c\3\2\2\2\u00c0\u00a1\3\2" +
      "\2\2\u00c0\u00a3\3\2\2\2\u00c0\u00b9\3\2\2\2\u00c1\t\3\2\2\2\u00c2\u00c4" +
      "\5\f\7\2\u00c3\u00c2\3\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5" +
      "\u00c6\5\24\13\2\u00c6\13\3\2\2\2\u00c7\u00c9\7b\2\2\u00c8\u00ca\7c\2" +
      "\2\u00c9\u00c8\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb\u00d0" +
      "\5&\24\2\u00cc\u00cd\7\5\2\2\u00cd\u00cf\5&\24\2\u00ce\u00cc\3\2\2\2\u00cf" +
      "\u00d2\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\r\3\2\2\2" +
      "\u00d2\u00d0\3\2\2\2\u00d3\u00d4\5j\66\2\u00d4\u00d5\5P)\2\u00d5\17\3" +
      "\2\2\2\u00d6\u00d7\7\4\2\2\u00d7\u00dc\5\22\n\2\u00d8\u00d9\7\5\2\2\u00d9" +
      "\u00db\5\22\n\2\u00da\u00d8\3\2\2\2\u00db\u00de\3\2\2\2\u00dc\u00da\3" +
      "\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00df\3\2\2\2\u00de\u00dc\3\2\2\2\u00df" +
      "\u00e0\7\6\2\2\u00e0\21\3\2\2\2\u00e1\u00e2\5j\66\2\u00e2\u00e3\7\u00be" +
      "\2\2\u00e3\u00e4\5:\36\2\u00e4\23\3\2\2\2\u00e5\u00f0\5\26\f\2\u00e6\u00e7" +
      "\7\33\2\2\u00e7\u00e8\7\26\2\2\u00e8\u00ed\5\32\16\2\u00e9\u00ea\7\5\2" +
      "\2\u00ea\u00ec\5\32\16\2\u00eb\u00e9\3\2\2\2\u00ec\u00ef\3\2\2\2\u00ed" +
      "\u00eb\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00f1\3\2\2\2\u00ef\u00ed\3\2" +
      "\2\2\u00f0\u00e6\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f4\3\2\2\2\u00f2" +
      "\u00f3\7\35\2\2\u00f3\u00f5\t\4\2\2\u00f4\u00f2\3\2\2\2\u00f4\u00f5\3" +
      "\2\2\2\u00f5\u00fb\3\2\2\2\u00f6\u00f7\7\36\2\2\u00f7\u00f8\7\37\2\2\u00f8" +
      "\u00f9\5n8\2\u00f9\u00fa\7 \2\2\u00fa\u00fc\3\2\2\2\u00fb\u00f6\3\2\2" +
      "\2\u00fb\u00fc\3\2\2\2\u00fc\25\3\2\2\2\u00fd\u00fe\b\f\1\2\u00fe\u00ff" +
      "\5\30\r\2\u00ff\u010e\3\2\2\2\u0100\u0101\f\4\2\2\u0101\u0103\7\u0091" +
      "\2\2\u0102\u0104\5(\25\2\u0103\u0102\3\2\2\2\u0103\u0104\3\2\2\2\u0104" +
      "\u0105\3\2\2\2\u0105\u010d\5\26\f\5\u0106\u0107\f\3\2\2\u0107\u0109\t" +
      "\5\2\2\u0108\u010a\5(\25\2\u0109\u0108\3\2\2\2\u0109\u010a\3\2\2\2\u010a" +
      "\u010b\3\2\2\2\u010b\u010d\5\26\f\4\u010c\u0100\3\2\2\2\u010c\u0106\3" +
      "\2\2\2\u010d\u0110\3\2\2\2\u010e\u010c\3\2\2\2\u010e\u010f\3\2\2\2\u010f" +
      "\27\3\2\2\2\u0110\u010e\3\2\2\2\u0111\u0122\5\34\17\2\u0112\u0113\7f\2" +
      "\2\u0113\u0122\5h\65\2\u0114\u0115\7d\2\2\u0115\u011a\5:\36\2\u0116\u0117" +
      "\7\5\2\2\u0117\u0119\5:\36\2\u0118\u0116\3\2\2\2\u0119\u011c\3\2\2\2\u011a" +
      "\u0118\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u0122\3\2\2\2\u011c\u011a\3\2" +
      "\2\2\u011d\u011e\7\4\2\2\u011e\u011f\5\24\13\2\u011f\u0120\7\6\2\2\u0120" +
      "\u0122\3\2\2\2\u0121\u0111\3\2\2\2\u0121\u0112\3\2\2\2\u0121\u0114\3\2" +
      "\2\2\u0121\u011d\3\2\2\2\u0122\31\3\2\2\2\u0123\u0125\5:\36\2\u0124\u0126" +
      "\t\6\2\2\u0125\u0124\3\2\2\2\u0125\u0126\3\2\2\2\u0126\u0129\3\2\2\2\u0127" +
      "\u0128\7-\2\2\u0128\u012a\t\7\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2" +
      "\2\2\u012a\33\3\2\2\2\u012b\u012d\7\f\2\2\u012c\u012e\5(\25\2\u012d\u012c" +
      "\3\2\2\2\u012d\u012e\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0134\5*\26\2\u0130" +
      "\u0131\7\5\2\2\u0131\u0133\5*\26\2\u0132\u0130\3\2\2\2\u0133\u0136\3\2" +
      "\2\2\u0134\u0132\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0139\3\2\2\2\u0136" +
      "\u0134\3\2\2\2\u0137\u0138\7m\2\2\u0138\u013a\58\35\2\u0139\u0137\3\2" +
      "\2\2\u0139\u013a\3\2\2\2\u013a\u0144\3\2\2\2\u013b\u013c\7\r\2\2\u013c" +
      "\u0141\5,\27\2\u013d\u013e\7\5\2\2\u013e\u0140\5,\27\2\u013f\u013d\3\2" +
      "\2\2\u0140\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0141\u0142\3\2\2\2\u0142" +
      "\u0145\3\2\2\2\u0143\u0141\3\2\2\2\u0144\u013b\3\2\2\2\u0144\u0145\3\2" +
      "\2\2\u0145\u0148\3\2\2\2\u0146\u0147\7\24\2\2\u0147\u0149\5<\37\2\u0148" +
      "\u0146\3\2\2\2\u0148\u0149\3\2\2\2\u0149\u014d\3\2\2\2\u014a\u014b\7\25" +
      "\2\2\u014b\u014c\7\26\2\2\u014c\u014e\5\36\20\2\u014d\u014a\3\2\2\2\u014d" +
      "\u014e\3\2\2\2\u014e\u0151\3\2\2\2\u014f\u0150\7\34\2\2\u0150\u0152\5" +
      "<\37\2\u0151\u014f\3\2\2\2\u0151\u0152\3\2\2\2\u0152\35\3\2\2\2\u0153" +
      "\u0155\5(\25\2\u0154\u0153\3\2\2\2\u0154\u0155\3\2\2\2\u0155\u0156\3\2" +
      "\2\2\u0156\u015b\5 \21\2\u0157\u0158\7\5\2\2\u0158\u015a\5 \21\2\u0159" +
      "\u0157\3\2\2\2\u015a\u015d\3\2\2\2\u015b\u0159\3\2\2\2\u015b\u015c\3\2" +
      "\2\2\u015c\37\3\2\2\2\u015d\u015b\3\2\2\2\u015e\u0187\5\"\22\2\u015f\u0160" +
      "\7\32\2\2\u0160\u0169\7\4\2\2\u0161\u0166\5h\65\2\u0162\u0163\7\5\2\2" +
      "\u0163\u0165\5h\65\2\u0164\u0162\3\2\2\2\u0165\u0168\3\2\2\2\u0166\u0164" +
      "\3\2\2\2\u0166\u0167\3\2\2\2\u0167\u016a\3\2\2\2\u0168\u0166\3\2\2\2\u0169" +
      "\u0161\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b\3\2\2\2\u016b\u0187\7\6" +
      "\2\2\u016c\u016d\7\31\2\2\u016d\u0176\7\4\2\2\u016e\u0173\5h\65\2\u016f" +
      "\u0170\7\5\2\2\u0170\u0172\5h\65\2\u0171\u016f\3\2\2\2\u0172\u0175\3\2" +
      "\2\2\u0173\u0171\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0177\3\2\2\2\u0175" +
      "\u0173\3\2\2\2\u0176\u016e\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u0178\3\2" +
      "\2\2\u0178\u0187\7\6\2\2\u0179\u017a\7\27\2\2\u017a\u017b\7\30\2\2\u017b" +
      "\u017c\7\4\2\2\u017c\u0181\5$\23\2\u017d\u017e\7\5\2\2\u017e\u0180\5$" +
      "\23\2\u017f\u017d\3\2\2\2\u0180\u0183\3\2\2\2\u0181\u017f\3\2\2\2\u0181" +
      "\u0182\3\2\2\2\u0182\u0184\3\2\2\2\u0183\u0181\3\2\2\2\u0184\u0185\7\6" +
      "\2\2\u0185\u0187\3\2\2\2\u0186\u015e\3\2\2\2\u0186\u015f\3\2\2\2\u0186" +
      "\u016c\3\2\2\2\u0186\u0179\3\2\2\2\u0187!\3\2\2\2\u0188\u0191\7\4\2\2" +
      "\u0189\u018e\5:\36\2\u018a\u018b\7\5\2\2\u018b\u018d\5:\36\2\u018c\u018a" +
      "\3\2\2\2\u018d\u0190\3\2\2\2\u018e\u018c\3\2\2\2\u018e\u018f\3\2\2\2\u018f" +
      "\u0192\3\2\2\2\u0190\u018e\3\2\2\2\u0191\u0189\3\2\2\2\u0191\u0192\3\2" +
      "\2\2\u0192\u0193\3\2\2\2\u0193\u0196\7\6\2\2\u0194\u0196\5:\36\2\u0195" +
      "\u0188\3\2\2\2\u0195\u0194\3\2\2\2\u0196#\3\2\2\2\u0197\u01a0\7\4\2\2" +
      "\u0198\u019d\5h\65\2\u0199\u019a\7\5\2\2\u019a\u019c\5h\65\2\u019b\u0199" +
      "\3\2\2\2\u019c\u019f\3\2\2\2\u019d\u019b\3\2\2\2\u019d\u019e\3\2\2\2\u019e" +
      "\u01a1\3\2\2\2\u019f\u019d\3\2\2\2\u01a0\u0198\3\2\2\2\u01a0\u01a1\3\2" +
      "\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01a5\7\6\2\2\u01a3\u01a5\5h\65\2\u01a4" +
      "\u0197\3\2\2\2\u01a4\u01a3\3\2\2\2\u01a5%\3\2\2\2\u01a6\u01a8\5j\66\2" +
      "\u01a7\u01a9\5\66\34\2\u01a8\u01a7\3\2\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa" +
      "\3\2\2\2\u01aa\u01ab\7\17\2\2\u01ab\u01ac\7\4\2\2\u01ac\u01ad\5\n\6\2" +
      "\u01ad\u01ae\7\6\2\2\u01ae\'\3\2\2\2\u01af\u01b0\t\b\2\2\u01b0)\3\2\2" +
      "\2\u01b1\u01b6\5:\36\2\u01b2\u01b4\7\17\2\2\u01b3\u01b2\3\2\2\2\u01b3" +
      "\u01b4\3\2\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01b7\5j\66\2\u01b6\u01b3\3\2" +
      "\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01be\3\2\2\2\u01b8\u01b9\5h\65\2\u01b9" +
      "\u01ba\7\7\2\2\u01ba\u01bb\7\u00c6\2\2\u01bb\u01be\3\2\2\2\u01bc\u01be" +
      "\7\u00c6\2\2\u01bd\u01b1\3\2\2\2\u01bd\u01b8\3\2\2\2\u01bd\u01bc\3\2\2" +
      "\2\u01be+\3\2\2\2\u01bf\u01c0\b\27\1\2\u01c0\u01c1\5\64\33\2\u01c1\u01d4" +
      "\3\2\2\2\u01c2\u01d0\f\4\2\2\u01c3\u01c4\7P\2\2\u01c4\u01c5\7O\2\2\u01c5" +
      "\u01d1\5\64\33\2\u01c6\u01c7\5.\30\2\u01c7\u01c8\7O\2\2\u01c8\u01c9\5" +
      ",\27\2\u01c9\u01ca\5\60\31\2\u01ca\u01d1\3\2\2\2\u01cb\u01cc\7V\2\2\u01cc" +
      "\u01cd\5.\30\2\u01cd\u01ce\7O\2\2\u01ce\u01cf\5\64\33\2\u01cf\u01d1\3" +
      "\2\2\2\u01d0\u01c3\3\2\2\2\u01d0\u01c6\3\2\2\2\u01d0\u01cb\3\2\2\2\u01d1" +
      "\u01d3\3\2\2\2\u01d2\u01c2\3\2\2\2\u01d3\u01d6\3\2\2\2\u01d4\u01d2\3\2" +
      "\2\2\u01d4\u01d5\3\2\2\2\u01d5-\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d7\u01d9" +
      "\7R\2\2\u01d8\u01d7\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9\u01e7\3\2\2\2\u01da" +
      "\u01dc\7S\2\2\u01db\u01dd\7Q\2\2\u01dc\u01db\3\2\2\2\u01dc\u01dd\3\2\2" +
      "\2\u01dd\u01e7\3\2\2\2\u01de\u01e0\7T\2\2\u01df\u01e1\7Q\2\2\u01e0\u01df" +
      "\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1\u01e7\3\2\2\2\u01e2\u01e4\7U\2\2\u01e3" +
      "\u01e5\7Q\2\2\u01e4\u01e3\3\2\2\2\u01e4\u01e5\3\2\2\2\u01e5\u01e7\3\2" +
      "\2\2\u01e6\u01d8\3\2\2\2\u01e6\u01da\3\2\2\2\u01e6\u01de\3\2\2\2\u01e6" +
      "\u01e2\3\2\2\2\u01e7/\3\2\2\2\u01e8\u01e9\7X\2\2\u01e9\u01f7\5<\37\2\u01ea" +
      "\u01eb\7W\2\2\u01eb\u01ec\7\4\2\2\u01ec\u01f1\5j\66\2\u01ed\u01ee\7\5" +
      "\2\2\u01ee\u01f0\5j\66\2\u01ef\u01ed\3\2\2\2\u01f0\u01f3\3\2\2\2\u01f1" +
      "\u01ef\3\2\2\2\u01f1\u01f2\3\2\2\2\u01f2\u01f4\3\2\2\2\u01f3\u01f1\3\2" +
      "\2\2\u01f4\u01f5\7\6\2\2\u01f5\u01f7\3\2\2\2\u01f6\u01e8\3\2\2\2\u01f6" +
      "\u01ea\3\2\2\2\u01f7\61\3\2\2\2\u01f8\u01f9\t\t\2\2\u01f9\63\3\2\2\2\u01fa" +
      "\u0202\58\35\2\u01fb\u01fd\7\17\2\2\u01fc\u01fb\3\2\2\2\u01fc\u01fd\3" +
      "\2\2\2\u01fd\u01fe\3\2\2\2\u01fe\u0200\5j\66\2\u01ff\u0201\5\66\34\2\u0200" +
      "\u01ff\3\2\2\2\u0200\u0201\3\2\2\2\u0201\u0203\3\2\2\2\u0202\u01fc\3\2" +
      "\2\2\u0202\u0203\3\2\2\2\u0203\65\3\2\2\2\u0204\u0205\7\4\2\2\u0205\u020a" +
      "\5j\66\2\u0206\u0207\7\5\2\2\u0207\u0209\5j\66\2\u0208\u0206\3\2\2\2\u0209" +
      "\u020c\3\2\2\2\u020a\u0208\3\2\2\2\u020a\u020b\3\2\2\2\u020b\u020d\3\2" +
      "\2\2\u020c\u020a\3\2\2\2\u020d\u020e\7\6\2\2\u020e\67\3\2\2\2\u020f\u0228" +
      "\5h\65\2\u0210\u0211\7\4\2\2\u0211\u0212\5\n\6\2\u0212\u0213\7\6\2\2\u0213" +
      "\u0228\3\2\2\2\u0214\u0215\7\u009b\2\2\u0215\u0216\7\4\2\2\u0216\u021b" +
      "\5:\36\2\u0217\u0218\7\5\2\2\u0218\u021a\5:\36\2\u0219\u0217\3\2\2\2\u021a" +
      "\u021d\3\2\2\2\u021b\u0219\3\2\2\2\u021b\u021c\3\2\2\2\u021c\u021e\3\2" +
      "\2\2\u021d\u021b\3\2\2\2\u021e\u0221\7\6\2\2\u021f\u0220\7b\2\2\u0220" +
      "\u0222\7\u009c\2\2\u0221\u021f\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0228" +
      "\3\2\2\2\u0223\u0224\7\4\2\2\u0224\u0225\5,\27\2\u0225\u0226\7\6\2\2\u0226" +
      "\u0228\3\2\2\2\u0227\u020f\3\2\2\2\u0227\u0210\3\2\2\2\u0227\u0214\3\2" +
      "\2\2\u0227\u0223\3\2\2\2\u02289\3\2\2\2\u0229\u022a\5<\37\2\u022a;\3\2" +
      "\2\2\u022b\u022c\b\37\1\2\u022c\u0230\5> \2\u022d\u022e\7$\2\2\u022e\u0230" +
      "\5<\37\5\u022f\u022b\3\2\2\2\u022f\u022d\3\2\2\2\u0230\u0239\3\2\2\2\u0231" +
      "\u0232\f\4\2\2\u0232\u0233\7\"\2\2\u0233\u0238\5<\37\5\u0234\u0235\f\3" +
      "\2\2\u0235\u0236\7!\2\2\u0236\u0238\5<\37\4\u0237\u0231\3\2\2\2\u0237" +
      "\u0234\3\2\2\2\u0238\u023b\3\2\2\2\u0239\u0237\3\2\2\2\u0239\u023a\3\2" +
      "\2\2\u023a=\3\2\2\2\u023b\u0239\3\2\2\2\u023c\u023e\5B\"\2\u023d\u023f" +
      "\5@!\2\u023e\u023d\3\2\2\2\u023e\u023f\3\2\2\2\u023f?\3\2\2\2\u0240\u0241" +
      "\5H%\2\u0241\u0242\5B\"\2\u0242\u0278\3\2\2\2\u0243\u0245\7$\2\2\u0244" +
      "\u0243\3\2\2\2\u0244\u0245\3\2\2\2\u0245\u0246\3\2\2\2\u0246\u0247\7\'" +
      "\2\2\u0247\u0248\5B\"\2\u0248\u0249\7\"\2\2\u0249\u024a\5B\"\2\u024a\u0278" +
      "\3\2\2\2\u024b\u024d\7$\2\2\u024c\u024b\3\2\2\2\u024c\u024d\3\2\2\2\u024d" +
      "\u024e\3\2\2\2\u024e\u024f\7#\2\2\u024f\u0250\7\4\2\2\u0250\u0255\5:\36" +
      "\2\u0251\u0252\7\5\2\2\u0252\u0254\5:\36\2\u0253\u0251\3\2\2\2\u0254\u0257" +
      "\3\2\2\2\u0255\u0253\3\2\2\2\u0255\u0256\3\2\2\2\u0256\u0258\3\2\2\2\u0257" +
      "\u0255\3\2\2\2\u0258\u0259\7\6\2\2\u0259\u0278\3\2\2\2\u025a\u025c\7$" +
      "\2\2\u025b\u025a\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025d\3\2\2\2\u025d" +
      "\u025e\7#\2\2\u025e\u025f\7\4\2\2\u025f\u0260\5\n\6\2\u0260\u0261\7\6" +
      "\2\2\u0261\u0278\3\2\2\2\u0262\u0264\7$\2\2\u0263\u0262\3\2\2\2\u0263" +
      "\u0264\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0266\7(\2\2\u0266\u0269\5B\"" +
      "\2\u0267\u0268\7\60\2\2\u0268\u026a\5B\"\2\u0269\u0267\3\2\2\2\u0269\u026a" +
      "\3\2\2\2\u026a\u0278\3\2\2\2\u026b\u026d\7)\2\2\u026c\u026e\7$\2\2\u026d" +
      "\u026c\3\2\2\2\u026d\u026e\3\2\2\2\u026e\u026f\3\2\2\2\u026f\u0278\7*" +
      "\2\2\u0270\u0272\7)\2\2\u0271\u0273\7$\2\2\u0272\u0271\3\2\2\2\u0272\u0273" +
      "\3\2\2\2\u0273\u0274\3\2\2\2\u0274\u0275\7\23\2\2\u0275\u0276\7\r\2\2" +
      "\u0276\u0278\5B\"\2\u0277\u0240\3\2\2\2\u0277\u0244\3\2\2\2\u0277\u024c" +
      "\3\2\2\2\u0277\u025b\3\2\2\2\u0277\u0263\3\2\2\2\u0277\u026b\3\2\2\2\u0277" +
      "\u0270\3\2\2\2\u0278A\3\2\2\2\u0279\u027a\b\"\1\2\u027a\u027e\5D#\2\u027b" +
      "\u027c\t\n\2\2\u027c\u027e\5B\"\6\u027d\u0279\3\2\2\2\u027d\u027b\3\2" +
      "\2\2\u027e\u028d\3\2\2\2\u027f\u0280\f\5\2\2\u0280\u0281\t\13\2\2\u0281" +
      "\u028c\5B\"\6\u0282\u0283\f\4\2\2\u0283\u0284\t\n\2\2\u0284\u028c\5B\"" +
      "\5\u0285\u0286\f\3\2\2\u0286\u0287\7\u00c9\2\2\u0287\u028c\5B\"\4\u0288" +
      "\u0289\f\7\2\2\u0289\u028a\7\37\2\2\u028a\u028c\5F$\2\u028b\u027f\3\2" +
      "\2\2\u028b\u0282\3\2\2\2\u028b\u0285\3\2\2\2\u028b\u0288\3\2\2\2\u028c" +
      "\u028f\3\2\2\2\u028d\u028b\3\2\2\2\u028d\u028e\3\2\2\2\u028eC\3\2\2\2" +
      "\u028f\u028d\3\2\2\2\u0290\u0291\b#\1\2\u0291\u0359\7*\2\2\u0292\u0359" +
      "\5L\'\2\u0293\u0294\5j\66\2\u0294\u0295\7\u00ca\2\2\u0295\u0359\3\2\2" +
      "\2\u0296\u0359\5n8\2\u0297\u0359\5J&\2\u0298\u0359\7\u00ca\2\2\u0299\u0359" +
      "\7\u00cb\2\2\u029a\u029b\7\64\2\2\u029b\u029c\7\4\2\2\u029c\u029d\5B\"" +
      "\2\u029d\u029e\7#\2\2\u029e\u029f\5B\"\2\u029f\u02a0\7\6\2\2\u02a0\u0359" +
      "\3\2\2\2\u02a1\u02a2\7\4\2\2\u02a2\u02a5\5:\36\2\u02a3\u02a4\7\5\2\2\u02a4" +
      "\u02a6\5:\36\2\u02a5\u02a3\3\2\2\2\u02a6\u02a7\3\2\2\2\u02a7\u02a5\3\2" +
      "\2\2\u02a7\u02a8\3\2\2\2\u02a8\u02a9\3\2\2\2\u02a9\u02aa\7\6\2\2\u02aa" +
      "\u0359\3\2\2\2\u02ab\u02ac\7a\2\2\u02ac\u02ad\7\4\2\2\u02ad\u02b2\5:\36" +
      "\2\u02ae\u02af\7\5\2\2\u02af\u02b1\5:\36\2\u02b0\u02ae\3\2\2\2\u02b1\u02b4" +
      "\3\2\2\2\u02b2\u02b0\3\2\2\2\u02b2\u02b3\3\2\2\2\u02b3\u02b5\3\2\2\2\u02b4" +
      "\u02b2\3\2\2\2\u02b5\u02b6\7\6\2\2\u02b6\u0359\3\2\2\2\u02b7\u02b8\5h" +
      "\65\2\u02b8\u02b9\7\4\2\2\u02b9\u02ba\7\u00c6\2\2\u02ba\u02bc\7\6\2\2" +
      "\u02bb\u02bd\5X-\2\u02bc\u02bb\3\2\2\2\u02bc\u02bd\3\2\2\2\u02bd\u0359" +
      "\3\2\2\2\u02be\u02bf\5h\65\2\u02bf\u02cb\7\4\2\2\u02c0\u02c2\5(\25\2\u02c1" +
      "\u02c0\3\2\2\2\u02c1\u02c2\3\2\2\2\u02c2\u02c3\3\2\2\2\u02c3\u02c8\5:" +
      "\36\2\u02c4\u02c5\7\5\2\2\u02c5\u02c7\5:\36\2\u02c6\u02c4\3\2\2\2\u02c7" +
      "\u02ca\3\2\2\2\u02c8\u02c6\3\2\2\2\u02c8\u02c9\3\2\2\2\u02c9\u02cc\3\2" +
      "\2\2\u02ca\u02c8\3\2\2\2\u02cb\u02c1\3\2\2\2\u02cb\u02cc\3\2\2\2\u02cc" +
      "\u02cd\3\2\2\2\u02cd\u02cf\7\6\2\2\u02ce\u02d0\5X-\2\u02cf\u02ce\3\2\2" +
      "\2\u02cf\u02d0\3\2\2\2\u02d0\u0359\3\2\2\2\u02d1\u02d2\5j\66\2\u02d2\u02d3" +
      "\7\b\2\2\u02d3\u02d4\5:\36\2\u02d4\u0359\3\2\2\2\u02d5\u02d6\7\4\2\2\u02d6" +
      "\u02db\5j\66\2\u02d7\u02d8\7\5\2\2\u02d8\u02da\5j\66\2\u02d9\u02d7\3\2" +
      "\2\2\u02da\u02dd\3\2\2\2\u02db\u02d9\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc" +
      "\u02de\3\2\2\2\u02dd\u02db\3\2\2\2\u02de\u02df\7\6\2\2\u02df\u02e0\7\b" +
      "\2\2\u02e0\u02e1\5:\36\2\u02e1\u0359\3\2\2\2\u02e2\u02e3\7\4\2\2\u02e3" +
      "\u02e4\5\n\6\2\u02e4\u02e5\7\6\2\2\u02e5\u0359\3\2\2\2\u02e6\u02e7\7&" +
      "\2\2\u02e7\u02e8\7\4\2\2\u02e8\u02e9\5\n\6\2\u02e9\u02ea\7\6\2\2\u02ea" +
      "\u0359\3\2\2\2\u02eb\u02ec\7J\2\2\u02ec\u02ee\5B\"\2\u02ed\u02ef\5V,\2" +
      "\u02ee\u02ed\3\2\2\2\u02ef\u02f0\3\2\2\2\u02f0\u02ee\3\2\2\2\u02f0\u02f1" +
      "\3\2\2\2\u02f1\u02f4\3\2\2\2\u02f2\u02f3\7M\2\2\u02f3\u02f5\5:\36\2\u02f4" +
      "\u02f2\3\2\2\2\u02f4\u02f5\3\2\2\2\u02f5\u02f6\3\2\2\2\u02f6\u02f7\7N" +
      "\2\2\u02f7\u0359\3\2\2\2\u02f8\u02fa\7J\2\2\u02f9\u02fb\5V,\2\u02fa\u02f9" +
      "\3\2\2\2\u02fb\u02fc\3\2\2\2\u02fc\u02fa\3\2\2\2\u02fc\u02fd\3\2\2\2\u02fd" +
      "\u0300\3\2\2\2\u02fe\u02ff\7M\2\2\u02ff\u0301\5:\36\2\u0300\u02fe\3\2" +
      "\2\2\u0300\u0301\3\2\2\2\u0301\u0302\3\2\2\2\u0302\u0303\7N\2\2\u0303" +
      "\u0359\3\2\2\2\u0304\u0305\7\177\2\2\u0305\u0306\7\4\2\2\u0306\u0307\5" +
      ":\36\2\u0307\u0308\7\17\2\2\u0308\u0309\5P)\2\u0309\u030a\7\6\2\2\u030a" +
      "\u0359\3\2\2\2\u030b\u030c\7\u0080\2\2\u030c\u030d\7\4\2\2\u030d\u030e" +
      "\5:\36\2\u030e\u030f\7\17\2\2\u030f\u0310\5P)\2\u0310\u0311\7\6\2\2\u0311" +
      "\u0359\3\2\2\2\u0312\u0313\7\u009d\2\2\u0313\u031c\7\t\2\2\u0314\u0319" +
      "\5:\36\2\u0315\u0316\7\5\2\2\u0316\u0318\5:\36\2\u0317\u0315\3\2\2\2\u0318" +
      "\u031b\3\2\2\2\u0319\u0317\3\2\2\2\u0319\u031a\3\2\2\2\u031a\u031d\3\2" +
      "\2\2\u031b\u0319\3\2\2\2\u031c\u0314\3\2\2\2\u031c\u031d\3\2\2\2\u031d" +
      "\u031e\3\2\2\2\u031e\u0359\7\n\2\2\u031f\u0359\5j\66\2\u0320\u0359\7D" +
      "\2\2\u0321\u0325\7E\2\2\u0322\u0323\7\4\2\2\u0323\u0324\7\u00cc\2\2\u0324" +
      "\u0326\7\6\2\2\u0325\u0322\3\2\2\2\u0325\u0326\3\2\2\2\u0326\u0359\3\2" +
      "\2\2\u0327\u032b\7F\2\2\u0328\u0329\7\4\2\2\u0329\u032a\7\u00cc\2\2\u032a" +
      "\u032c\7\6\2\2\u032b\u0328\3\2\2\2\u032b\u032c\3\2\2\2\u032c\u0359\3\2" +
      "\2\2\u032d\u0331\7G\2\2\u032e\u032f\7\4\2\2\u032f\u0330\7\u00cc\2\2\u0330" +
      "\u0332\7\6\2\2\u0331\u032e\3\2\2\2\u0331\u0332\3\2\2\2\u0332\u0359\3\2" +
      "\2\2\u0333\u0337\7H\2\2\u0334\u0335\7\4\2\2\u0335\u0336\7\u00cc\2\2\u0336" +
      "\u0338\7\6\2\2\u0337\u0334\3\2\2\2\u0337\u0338\3\2\2\2\u0338\u0359\3\2" +
      "\2\2\u0339\u033a\7\63\2\2\u033a\u033b\7\4\2\2\u033b\u033c\5B\"\2\u033c" +
      "\u033d\7\r\2\2\u033d\u0340\5B\"\2\u033e\u033f\7\65\2\2\u033f\u0341\5B" +
      "\"\2\u0340\u033e\3\2\2\2\u0340\u0341\3\2\2\2\u0341\u0342\3\2\2\2\u0342" +
      "\u0343\7\6\2\2\u0343\u0359\3\2\2\2\u0344\u0345\7\u00b6\2\2\u0345\u0346" +
      "\7\4\2\2\u0346\u0349\5B\"\2\u0347\u0348\7\5\2\2\u0348\u034a\5r:\2\u0349" +
      "\u0347\3\2\2\2\u0349\u034a\3\2\2\2\u034a\u034b\3\2\2\2\u034b\u034c\7\6" +
      "\2\2\u034c\u0359\3\2\2\2\u034d\u034e\7I\2\2\u034e\u034f\7\4\2\2\u034f" +
      "\u0350\5j\66\2\u0350\u0351\7\r\2\2\u0351\u0352\5B\"\2\u0352\u0353\7\6" +
      "\2\2\u0353\u0359\3\2\2\2\u0354\u0355\7\4\2\2\u0355\u0356\5:\36\2\u0356" +
      "\u0357\7\6\2\2\u0357\u0359\3\2\2\2\u0358\u0290\3\2\2\2\u0358\u0292\3\2" +
      "\2\2\u0358\u0293\3\2\2\2\u0358\u0296\3\2\2\2\u0358\u0297\3\2\2\2\u0358" +
      "\u0298\3\2\2\2\u0358\u0299\3\2\2\2\u0358\u029a\3\2\2\2\u0358\u02a1\3\2" +
      "\2\2\u0358\u02ab\3\2\2\2\u0358\u02b7\3\2\2\2\u0358\u02be\3\2\2\2\u0358" +
      "\u02d1\3\2\2\2\u0358\u02d5\3\2\2\2\u0358\u02e2\3\2\2\2\u0358\u02e6\3\2" +
      "\2\2\u0358\u02eb\3\2\2\2\u0358\u02f8\3\2\2\2\u0358\u0304\3\2\2\2\u0358" +
      "\u030b\3\2\2\2\u0358\u0312\3\2\2\2\u0358\u031f\3\2\2\2\u0358\u0320\3\2" +
      "\2\2\u0358\u0321\3\2\2\2\u0358\u0327\3\2\2\2\u0358\u032d\3\2\2\2\u0358" +
      "\u0333\3\2\2\2\u0358\u0339\3\2\2\2\u0358\u0344\3\2\2\2\u0358\u034d\3\2" +
      "\2\2\u0358\u0354\3\2\2\2\u0359\u0364\3\2\2\2\u035a\u035b\f\16\2\2\u035b" +
      "\u035c\7\t\2\2\u035c\u035d\5B\"\2\u035d\u035e\7\n\2\2\u035e\u0363\3\2" +
      "\2\2\u035f\u0360\f\f\2\2\u0360\u0361\7\7\2\2\u0361\u0363\5j\66\2\u0362" +
      "\u035a\3\2\2\2\u0362\u035f\3\2\2\2\u0363\u0366\3\2\2\2\u0364\u0362\3\2" +
      "\2\2\u0364\u0365\3\2\2\2\u0365E\3\2\2\2\u0366\u0364\3\2\2\2\u0367\u0368" +
      "\7:\2\2\u0368\u0369\7C\2\2\u0369\u036e\5L\'\2\u036a\u036b\7:\2\2\u036b" +
      "\u036c\7C\2\2\u036c\u036e\7\u00ca\2\2\u036d\u0367\3\2\2\2\u036d\u036a" +
      "\3\2\2\2\u036eG\3\2\2\2\u036f\u0370\t\f\2\2\u0370I\3\2\2\2\u0371\u0372" +
      "\t\r\2\2\u0372K\3\2\2\2\u0373\u0375\7<\2\2\u0374\u0376\t\n\2\2\u0375\u0374" +
      "\3\2\2\2\u0375\u0376\3\2\2\2\u0376\u0377\3\2\2\2\u0377\u0378\7\u00ca\2" +
      "\2\u0378\u037b\5N(\2\u0379\u037a\7\u0092\2\2\u037a\u037c\5N(\2\u037b\u0379" +
      "\3\2\2\2\u037b\u037c\3\2\2\2\u037cM\3\2\2\2\u037d\u037e\t\16\2\2\u037e" +
      "O\3\2\2\2\u037f\u0380\b)\1\2\u0380\u0381\7\u009d\2\2\u0381\u0382\7\u00c0" +
      "\2\2\u0382\u0383\5P)\2\u0383\u0384\7\u00c2\2\2\u0384\u03aa\3\2\2\2\u0385" +
      "\u0386\7\u009e\2\2\u0386\u0387\7\u00c0\2\2\u0387\u0388\5P)\2\u0388\u0389" +
      "\7\5\2\2\u0389\u038a\5P)\2\u038a\u038b\7\u00c2\2\2\u038b\u03aa\3\2\2\2" +
      "\u038c\u038d\7a\2\2\u038d\u038e\7\4\2\2\u038e\u038f\5j\66\2\u038f\u0396" +
      "\5P)\2\u0390\u0391\7\5\2\2\u0391\u0392\5j\66\2\u0392\u0393\5P)\2\u0393" +
      "\u0395\3\2\2\2\u0394\u0390\3\2\2\2\u0395\u0398\3\2\2\2\u0396\u0394\3\2" +
      "\2\2\u0396\u0397\3\2\2\2\u0397\u0399\3\2\2\2\u0398\u0396\3\2\2\2\u0399" +
      "\u039a\7\6\2\2\u039a\u03aa\3\2\2\2\u039b\u03a7\5T+\2\u039c\u039d\7\4\2" +
      "\2\u039d\u03a2\5R*\2\u039e\u039f\7\5\2\2\u039f\u03a1\5R*\2\u03a0\u039e" +
      "\3\2\2\2\u03a1\u03a4\3\2\2\2\u03a2\u03a0\3\2\2\2\u03a2\u03a3\3\2\2\2\u03a3" +
      "\u03a5\3\2\2\2\u03a4\u03a2\3\2\2\2\u03a5\u03a6\7\6\2\2\u03a6\u03a8\3\2" +
      "\2\2\u03a7\u039c\3\2\2\2\u03a7\u03a8\3\2\2\2\u03a8\u03aa\3\2\2\2\u03a9" +
      "\u037f\3\2\2\2\u03a9\u0385\3\2\2\2\u03a9\u038c\3\2\2\2\u03a9\u039b\3\2" +
      "\2\2\u03aa\u03af\3\2\2\2\u03ab\u03ac\f\7\2\2\u03ac\u03ae\7\u009d\2\2\u03ad" +
      "\u03ab\3\2\2\2\u03ae\u03b1\3\2\2\2\u03af\u03ad\3\2\2\2\u03af\u03b0\3\2" +
      "\2\2\u03b0Q\3\2\2\2\u03b1\u03af\3\2\2\2\u03b2\u03b5\7\u00cc\2\2\u03b3" +
      "\u03b5\5P)\2\u03b4\u03b2\3\2\2\2\u03b4\u03b3\3\2\2\2\u03b5S\3\2\2\2\u03b6" +
      "\u03ba\7\u00d2\2\2\u03b7\u03ba\7\u00d3\2\2\u03b8\u03ba\5j\66\2\u03b9\u03b6" +
      "\3\2\2\2\u03b9\u03b7\3\2\2\2\u03b9\u03b8\3\2\2\2\u03baU\3\2\2\2\u03bb" +
      "\u03bc\7K\2\2\u03bc\u03bd\5:\36\2\u03bd\u03be\7L\2\2\u03be\u03bf\5:\36" +
      "\2\u03bfW\3\2\2\2\u03c0\u03c1\7Y\2\2\u03c1\u03cc\7\4\2\2\u03c2\u03c3\7" +
      "Z\2\2\u03c3\u03c4\7\26\2\2\u03c4\u03c9\5:\36\2\u03c5\u03c6\7\5\2\2\u03c6" +
      "\u03c8\5:\36\2\u03c7\u03c5\3\2\2\2\u03c8\u03cb\3\2\2\2\u03c9\u03c7\3\2" +
      "\2\2\u03c9\u03ca\3\2\2\2\u03ca\u03cd\3\2\2\2\u03cb\u03c9\3\2\2\2\u03cc" +
      "\u03c2\3\2\2\2\u03cc\u03cd\3\2\2\2\u03cd\u03d8\3\2\2\2\u03ce\u03cf\7\33" +
      "\2\2\u03cf\u03d0\7\26\2\2\u03d0\u03d5\5\32\16\2\u03d1\u03d2\7\5\2\2\u03d2" +
      "\u03d4\5\32\16\2\u03d3\u03d1\3\2\2\2\u03d4\u03d7\3\2\2\2\u03d5\u03d3\3" +
      "\2\2\2\u03d5\u03d6\3\2\2\2\u03d6\u03d9\3\2\2\2\u03d7\u03d5\3\2\2\2\u03d8" +
      "\u03ce\3\2\2\2\u03d8\u03d9\3\2\2\2\u03d9\u03db\3\2\2\2\u03da\u03dc\5Z" +
      ".\2\u03db\u03da\3\2\2\2\u03db\u03dc\3\2\2\2\u03dc\u03dd\3\2\2\2\u03dd" +
      "\u03de\7\6\2\2\u03deY\3\2\2\2\u03df\u03e0\7[\2\2\u03e0\u03f0\5\\/\2\u03e1" +
      "\u03e2\7\\\2\2\u03e2\u03f0\5\\/\2\u03e3\u03e4\7[\2\2\u03e4\u03e5\7\'\2" +
      "\2\u03e5\u03e6\5\\/\2\u03e6\u03e7\7\"\2\2\u03e7\u03e8\5\\/\2\u03e8\u03f0" +
      "\3\2\2\2\u03e9\u03ea\7\\\2\2\u03ea\u03eb\7\'\2\2\u03eb\u03ec\5\\/\2\u03ec" +
      "\u03ed\7\"\2\2\u03ed\u03ee\5\\/\2\u03ee\u03f0\3\2\2\2\u03ef\u03df\3\2" +
      "\2\2\u03ef\u03e1\3\2\2\2\u03ef\u03e3\3\2\2\2\u03ef\u03e9\3\2\2\2\u03f0" +
      "[\3\2\2\2\u03f1\u03f2\7]\2\2\u03f2\u03fb\7^\2\2\u03f3\u03f4\7]\2\2\u03f4" +
      "\u03fb\7_\2\2\u03f5\u03f6\7`\2\2\u03f6\u03fb\7a\2\2\u03f7\u03f8\5:\36" +
      "\2\u03f8\u03f9\t\17\2\2\u03f9\u03fb\3\2\2\2\u03fa\u03f1\3\2\2\2\u03fa" +
      "\u03f3\3\2\2\2\u03fa\u03f5\3\2\2\2\u03fa\u03f7\3\2\2\2\u03fb]\3\2\2\2" +
      "\u03fc\u03fd\7x\2\2\u03fd\u0401\t\20\2\2\u03fe\u03ff\7y\2\2\u03ff\u0401" +
      "\t\21\2\2\u0400\u03fc\3\2\2\2\u0400\u03fe\3\2\2\2\u0401_\3\2\2\2\u0402" +
      "\u0403\7\u00a8\2\2\u0403\u0404\7\u00a9\2\2\u0404\u0408\5b\62\2\u0405\u0406" +
      "\7\u00ae\2\2\u0406\u0408\t\22\2\2\u0407\u0402\3\2\2\2\u0407\u0405\3\2" +
      "\2\2\u0408a\3\2\2\2\u0409\u040a\7\u00ae\2\2\u040a\u0411\7\u00ad\2\2\u040b" +
      "\u040c\7\u00ae\2\2\u040c\u0411\7\u00ac\2\2\u040d\u040e\7\u00ab\2\2\u040e" +
      "\u0411\7\u00ae\2\2\u040f\u0411\7\u00aa\2\2\u0410\u0409\3\2\2\2\u0410\u040b" +
      "\3\2\2\2\u0410\u040d\3\2\2\2\u0410\u040f\3\2\2\2\u0411c\3\2\2\2\u0412" +
      "\u0418\5:\36\2\u0413\u0414\5j\66\2\u0414\u0415\7\13\2\2\u0415\u0416\5" +
      ":\36\2\u0416\u0418\3\2\2\2\u0417\u0412\3\2\2\2\u0417\u0413\3\2\2\2\u0418" +
      "e\3\2\2\2\u0419\u041e\7\f\2\2\u041a\u041e\7l\2\2\u041b\u041e\7k\2\2\u041c" +
      "\u041e\5j\66\2\u041d\u0419\3\2\2\2\u041d\u041a\3\2\2\2\u041d\u041b\3\2" +
      "\2\2\u041d\u041c\3\2\2\2\u041eg\3\2\2\2\u041f\u0424\5j\66\2\u0420\u0421" +
      "\7\7\2\2\u0421\u0423\5j\66\2\u0422\u0420\3\2\2\2\u0423\u0426\3\2\2\2\u0424" +
      "\u0422\3\2\2\2\u0424\u0425\3\2\2\2\u0425i\3\2\2\2\u0426\u0424\3\2\2\2" +
      "\u0427\u042d\7\u00ce\2\2\u0428\u042d\5l\67\2\u0429\u042d\5p9\2\u042a\u042d" +
      "\7\u00d1\2\2\u042b\u042d\7\u00cf\2\2\u042c\u0427\3\2\2\2\u042c\u0428\3" +
      "\2\2\2\u042c\u0429\3\2\2\2\u042c\u042a\3\2\2\2\u042c\u042b\3\2\2\2\u042d" +
      "k\3\2\2\2\u042e\u042f\7\u00d0\2\2\u042fm\3\2\2\2\u0430\u0433\7\u00cd\2" +
      "\2\u0431\u0433\7\u00cc\2\2\u0432\u0430\3\2\2\2\u0432\u0431\3\2\2\2\u0433" +
      "o\3\2\2\2\u0434\u048a\7\u0081\2\2\u0435\u048a\7\u0082\2\2\u0436\u048a" +
      "\7\u0089\2\2\u0437\u048a\7\u008a\2\2\u0438\u048a\7\u008c\2\2\u0439\u048a" +
      "\7\u008d\2\2\u043a\u048a\7\u0087\2\2\u043b\u048a\7\u0088\2\2\u043c\u048a" +
      "\7\u00a1\2\2\u043d\u048a\7\16\2\2\u043e\u048a\7Y\2\2\u043f\u048a\7Z\2" +
      "\2\u0440\u048a\7[\2\2\u0441\u048a\7\\\2\2\u0442\u048a\7^\2\2\u0443\u048a" +
      "\7_\2\2\u0444\u048a\7`\2\2\u0445\u048a\7a\2\2\u0446\u048a\7\u009e\2\2" +
      "\u0447\u048a\7\u009d\2\2\u0448\u048a\7\66\2\2\u0449\u048a\7\67\2\2\u044a" +
      "\u048a\78\2\2\u044b\u048a\79\2\2\u044c\u048a\7:\2\2\u044d\u048a\7;\2\2" +
      "\u044e\u048a\7<\2\2\u044f\u048a\7C\2\2\u0450\u048a\7=\2\2\u0451\u048a" +
      "\7>\2\2\u0452\u048a\7?\2\2\u0453\u048a\7@\2\2\u0454\u048a\7A\2\2\u0455" +
      "\u048a\7B\2\2\u0456\u048a\7v\2\2\u0457\u048a\7w\2\2\u0458\u048a\7x\2\2" +
      "\u0459\u048a\7y\2\2\u045a\u048a\7z\2\2\u045b\u048a\7{\2\2\u045c\u048a" +
      "\7|\2\2\u045d\u048a\7}\2\2\u045e\u048a\7\u0096\2\2\u045f\u048a\7\u0093" +
      "\2\2\u0460\u048a\7\u0094\2\2\u0461\u048a\7\u0095\2\2\u0462\u048a\7\u008b" +
      "\2\2\u0463\u048a\7\u0092\2\2\u0464\u048a\7\u0097\2\2\u0465\u048a\7\36" +
      "\2\2\u0466\u048a\7\37\2\2\u0467\u048a\7 \2\2\u0468\u048a\7\u009f\2\2\u0469" +
      "\u048a\7\u00a0\2\2\u046a\u048a\7i\2\2\u046b\u048a\7j\2\2\u046c\u048a\7" +
      "\u00bb\2\2\u046d\u048a\7\u00bc\2\2\u046e\u048a\7\u00bd\2\2\u046f\u048a" +
      "\7~\2\2\u0470\u048a\5r:\2\u0471\u048a\7\64\2\2\u0472\u048a\7%\2\2\u0473" +
      "\u048a\7\u00a2\2\2\u0474\u048a\7\u00a3\2\2\u0475\u048a\7\u00a4\2\2\u0476" +
      "\u048a\7\u00a5\2\2\u0477\u048a\7\u00a6\2\2\u0478\u048a\7\u00a7\2\2\u0479" +
      "\u048a\7\u00a8\2\2\u047a\u048a\7\u00a9\2\2\u047b\u048a\7\u00aa\2\2\u047c" +
      "\u048a\7\u00ab\2\2\u047d\u048a\7\u00ac\2\2\u047e\u048a\7\u00ad\2\2\u047f" +
      "\u048a\7\u00ae\2\2\u0480\u048a\7\u00af\2\2\u0481\u048a\7\u00b0\2\2\u0482" +
      "\u048a\7\u00b1\2\2\u0483\u048a\7q\2\2\u0484\u048a\7r\2\2\u0485\u048a\7" +
      "s\2\2\u0486\u048a\7t\2\2\u0487\u048a\7u\2\2\u0488\u048a\7\63\2\2\u0489" +
      "\u0434\3\2\2\2\u0489\u0435\3\2\2\2\u0489\u0436\3\2\2\2\u0489\u0437\3\2" +
      "\2\2\u0489\u0438\3\2\2\2\u0489\u0439\3\2\2\2\u0489\u043a\3\2\2\2\u0489" +
      "\u043b\3\2\2\2\u0489\u043c\3\2\2\2\u0489\u043d\3\2\2\2\u0489\u043e\3\2" +
      "\2\2\u0489\u043f\3\2\2\2\u0489\u0440\3\2\2\2\u0489\u0441\3\2\2\2\u0489" +
      "\u0442\3\2\2\2\u0489\u0443\3\2\2\2\u0489\u0444\3\2\2\2\u0489\u0445\3\2" +
      "\2\2\u0489\u0446\3\2\2\2\u0489\u0447\3\2\2\2\u0489\u0448\3\2\2\2\u0489" +
      "\u0449\3\2\2\2\u0489\u044a\3\2\2\2\u0489\u044b\3\2\2\2\u0489\u044c\3\2" +
      "\2\2\u0489\u044d\3\2\2\2\u0489\u044e\3\2\2\2\u0489\u044f\3\2\2\2\u0489" +
      "\u0450\3\2\2\2\u0489\u0451\3\2\2\2\u0489\u0452\3\2\2\2\u0489\u0453\3\2" +
      "\2\2\u0489\u0454\3\2\2\2\u0489\u0455\3\2\2\2\u0489\u0456\3\2\2\2\u0489" +
      "\u0457\3\2\2\2\u0489\u0458\3\2\2\2\u0489\u0459\3\2\2\2\u0489\u045a\3\2" +
      "\2\2\u0489\u045b\3\2\2\2\u0489\u045c\3\2\2\2\u0489\u045d\3\2\2\2\u0489" +
      "\u045e\3\2\2\2\u0489\u045f\3\2\2\2\u0489\u0460\3\2\2\2\u0489\u0461\3\2" +
      "\2\2\u0489\u0462\3\2\2\2\u0489\u0463\3\2\2\2\u0489\u0464\3\2\2\2\u0489" +
      "\u0465\3\2\2\2\u0489\u0466\3\2\2\2\u0489\u0467\3\2\2\2\u0489\u0468\3\2" +
      "\2\2\u0489\u0469\3\2\2\2\u0489\u046a\3\2\2\2\u0489\u046b\3\2\2\2\u0489" +
      "\u046c\3\2\2\2\u0489\u046d\3\2\2\2\u0489\u046e\3\2\2\2\u0489\u046f\3\2" +
      "\2\2\u0489\u0470\3\2\2\2\u0489\u0471\3\2\2\2\u0489\u0472\3\2\2\2\u0489" +
      "\u0473\3\2\2\2\u0489\u0474\3\2\2\2\u0489\u0475\3\2\2\2\u0489\u0476\3\2" +
      "\2\2\u0489\u0477\3\2\2\2\u0489\u0478\3\2\2\2\u0489\u0479\3\2\2\2\u0489" +
      "\u047a\3\2\2\2\u0489\u047b\3\2\2\2\u0489\u047c\3\2\2\2\u0489\u047d\3\2" +
      "\2\2\u0489\u047e\3\2\2\2\u0489\u047f\3\2\2\2\u0489\u0480\3\2\2\2\u0489" +
      "\u0481\3\2\2\2\u0489\u0482\3\2\2\2\u0489\u0483\3\2\2\2\u0489\u0484\3\2" +
      "\2\2\u0489\u0485\3\2\2\2\u0489\u0486\3\2\2\2\u0489\u0487\3\2\2\2\u0489" +
      "\u0488\3\2\2\2\u048aq\3\2\2\2\u048b\u048c\t\23\2\2\u048cs\3\2\2\2\u0086" +
      "x\u0088\u008c\u0096\u00a8\u00b1\u00b7\u00bd\u00c0\u00c3\u00c9\u00d0\u00dc" +
      "\u00ed\u00f0\u00f4\u00fb\u0103\u0109\u010c\u010e\u011a\u0121\u0125\u0129" +
      "\u012d\u0134\u0139\u0141\u0144\u0148\u014d\u0151\u0154\u015b\u0166\u0169" +
      "\u0173\u0176\u0181\u0186\u018e\u0191\u0195\u019d\u01a0\u01a4\u01a8\u01b3" +
      "\u01b6\u01bd\u01d0\u01d4\u01d8\u01dc\u01e0\u01e4\u01e6\u01f1\u01f6\u01fc" +
      "\u0200\u0202\u020a\u021b\u0221\u0227\u022f\u0237\u0239\u023e\u0244\u024c" +
      "\u0255\u025b\u0263\u0269\u026d\u0272\u0277\u027d\u028b\u028d\u02a7\u02b2" +
      "\u02bc\u02c1\u02c8\u02cb\u02cf\u02db\u02f0\u02f4\u02fc\u0300\u0319\u031c" +
      "\u0325\u032b\u0331\u0337\u0340\u0349\u0358\u0362\u0364\u036d\u0375\u037b" +
      "\u0396\u03a2\u03a7\u03a9\u03af\u03b4\u03b9\u03c9\u03cc\u03d5\u03d8\u03db" +
      "\u03ef\u03fa\u0400\u0407\u0410\u0417\u041d\u0424\u042c\u0432\u0489";
  public static final ATN _ATN =
      new ATNDeserializer().deserialize(_serializedATN.toCharArray());

  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}