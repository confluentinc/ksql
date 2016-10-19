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
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		SELECT=10, FROM=11, ADD=12, AS=13, ALL=14, SOME=15, ANY=16, DISTINCT=17, 
		WHERE=18, GROUP=19, BY=20, GROUPING=21, SETS=22, CUBE=23, ROLLUP=24, ORDER=25, 
		HAVING=26, LIMIT=27, APPROXIMATE=28, AT=29, CONFIDENCE=30, OR=31, AND=32, 
		IN=33, NOT=34, NO=35, EXISTS=36, BETWEEN=37, LIKE=38, IS=39, NULL=40, 
		TRUE=41, FALSE=42, NULLS=43, FIRST=44, LAST=45, ESCAPE=46, ASC=47, DESC=48, 
		SUBSTRING=49, POSITION=50, FOR=51, TINYINT=52, SMALLINT=53, INTEGER=54, 
		DATE=55, TIME=56, TIMESTAMP=57, INTERVAL=58, YEAR=59, MONTH=60, DAY=61, 
		HOUR=62, MINUTE=63, SECOND=64, ZONE=65, CURRENT_DATE=66, CURRENT_TIME=67, 
		CURRENT_TIMESTAMP=68, LOCALTIME=69, LOCALTIMESTAMP=70, EXTRACT=71, CASE=72, 
		WHEN=73, THEN=74, ELSE=75, END=76, JOIN=77, CROSS=78, OUTER=79, INNER=80, 
		LEFT=81, RIGHT=82, FULL=83, NATURAL=84, USING=85, ON=86, OVER=87, PARTITION=88, 
		RANGE=89, ROWS=90, UNBOUNDED=91, PRECEDING=92, FOLLOWING=93, CURRENT=94, 
		ROW=95, WITH=96, RECURSIVE=97, VALUES=98, CREATE=99, TABLE=100, TOPIC=101, 
		STREAM=102, VIEW=103, REPLACE=104, INSERT=105, DELETE=106, INTO=107, CONSTRAINT=108, 
		DESCRIBE=109, PRINT=110, GRANT=111, REVOKE=112, PRIVILEGES=113, PUBLIC=114, 
		OPTION=115, EXPLAIN=116, ANALYZE=117, FORMAT=118, TYPE=119, TEXT=120, 
		GRAPHVIZ=121, LOGICAL=122, DISTRIBUTED=123, TRY=124, CAST=125, TRY_CAST=126, 
		SHOW=127, TABLES=128, TOPICS=129, QUERIES=130, TERMINATE=131, LOAD=132, 
		SCHEMAS=133, CATALOGS=134, COLUMNS=135, COLUMN=136, USE=137, PARTITIONS=138, 
		FUNCTIONS=139, DROP=140, UNION=141, EXCEPT=142, INTERSECT=143, TO=144, 
		SYSTEM=145, BERNOULLI=146, POISSONIZED=147, TABLESAMPLE=148, RESCALED=149, 
		STRATIFY=150, ALTER=151, RENAME=152, UNNEST=153, ORDINALITY=154, ARRAY=155, 
		MAP=156, SET=157, RESET=158, SESSION=159, DATA=160, START=161, TRANSACTION=162, 
		COMMIT=163, ROLLBACK=164, WORK=165, ISOLATION=166, LEVEL=167, SERIALIZABLE=168, 
		REPEATABLE=169, COMMITTED=170, UNCOMMITTED=171, READ=172, WRITE=173, ONLY=174, 
		CALL=175, PREPARE=176, DEALLOCATE=177, EXECUTE=178, NORMALIZE=179, NFD=180, 
		NFC=181, NFKD=182, NFKC=183, IF=184, NULLIF=185, COALESCE=186, EQ=187, 
		NEQ=188, LT=189, LTE=190, GT=191, GTE=192, PLUS=193, MINUS=194, ASTERISK=195, 
		SLASH=196, PERCENT=197, CONCAT=198, STRING=199, BINARY_LITERAL=200, INTEGER_VALUE=201, 
		DECIMAL_VALUE=202, IDENTIFIER=203, DIGIT_IDENTIFIER=204, QUOTED_IDENTIFIER=205, 
		BACKQUOTED_IDENTIFIER=206, TIME_WITH_TIME_ZONE=207, TIMESTAMP_WITH_TIME_ZONE=208, 
		SIMPLE_COMMENT=209, BRACKETED_COMMENT=210, WS=211, UNRECOGNIZED=212, DELIMITER=213;
	public static final int
		RULE_statements = 0, RULE_singleStatement = 1, RULE_singleExpression = 2, 
		RULE_statement = 3, RULE_query = 4, RULE_with = 5, RULE_tableElement = 6, 
		RULE_tableProperties = 7, RULE_tableProperty = 8, RULE_queryNoWith = 9, 
		RULE_queryTerm = 10, RULE_queryPrimary = 11, RULE_sortItem = 12, RULE_querySpecification = 13, 
		RULE_groupBy = 14, RULE_groupingElement = 15, RULE_groupingExpressions = 16, 
		RULE_groupingSet = 17, RULE_namedQuery = 18, RULE_setQuantifier = 19, 
		RULE_selectItem = 20, RULE_relation = 21, RULE_joinType = 22, RULE_joinCriteria = 23, 
		RULE_sampledRelation = 24, RULE_sampleType = 25, RULE_aliasedRelation = 26, 
		RULE_columnAliases = 27, RULE_relationPrimary = 28, RULE_expression = 29, 
		RULE_booleanExpression = 30, RULE_predicated = 31, RULE_predicate = 32, 
		RULE_valueExpression = 33, RULE_primaryExpression = 34, RULE_timeZoneSpecifier = 35, 
		RULE_comparisonOperator = 36, RULE_booleanValue = 37, RULE_interval = 38, 
		RULE_intervalField = 39, RULE_type = 40, RULE_typeParameter = 41, RULE_baseType = 42, 
		RULE_whenClause = 43, RULE_over = 44, RULE_windowFrame = 45, RULE_frameBound = 46, 
		RULE_explainOption = 47, RULE_transactionMode = 48, RULE_levelOfIsolation = 49, 
		RULE_callArgument = 50, RULE_privilege = 51, RULE_qualifiedName = 52, 
		RULE_identifier = 53, RULE_quotedIdentifier = 54, RULE_number = 55, RULE_nonReserved = 56, 
		RULE_normalForm = 57;
	public static final String[] ruleNames = {
		"statements", "singleStatement", "singleExpression", "statement", "query", 
		"with", "tableElement", "tableProperties", "tableProperty", "queryNoWith", 
		"queryTerm", "queryPrimary", "sortItem", "querySpecification", "groupBy", 
		"groupingElement", "groupingExpressions", "groupingSet", "namedQuery", 
		"setQuantifier", "selectItem", "relation", "joinType", "joinCriteria", 
		"sampledRelation", "sampleType", "aliasedRelation", "columnAliases", "relationPrimary", 
		"expression", "booleanExpression", "predicated", "predicate", "valueExpression", 
		"primaryExpression", "timeZoneSpecifier", "comparisonOperator", "booleanValue", 
		"interval", "intervalField", "type", "typeParameter", "baseType", "whenClause", 
		"over", "windowFrame", "frameBound", "explainOption", "transactionMode", 
		"levelOfIsolation", "callArgument", "privilege", "qualifiedName", "identifier", 
		"quotedIdentifier", "number", "nonReserved", "normalForm"
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
		"'CALL'", "'PREPARE'", "'DEALLOCATE'", "'EXECUTE'", "'NORMALIZE'", "'NFD'", 
		"'NFC'", "'NFKD'", "'NFKC'", "'IF'", "'NULLIF'", "'COALESCE'", "'='", 
		null, "'<'", "'<='", "'>'", "'>='", "'+'", "'-'", "'*'", "'/'", "'%'", 
		"'||'"
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
		"DEALLOCATE", "EXECUTE", "NORMALIZE", "NFD", "NFC", "NFKD", "NFKC", "IF", 
		"NULLIF", "COALESCE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
		"ASTERISK", "SLASH", "PERCENT", "CONCAT", "STRING", "BINARY_LITERAL", 
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
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StatementsContext extends ParserRuleContext {
		public List<SingleStatementContext> singleStatement() {
			return getRuleContexts(SingleStatementContext.class);
		}
		public SingleStatementContext singleStatement(int i) {
			return getRuleContext(SingleStatementContext.class,i);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public StatementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatements(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatements(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementsContext statements() throws RecognitionException {
		StatementsContext _localctx = new StatementsContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_statements);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(116);
			singleStatement();
			setState(120);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1 || _la==SELECT || ((((_la - 96)) & ~0x3f) == 0 && ((1L << (_la - 96)) & ((1L << (WITH - 96)) | (1L << (VALUES - 96)) | (1L << (CREATE - 96)) | (1L << (TABLE - 96)) | (1L << (DESCRIBE - 96)) | (1L << (PRINT - 96)) | (1L << (SHOW - 96)) | (1L << (TERMINATE - 96)) | (1L << (LOAD - 96)) | (1L << (DROP - 96)) | (1L << (SET - 96)))) != 0)) {
				{
				{
				setState(117);
				singleStatement();
				}
				}
				setState(122);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(123);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(125);
			statement();
			setState(126);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(128);
			expression();
			setState(129);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TOPIC() { return getToken(SqlBaseParser.TOPIC, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<TableElementContext> tableElement() {
			return getRuleContexts(TableElementContext.class);
		}
		public TableElementContext tableElement(int i) {
			return getRuleContext(TableElementContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TablePropertiesContext tableProperties() {
			return getRuleContext(TablePropertiesContext.class,0);
		}
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QuerystatementContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public QuerystatementContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerystatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerystatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerystatement(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowQueriesContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode QUERIES() { return getToken(SqlBaseParser.QUERIES, 0); }
		public ShowQueriesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowQueries(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowQueries(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowQueries(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetPropertyContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetPropertyContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetProperty(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TerminateQueryContext extends StatementContext {
		public TerminalNode TERMINATE() { return getToken(SqlBaseParser.TERMINATE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminateQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTerminateQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTerminateQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTerminateQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTopicsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TOPICS() { return getToken(SqlBaseParser.TOPICS, 0); }
		public ShowTopicsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTopics(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTopics(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTopics(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PrintTopicContext extends StatementContext {
		public TerminalNode PRINT() { return getToken(SqlBaseParser.PRINT, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public PrintTopicContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrintTopic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrintTopic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrintTopic(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LoadPropertiesContext extends StatementContext {
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LoadPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoadProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoadProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLoadProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TOPIC() { return getToken(SqlBaseParser.TOPIC, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_statement);
		int _la;
		try {
			setState(188);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				_localctx = new QuerystatementContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(131);
				query();
				}
				break;
			case 2:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(132);
				match(SHOW);
				setState(133);
				match(TABLES);
				setState(136);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(134);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(135);
					qualifiedName();
					}
				}

				setState(140);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(138);
					match(LIKE);
					setState(139);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 3:
				_localctx = new ShowTopicsContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(142);
				match(SHOW);
				setState(143);
				match(TOPICS);
				}
				break;
			case 4:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(144);
				match(DESCRIBE);
				setState(145);
				qualifiedName();
				}
				break;
			case 5:
				_localctx = new PrintTopicContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(146);
				match(PRINT);
				setState(147);
				qualifiedName();
				}
				break;
			case 6:
				_localctx = new ShowQueriesContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(148);
				match(SHOW);
				setState(149);
				match(QUERIES);
				}
				break;
			case 7:
				_localctx = new TerminateQueryContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(150);
				match(TERMINATE);
				setState(151);
				qualifiedName();
				}
				break;
			case 8:
				_localctx = new SetPropertyContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(152);
				match(SET);
				setState(153);
				qualifiedName();
				setState(154);
				match(EQ);
				setState(155);
				expression();
				}
				break;
			case 9:
				_localctx = new LoadPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(157);
				match(LOAD);
				setState(158);
				expression();
				}
				break;
			case 10:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(159);
				match(CREATE);
				setState(160);
				match(TOPIC);
				setState(164);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
				case 1:
					{
					setState(161);
					match(IF);
					setState(162);
					match(NOT);
					setState(163);
					match(EXISTS);
					}
					break;
				}
				setState(166);
				qualifiedName();
				setState(167);
				match(T__1);
				setState(168);
				tableElement();
				setState(173);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(169);
					match(T__2);
					setState(170);
					tableElement();
					}
					}
					setState(175);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(176);
				match(T__3);
				setState(179);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(177);
					match(WITH);
					setState(178);
					tableProperties();
					}
				}

				}
				break;
			case 11:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(181);
				match(DROP);
				setState(182);
				match(TOPIC);
				setState(185);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(183);
					match(IF);
					setState(184);
					match(EXISTS);
					}
					break;
				}
				setState(187);
				qualifiedName();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public WithContext with() {
			return getRuleContext(WithContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(191);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(190);
				with();
				}
			}

			setState(193);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public TerminalNode RECURSIVE() { return getToken(SqlBaseParser.RECURSIVE, 0); }
		public WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WithContext with() throws RecognitionException {
		WithContext _localctx = new WithContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_with);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(195);
			match(WITH);
			setState(197);
			_la = _input.LA(1);
			if (_la==RECURSIVE) {
				{
				setState(196);
				match(RECURSIVE);
				}
			}

			setState(199);
			namedQuery();
			setState(204);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(200);
				match(T__2);
				setState(201);
				namedQuery();
				}
				}
				setState(206);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableElementContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TableElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableElementContext tableElement() throws RecognitionException {
		TableElementContext _localctx = new TableElementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_tableElement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			identifier();
			setState(208);
			type(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertiesContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertiesContext tableProperties() throws RecognitionException {
		TablePropertiesContext _localctx = new TablePropertiesContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_tableProperties);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210);
			match(T__1);
			setState(211);
			tableProperty();
			setState(216);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(212);
				match(T__2);
				setState(213);
				tableProperty();
				}
				}
				setState(218);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(219);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_tableProperty);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(221);
			identifier();
			setState(222);
			match(EQ);
			setState(223);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryNoWithContext extends ParserRuleContext {
		public Token limit;
		public NumberContext confidence;
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode APPROXIMATE() { return getToken(SqlBaseParser.APPROXIMATE, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode CONFIDENCE() { return getToken(SqlBaseParser.CONFIDENCE, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryNoWith(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryNoWith(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryNoWith(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_queryNoWith);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(225);
			queryTerm(0);
			setState(236);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(226);
				match(ORDER);
				setState(227);
				match(BY);
				setState(228);
				sortItem();
				setState(233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(229);
					match(T__2);
					setState(230);
					sortItem();
					}
					}
					setState(235);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(240);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(238);
				match(LIMIT);
				setState(239);
				((QueryNoWithContext)_localctx).limit = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ALL || _la==INTEGER_VALUE) ) {
					((QueryNoWithContext)_localctx).limit = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(247);
			_la = _input.LA(1);
			if (_la==APPROXIMATE) {
				{
				setState(242);
				match(APPROXIMATE);
				setState(243);
				match(AT);
				setState(244);
				((QueryNoWithContext)_localctx).confidence = number();
				setState(245);
				match(CONFIDENCE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
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
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
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

			setState(250);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(266);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(264);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
					case 1:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(252);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(253);
						((SetOperationContext)_localctx).operator = match(INTERSECT);
						setState(255);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(254);
							setQuantifier();
							}
						}

						setState(257);
						((SetOperationContext)_localctx).right = queryTerm(3);
						}
						break;
					case 2:
						{
						_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
						((SetOperationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
						setState(258);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(259);
						((SetOperationContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==UNION || _la==EXCEPT) ) {
							((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(261);
						_la = _input.LA(1);
						if (_la==ALL || _la==DISTINCT) {
							{
							setState(260);
							setQuantifier();
							}
						}

						setState(263);
						((SetOperationContext)_localctx).right = queryTerm(2);
						}
						break;
					}
					} 
				}
				setState(268);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableContext extends QueryPrimaryContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public InlineTableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_queryPrimary);
		try {
			int _alt;
			setState(285);
			switch (_input.LA(1)) {
			case SELECT:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(269);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(270);
				match(TABLE);
				setState(271);
				qualifiedName();
				}
				break;
			case VALUES:
				_localctx = new InlineTableContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(272);
				match(VALUES);
				setState(273);
				expression();
				setState(278);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(274);
						match(T__2);
						setState(275);
						expression();
						}
						} 
					}
					setState(280);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,20,_ctx);
				}
				}
				break;
			case T__1:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(281);
				match(T__1);
				setState(282);
				queryNoWith();
				setState(283);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrdering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287);
			expression();
			setState(289);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(288);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(293);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(291);
				match(NULLS);
				setState(292);
				((SortItemContext)_localctx).nullOrdering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public RelationContext into;
		public RelationContext from;
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public GroupByContext groupBy() {
			return getRuleContext(GroupByContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
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
			setState(295);
			match(SELECT);
			setState(297);
			_la = _input.LA(1);
			if (_la==ALL || _la==DISTINCT) {
				{
				setState(296);
				setQuantifier();
				}
			}

			setState(299);
			selectItem();
			setState(304);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,25,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(300);
					match(T__2);
					setState(301);
					selectItem();
					}
					} 
				}
				setState(306);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,25,_ctx);
			}
			setState(309);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				{
				setState(307);
				match(INTO);
				setState(308);
				((QuerySpecificationContext)_localctx).into = relation();
				}
				break;
			}
			setState(320);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
			case 1:
				{
				setState(311);
				match(FROM);
				setState(312);
				((QuerySpecificationContext)_localctx).from = relation();
				setState(317);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,27,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(313);
						match(T__2);
						setState(314);
						relation();
						}
						} 
					}
					setState(319);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,27,_ctx);
				}
				}
				break;
			}
			setState(324);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,29,_ctx) ) {
			case 1:
				{
				setState(322);
				match(WHERE);
				setState(323);
				((QuerySpecificationContext)_localctx).where = booleanExpression(0);
				}
				break;
			}
			setState(329);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
			case 1:
				{
				setState(326);
				match(GROUP);
				setState(327);
				match(BY);
				setState(328);
				groupBy();
				}
				break;
			}
			setState(333);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
			case 1:
				{
				setState(331);
				match(HAVING);
				setState(332);
				((QuerySpecificationContext)_localctx).having = booleanExpression(0);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupByContext extends ParserRuleContext {
		public List<GroupingElementContext> groupingElement() {
			return getRuleContexts(GroupingElementContext.class);
		}
		public GroupingElementContext groupingElement(int i) {
			return getRuleContext(GroupingElementContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public GroupByContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupBy; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupBy(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupBy(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupBy(this);
			else return visitor.visitChildren(this);
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
			setState(336);
			_la = _input.LA(1);
			if (_la==ALL || _la==DISTINCT) {
				{
				setState(335);
				setQuantifier();
				}
			}

			setState(338);
			groupingElement();
			setState(343);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(339);
					match(T__2);
					setState(340);
					groupingElement();
					}
					} 
				}
				setState(345);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,33,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingElementContext extends ParserRuleContext {
		public GroupingElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingElement; }
	 
		public GroupingElementContext() { }
		public void copyFrom(GroupingElementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class MultipleGroupingSetsContext extends GroupingElementContext {
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public MultipleGroupingSetsContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultipleGroupingSets(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultipleGroupingSets(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultipleGroupingSets(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SingleGroupingSetContext extends GroupingElementContext {
		public GroupingExpressionsContext groupingExpressions() {
			return getRuleContext(GroupingExpressionsContext.class,0);
		}
		public SingleGroupingSetContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CubeContext extends GroupingElementContext {
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public CubeContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCube(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCube(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCube(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RollupContext extends GroupingElementContext {
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public RollupContext(GroupingElementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRollup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRollup(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRollup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingElementContext groupingElement() throws RecognitionException {
		GroupingElementContext _localctx = new GroupingElementContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_groupingElement);
		int _la;
		try {
			setState(386);
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
				setState(346);
				groupingExpressions();
				}
				break;
			case ROLLUP:
				_localctx = new RollupContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(347);
				match(ROLLUP);
				setState(348);
				match(T__1);
				setState(357);
				_la = _input.LA(1);
				if (((((_la - 12)) & ~0x3f) == 0 && ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT - 12)) | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME - 12)) | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L << (YEAR - 12)) | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR - 12)) | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE - 12)))) != 0) || ((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L << (RANGE - 87)) | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87)) | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L << (REPLACE - 87)) | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L << (PRIVILEGES - 87)) | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87)) | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L << (TEXT - 87)) | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87)) | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87)) | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L << (SCHEMAS - 87)) | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87)) | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L << (PARTITIONS - 87)) | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L << (SYSTEM - 87)) | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED - 87)) | (1L << (TABLESAMPLE - 87)) | (1L << (RESCALED - 87)))) != 0) || ((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET - 155)) | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L << (DATA - 155)) | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L << (WORK - 155)) | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L << (SERIALIZABLE - 155)) | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155)) | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L << (WRITE - 155)) | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC - 155)) | (1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER - 155)) | (1L << (QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
					{
					setState(349);
					qualifiedName();
					setState(354);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(350);
						match(T__2);
						setState(351);
						qualifiedName();
						}
						}
						setState(356);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(359);
				match(T__3);
				}
				break;
			case CUBE:
				_localctx = new CubeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(360);
				match(CUBE);
				setState(361);
				match(T__1);
				setState(370);
				_la = _input.LA(1);
				if (((((_la - 12)) & ~0x3f) == 0 && ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT - 12)) | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME - 12)) | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L << (YEAR - 12)) | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR - 12)) | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE - 12)))) != 0) || ((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L << (RANGE - 87)) | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87)) | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L << (REPLACE - 87)) | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L << (PRIVILEGES - 87)) | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87)) | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L << (TEXT - 87)) | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87)) | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87)) | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L << (SCHEMAS - 87)) | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87)) | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L << (PARTITIONS - 87)) | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L << (SYSTEM - 87)) | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED - 87)) | (1L << (TABLESAMPLE - 87)) | (1L << (RESCALED - 87)))) != 0) || ((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET - 155)) | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L << (DATA - 155)) | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L << (WORK - 155)) | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L << (SERIALIZABLE - 155)) | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155)) | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L << (WRITE - 155)) | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC - 155)) | (1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER - 155)) | (1L << (QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
					{
					setState(362);
					qualifiedName();
					setState(367);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(363);
						match(T__2);
						setState(364);
						qualifiedName();
						}
						}
						setState(369);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(372);
				match(T__3);
				}
				break;
			case GROUPING:
				_localctx = new MultipleGroupingSetsContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(373);
				match(GROUPING);
				setState(374);
				match(SETS);
				setState(375);
				match(T__1);
				setState(376);
				groupingSet();
				setState(381);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(377);
					match(T__2);
					setState(378);
					groupingSet();
					}
					}
					setState(383);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(384);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingExpressionsContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingExpressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingExpressions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingExpressionsContext groupingExpressions() throws RecognitionException {
		GroupingExpressionsContext _localctx = new GroupingExpressionsContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_groupingExpressions);
		int _la;
		try {
			setState(401);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(388);
				match(T__1);
				setState(397);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << APPROXIMATE) | (1L << AT) | (1L << CONFIDENCE) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << SUBSTRING) | (1L << POSITION) | (1L << TINYINT) | (1L << SMALLINT) | (1L << INTEGER) | (1L << DATE) | (1L << TIME) | (1L << TIMESTAMP) | (1L << INTERVAL) | (1L << YEAR) | (1L << MONTH) | (1L << DAY) | (1L << HOUR) | (1L << MINUTE))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L << (CURRENT_DATE - 64)) | (1L << (CURRENT_TIME - 64)) | (1L << (CURRENT_TIMESTAMP - 64)) | (1L << (LOCALTIME - 64)) | (1L << (LOCALTIMESTAMP - 64)) | (1L << (EXTRACT - 64)) | (1L << (CASE - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (GRANT - 64)) | (1L << (REVOKE - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (PUBLIC - 64)) | (1L << (OPTION - 64)) | (1L << (EXPLAIN - 64)) | (1L << (ANALYZE - 64)) | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (1L << (TEXT - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (LOGICAL - 64)) | (1L << (DISTRIBUTED - 64)) | (1L << (TRY - 64)) | (1L << (CAST - 64)) | (1L << (TRY_CAST - 64)) | (1L << (SHOW - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L << (CATALOGS - 128)) | (1L << (COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L << (USE - 128)) | (1L << (PARTITIONS - 128)) | (1L << (FUNCTIONS - 128)) | (1L << (TO - 128)) | (1L << (SYSTEM - 128)) | (1L << (BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET - 128)) | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L << (DATA - 128)) | (1L << (START - 128)) | (1L << (TRANSACTION - 128)) | (1L << (COMMIT - 128)) | (1L << (ROLLBACK - 128)) | (1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (1L << (LEVEL - 128)) | (1L << (SERIALIZABLE - 128)) | (1L << (REPEATABLE - 128)) | (1L << (COMMITTED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (1L << (WRITE - 128)) | (1L << (ONLY - 128)) | (1L << (CALL - 128)) | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L << (NFC - 128)) | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (IF - 128)) | (1L << (NULLIF - 128)) | (1L << (COALESCE - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PLUS - 193)) | (1L << (MINUS - 193)) | (1L << (STRING - 193)) | (1L << (BINARY_LITERAL - 193)) | (1L << (INTEGER_VALUE - 193)) | (1L << (DECIMAL_VALUE - 193)) | (1L << (IDENTIFIER - 193)) | (1L << (DIGIT_IDENTIFIER - 193)) | (1L << (QUOTED_IDENTIFIER - 193)) | (1L << (BACKQUOTED_IDENTIFIER - 193)))) != 0)) {
					{
					setState(389);
					expression();
					setState(394);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(390);
						match(T__2);
						setState(391);
						expression();
						}
						}
						setState(396);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(399);
				match(T__3);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(400);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<QualifiedNameContext> qualifiedName() {
			return getRuleContexts(QualifiedNameContext.class);
		}
		public QualifiedNameContext qualifiedName(int i) {
			return getRuleContext(QualifiedNameContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_groupingSet);
		int _la;
		try {
			setState(416);
			switch (_input.LA(1)) {
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(403);
				match(T__1);
				setState(412);
				_la = _input.LA(1);
				if (((((_la - 12)) & ~0x3f) == 0 && ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (APPROXIMATE - 12)) | (1L << (AT - 12)) | (1L << (CONFIDENCE - 12)) | (1L << (NO - 12)) | (1L << (SUBSTRING - 12)) | (1L << (POSITION - 12)) | (1L << (TINYINT - 12)) | (1L << (SMALLINT - 12)) | (1L << (INTEGER - 12)) | (1L << (DATE - 12)) | (1L << (TIME - 12)) | (1L << (TIMESTAMP - 12)) | (1L << (INTERVAL - 12)) | (1L << (YEAR - 12)) | (1L << (MONTH - 12)) | (1L << (DAY - 12)) | (1L << (HOUR - 12)) | (1L << (MINUTE - 12)) | (1L << (SECOND - 12)) | (1L << (ZONE - 12)))) != 0) || ((((_la - 87)) & ~0x3f) == 0 && ((1L << (_la - 87)) & ((1L << (OVER - 87)) | (1L << (PARTITION - 87)) | (1L << (RANGE - 87)) | (1L << (ROWS - 87)) | (1L << (PRECEDING - 87)) | (1L << (FOLLOWING - 87)) | (1L << (CURRENT - 87)) | (1L << (ROW - 87)) | (1L << (VIEW - 87)) | (1L << (REPLACE - 87)) | (1L << (GRANT - 87)) | (1L << (REVOKE - 87)) | (1L << (PRIVILEGES - 87)) | (1L << (PUBLIC - 87)) | (1L << (OPTION - 87)) | (1L << (EXPLAIN - 87)) | (1L << (ANALYZE - 87)) | (1L << (FORMAT - 87)) | (1L << (TYPE - 87)) | (1L << (TEXT - 87)) | (1L << (GRAPHVIZ - 87)) | (1L << (LOGICAL - 87)) | (1L << (DISTRIBUTED - 87)) | (1L << (TRY - 87)) | (1L << (SHOW - 87)) | (1L << (TABLES - 87)) | (1L << (SCHEMAS - 87)) | (1L << (CATALOGS - 87)) | (1L << (COLUMNS - 87)) | (1L << (COLUMN - 87)) | (1L << (USE - 87)) | (1L << (PARTITIONS - 87)) | (1L << (FUNCTIONS - 87)) | (1L << (TO - 87)) | (1L << (SYSTEM - 87)) | (1L << (BERNOULLI - 87)) | (1L << (POISSONIZED - 87)) | (1L << (TABLESAMPLE - 87)) | (1L << (RESCALED - 87)))) != 0) || ((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & ((1L << (ARRAY - 155)) | (1L << (MAP - 155)) | (1L << (SET - 155)) | (1L << (RESET - 155)) | (1L << (SESSION - 155)) | (1L << (DATA - 155)) | (1L << (START - 155)) | (1L << (TRANSACTION - 155)) | (1L << (COMMIT - 155)) | (1L << (ROLLBACK - 155)) | (1L << (WORK - 155)) | (1L << (ISOLATION - 155)) | (1L << (LEVEL - 155)) | (1L << (SERIALIZABLE - 155)) | (1L << (REPEATABLE - 155)) | (1L << (COMMITTED - 155)) | (1L << (UNCOMMITTED - 155)) | (1L << (READ - 155)) | (1L << (WRITE - 155)) | (1L << (ONLY - 155)) | (1L << (CALL - 155)) | (1L << (NFD - 155)) | (1L << (NFC - 155)) | (1L << (NFKD - 155)) | (1L << (NFKC - 155)) | (1L << (IF - 155)) | (1L << (NULLIF - 155)) | (1L << (COALESCE - 155)) | (1L << (IDENTIFIER - 155)) | (1L << (DIGIT_IDENTIFIER - 155)) | (1L << (QUOTED_IDENTIFIER - 155)) | (1L << (BACKQUOTED_IDENTIFIER - 155)))) != 0)) {
					{
					setState(404);
					qualifiedName();
					setState(409);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(405);
						match(T__2);
						setState(406);
						qualifiedName();
						}
						}
						setState(411);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(414);
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
				setState(415);
				qualifiedName();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(418);
			((NamedQueryContext)_localctx).name = identifier();
			setState(420);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(419);
				columnAliases();
				}
			}

			setState(422);
			match(AS);
			setState(423);
			match(T__1);
			setState(424);
			query();
			setState(425);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(427);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectItemContext extends ParserRuleContext {
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
	 
		public SelectItemContext() { }
		public void copyFrom(SelectItemContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SelectAllContext extends SelectItemContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public SelectAllContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectAll(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SelectSingleContext extends SelectItemContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public SelectSingleContext(SelectItemContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSelectSingle(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSelectSingle(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSelectSingle(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_selectItem);
		int _la;
		try {
			setState(441);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				_localctx = new SelectSingleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(429);
				expression();
				setState(434);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
				case 1:
					{
					setState(431);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(430);
						match(AS);
						}
					}

					setState(433);
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
				setState(436);
				qualifiedName();
				setState(437);
				match(T__4);
				setState(438);
				match(ASTERISK);
				}
				break;
			case 3:
				_localctx = new SelectAllContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(440);
				match(ASTERISK);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_relation);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(443);
			relationPrimary();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_joinType);
		int _la;
		try {
			setState(460);
			switch (_input.LA(1)) {
			case EOF:
			case INNER:
				enterOuterAlt(_localctx, 1);
				{
				setState(446);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(445);
					match(INNER);
					}
				}

				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(448);
				match(LEFT);
				setState(450);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(449);
					match(OUTER);
					}
				}

				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(452);
				match(RIGHT);
				setState(454);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(453);
					match(OUTER);
					}
				}

				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(456);
				match(FULL);
				setState(458);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(457);
					match(OUTER);
					}
				}

				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_joinCriteria);
		int _la;
		try {
			setState(476);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(462);
				match(ON);
				setState(463);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(464);
				match(USING);
				setState(465);
				match(T__1);
				setState(466);
				identifier();
				setState(471);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(467);
					match(T__2);
					setState(468);
					identifier();
					}
					}
					setState(473);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(474);
				match(T__3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampledRelationContext extends ParserRuleContext {
		public ExpressionContext percentage;
		public ExpressionContext expression;
		public List<ExpressionContext> stratify = new ArrayList<ExpressionContext>();
		public AliasedRelationContext aliasedRelation() {
			return getRuleContext(AliasedRelationContext.class,0);
		}
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public SampleTypeContext sampleType() {
			return getRuleContext(SampleTypeContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode RESCALED() { return getToken(SqlBaseParser.RESCALED, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public SampledRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampledRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampledRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampledRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampledRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampledRelationContext sampledRelation() throws RecognitionException {
		SampledRelationContext _localctx = new SampledRelationContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_sampledRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(478);
			aliasedRelation();
			setState(502);
			_la = _input.LA(1);
			if (_la==TABLESAMPLE) {
				{
				setState(479);
				match(TABLESAMPLE);
				setState(480);
				sampleType();
				setState(481);
				match(T__1);
				setState(482);
				((SampledRelationContext)_localctx).percentage = expression();
				setState(483);
				match(T__3);
				setState(485);
				_la = _input.LA(1);
				if (_la==RESCALED) {
					{
					setState(484);
					match(RESCALED);
					}
				}

				setState(500);
				_la = _input.LA(1);
				if (_la==STRATIFY) {
					{
					setState(487);
					match(STRATIFY);
					setState(488);
					match(ON);
					setState(489);
					match(T__1);
					setState(490);
					((SampledRelationContext)_localctx).expression = expression();
					((SampledRelationContext)_localctx).stratify.add(((SampledRelationContext)_localctx).expression);
					setState(495);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(491);
						match(T__2);
						setState(492);
						((SampledRelationContext)_localctx).expression = expression();
						((SampledRelationContext)_localctx).stratify.add(((SampledRelationContext)_localctx).expression);
						}
						}
						setState(497);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(498);
					match(T__3);
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleTypeContext extends ParserRuleContext {
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode POISSONIZED() { return getToken(SqlBaseParser.POISSONIZED, 0); }
		public SampleTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sampleType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSampleType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSampleType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSampleType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleTypeContext sampleType() throws RecognitionException {
		SampleTypeContext _localctx = new SampleTypeContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_sampleType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
			_la = _input.LA(1);
			if ( !(((((_la - 145)) & ~0x3f) == 0 && ((1L << (_la - 145)) & ((1L << (SYSTEM - 145)) | (1L << (BERNOULLI - 145)) | (1L << (POISSONIZED - 145)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AliasedRelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public ColumnAliasesContext columnAliases() {
			return getRuleContext(ColumnAliasesContext.class,0);
		}
		public AliasedRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasedRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasedRelationContext aliasedRelation() throws RecognitionException {
		AliasedRelationContext _localctx = new AliasedRelationContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_aliasedRelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(506);
			relationPrimary();
			setState(514);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,63,_ctx) ) {
			case 1:
				{
				setState(508);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(507);
					match(AS);
					}
				}

				setState(510);
				identifier();
				setState(512);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(511);
					columnAliases();
					}
				}

				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnAliasesContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ColumnAliasesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnAliases; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnAliases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnAliases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnAliases(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnAliasesContext columnAliases() throws RecognitionException {
		ColumnAliasesContext _localctx = new ColumnAliasesContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_columnAliases);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(516);
			match(T__1);
			setState(517);
			identifier();
			setState(522);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(518);
				match(T__2);
				setState(519);
				identifier();
				}
				}
				setState(524);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(525);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryRelationContext extends RelationPrimaryContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public ParenthesizedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnnestContext extends RelationPrimaryContext {
		public TerminalNode UNNEST() { return getToken(SqlBaseParser.UNNEST, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode ORDINALITY() { return getToken(SqlBaseParser.ORDINALITY, 0); }
		public UnnestContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnnest(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnnest(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnnest(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_relationPrimary);
		int _la;
		try {
			setState(551);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(527);
				qualifiedName();
				}
				break;
			case 2:
				_localctx = new SubqueryRelationContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(528);
				match(T__1);
				setState(529);
				query();
				setState(530);
				match(T__3);
				}
				break;
			case 3:
				_localctx = new UnnestContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(532);
				match(UNNEST);
				setState(533);
				match(T__1);
				setState(534);
				expression();
				setState(539);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(535);
					match(T__2);
					setState(536);
					expression();
					}
					}
					setState(541);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(542);
				match(T__3);
				setState(545);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
				case 1:
					{
					setState(543);
					match(WITH);
					setState(544);
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
				setState(547);
				match(T__1);
				setState(548);
				relation();
				setState(549);
				match(T__3);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(553);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanDefaultContext extends BooleanExpressionContext {
		public PredicatedContext predicated() {
			return getRuleContext(PredicatedContext.class,0);
		}
		public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
			else return visitor.visitChildren(this);
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
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
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
		int _startState = 60;
		enterRecursionRule(_localctx, 60, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(559);
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
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new BooleanDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(556);
				predicated();
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(557);
				match(NOT);
				setState(558);
				booleanExpression(3);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(569);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,70,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(567);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,69,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(561);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(562);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(563);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(564);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(565);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(566);
						((LogicalBinaryContext)_localctx).right = booleanExpression(2);
						}
						break;
					}
					} 
				}
				setState(571);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,70,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicatedContext extends ParserRuleContext {
		public ValueExpressionContext valueExpression;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicated; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicatedContext predicated() throws RecognitionException {
		PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_predicated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(572);
			((PredicatedContext)_localctx).valueExpression = valueExpression(0);
			setState(574);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
			case 1:
				{
				setState(573);
				predicate(((PredicatedContext)_localctx).valueExpression);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public ParserRuleContext value;
		public PredicateContext(ParserRuleContext parent, int invokingState) { super(parent, invokingState); }
		public PredicateContext(ParserRuleContext parent, int invokingState, ParserRuleContext value) {
			super(parent, invokingState);
			this.value = value;
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
	 
		public PredicateContext() { }
		public void copyFrom(PredicateContext ctx) {
			super.copyFrom(ctx);
			this.value = ctx.value;
		}
	}
	public static class ComparisonContext extends PredicateContext {
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ComparisonContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LikeContext extends PredicateContext {
		public ValueExpressionContext pattern;
		public ValueExpressionContext escape;
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode ESCAPE() { return getToken(SqlBaseParser.ESCAPE, 0); }
		public LikeContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InSubqueryContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InSubqueryContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DistinctFromContext extends PredicateContext {
		public ValueExpressionContext right;
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public DistinctFromContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDistinctFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDistinctFrom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDistinctFrom(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InListContext extends PredicateContext {
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public InListContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInList(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullPredicateContext extends PredicateContext {
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NullPredicateContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BetweenContext extends PredicateContext {
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BetweenContext(PredicateContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBetween(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate(ParserRuleContext value) throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState(), value);
		enterRule(_localctx, 64, RULE_predicate);
		int _la;
		try {
			setState(631);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
			case 1:
				_localctx = new ComparisonContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(576);
				comparisonOperator();
				setState(577);
				((ComparisonContext)_localctx).right = valueExpression(0);
				}
				break;
			case 2:
				_localctx = new BetweenContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(580);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(579);
					match(NOT);
					}
				}

				setState(582);
				match(BETWEEN);
				setState(583);
				((BetweenContext)_localctx).lower = valueExpression(0);
				setState(584);
				match(AND);
				setState(585);
				((BetweenContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 3:
				_localctx = new InListContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(588);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(587);
					match(NOT);
					}
				}

				setState(590);
				match(IN);
				setState(591);
				match(T__1);
				setState(592);
				expression();
				setState(597);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(593);
					match(T__2);
					setState(594);
					expression();
					}
					}
					setState(599);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(600);
				match(T__3);
				}
				break;
			case 4:
				_localctx = new InSubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(603);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(602);
					match(NOT);
					}
				}

				setState(605);
				match(IN);
				setState(606);
				match(T__1);
				setState(607);
				query();
				setState(608);
				match(T__3);
				}
				break;
			case 5:
				_localctx = new LikeContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(611);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(610);
					match(NOT);
					}
				}

				setState(613);
				match(LIKE);
				setState(614);
				((LikeContext)_localctx).pattern = valueExpression(0);
				setState(617);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(615);
					match(ESCAPE);
					setState(616);
					((LikeContext)_localctx).escape = valueExpression(0);
					}
					break;
				}
				}
				break;
			case 6:
				_localctx = new NullPredicateContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(619);
				match(IS);
				setState(621);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(620);
					match(NOT);
					}
				}

				setState(623);
				match(NULL);
				}
				break;
			case 7:
				_localctx = new DistinctFromContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(624);
				match(IS);
				setState(626);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(625);
					match(NOT);
					}
				}

				setState(628);
				match(DISTINCT);
				setState(629);
				match(FROM);
				setState(630);
				((DistinctFromContext)_localctx).right = valueExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConcatenationContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public TerminalNode CONCAT() { return getToken(SqlBaseParser.CONCAT, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ConcatenationContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConcatenation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConcatenation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConcatenation(this);
			else return visitor.visitChildren(this);
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
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AtTimeZoneContext extends ValueExpressionContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TimeZoneSpecifierContext timeZoneSpecifier() {
			return getRuleContext(TimeZoneSpecifierContext.class,0);
		}
		public AtTimeZoneContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAtTimeZone(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAtTimeZone(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAtTimeZone(this);
			else return visitor.visitChildren(this);
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
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(637);
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
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(634);
				primaryExpression(0);
				}
				break;
			case PLUS:
			case MINUS:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(635);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(636);
				valueExpression(4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(653);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,83,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(651);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(639);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(640);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (ASTERISK - 195)) | (1L << (SLASH - 195)) | (1L << (PERCENT - 195)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(641);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(642);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(643);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(644);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 3:
						{
						_localctx = new ConcatenationContext(new ValueExpressionContext(_parentctx, _parentState));
						((ConcatenationContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(645);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(646);
						match(CONCAT);
						setState(647);
						((ConcatenationContext)_localctx).right = valueExpression(2);
						}
						break;
					case 4:
						{
						_localctx = new AtTimeZoneContext(new ValueExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(648);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(649);
						match(AT);
						setState(650);
						timeZoneSpecifier();
						}
						break;
					}
					} 
				}
				setState(655);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,83,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SpecialDateTimeFunctionContext extends PrimaryExpressionContext {
		public Token name;
		public Token precision;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIME() { return getToken(SqlBaseParser.CURRENT_TIME, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TerminalNode LOCALTIME() { return getToken(SqlBaseParser.LOCALTIME, 0); }
		public TerminalNode LOCALTIMESTAMP() { return getToken(SqlBaseParser.LOCALTIMESTAMP, 0); }
		public SpecialDateTimeFunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpecialDateTimeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpecialDateTimeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpecialDateTimeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubstringContext extends PrimaryExpressionContext {
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public SubstringContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubstring(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubstring(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubstring(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TerminalNode TRY_CAST() { return getToken(SqlBaseParser.TRY_CAST, 0); }
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LambdaContext extends PrimaryExpressionContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLambda(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLambda(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NormalizeContext extends PrimaryExpressionContext {
		public TerminalNode NORMALIZE() { return getToken(SqlBaseParser.NORMALIZE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public NormalFormContext normalForm() {
			return getRuleContext(NormalFormContext.class,0);
		}
		public NormalizeContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalize(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalize(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntervalLiteralContext extends PrimaryExpressionContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends PrimaryExpressionContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends PrimaryExpressionContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullLiteralContext extends PrimaryExpressionContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BinaryLiteralContext extends PrimaryExpressionContext {
		public TerminalNode BINARY_LITERAL() { return getToken(SqlBaseParser.BINARY_LITERAL, 0); }
		public BinaryLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBinaryLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBinaryLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBinaryLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExtractContext extends PrimaryExpressionContext {
		public TerminalNode EXTRACT() { return getToken(SqlBaseParser.EXTRACT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public ExtractContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExtract(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExtract(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExtract(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends PrimaryExpressionContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public StringLiteralContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArrayConstructorContext extends PrimaryExpressionContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ArrayConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArrayConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArrayConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArrayConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public OverContext over() {
			return getRuleContext(OverContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends PrimaryExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PositionContext extends PrimaryExpressionContext {
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public PositionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPosition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
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
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(856);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
			case 1:
				{
				_localctx = new NullLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(657);
				match(NULL);
				}
				break;
			case 2:
				{
				_localctx = new IntervalLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(658);
				interval();
				}
				break;
			case 3:
				{
				_localctx = new TypeConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(659);
				identifier();
				setState(660);
				match(STRING);
				}
				break;
			case 4:
				{
				_localctx = new NumericLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(662);
				number();
				}
				break;
			case 5:
				{
				_localctx = new BooleanLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(663);
				booleanValue();
				}
				break;
			case 6:
				{
				_localctx = new StringLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(664);
				match(STRING);
				}
				break;
			case 7:
				{
				_localctx = new BinaryLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(665);
				match(BINARY_LITERAL);
				}
				break;
			case 8:
				{
				_localctx = new PositionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(666);
				match(POSITION);
				setState(667);
				match(T__1);
				setState(668);
				valueExpression(0);
				setState(669);
				match(IN);
				setState(670);
				valueExpression(0);
				setState(671);
				match(T__3);
				}
				break;
			case 9:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(673);
				match(T__1);
				setState(674);
				expression();
				setState(677); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(675);
					match(T__2);
					setState(676);
					expression();
					}
					}
					setState(679); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(681);
				match(T__3);
				}
				break;
			case 10:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(683);
				match(ROW);
				setState(684);
				match(T__1);
				setState(685);
				expression();
				setState(690);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(686);
					match(T__2);
					setState(687);
					expression();
					}
					}
					setState(692);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(693);
				match(T__3);
				}
				break;
			case 11:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(695);
				qualifiedName();
				setState(696);
				match(T__1);
				setState(697);
				match(ASTERISK);
				setState(698);
				match(T__3);
				setState(700);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
				case 1:
					{
					setState(699);
					over();
					}
					break;
				}
				}
				break;
			case 12:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(702);
				qualifiedName();
				setState(703);
				match(T__1);
				setState(715);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << ALL) | (1L << DISTINCT) | (1L << APPROXIMATE) | (1L << AT) | (1L << CONFIDENCE) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << SUBSTRING) | (1L << POSITION) | (1L << TINYINT) | (1L << SMALLINT) | (1L << INTEGER) | (1L << DATE) | (1L << TIME) | (1L << TIMESTAMP) | (1L << INTERVAL) | (1L << YEAR) | (1L << MONTH) | (1L << DAY) | (1L << HOUR) | (1L << MINUTE))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L << (CURRENT_DATE - 64)) | (1L << (CURRENT_TIME - 64)) | (1L << (CURRENT_TIMESTAMP - 64)) | (1L << (LOCALTIME - 64)) | (1L << (LOCALTIMESTAMP - 64)) | (1L << (EXTRACT - 64)) | (1L << (CASE - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (GRANT - 64)) | (1L << (REVOKE - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (PUBLIC - 64)) | (1L << (OPTION - 64)) | (1L << (EXPLAIN - 64)) | (1L << (ANALYZE - 64)) | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (1L << (TEXT - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (LOGICAL - 64)) | (1L << (DISTRIBUTED - 64)) | (1L << (TRY - 64)) | (1L << (CAST - 64)) | (1L << (TRY_CAST - 64)) | (1L << (SHOW - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L << (CATALOGS - 128)) | (1L << (COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L << (USE - 128)) | (1L << (PARTITIONS - 128)) | (1L << (FUNCTIONS - 128)) | (1L << (TO - 128)) | (1L << (SYSTEM - 128)) | (1L << (BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET - 128)) | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L << (DATA - 128)) | (1L << (START - 128)) | (1L << (TRANSACTION - 128)) | (1L << (COMMIT - 128)) | (1L << (ROLLBACK - 128)) | (1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (1L << (LEVEL - 128)) | (1L << (SERIALIZABLE - 128)) | (1L << (REPEATABLE - 128)) | (1L << (COMMITTED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (1L << (WRITE - 128)) | (1L << (ONLY - 128)) | (1L << (CALL - 128)) | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L << (NFC - 128)) | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (IF - 128)) | (1L << (NULLIF - 128)) | (1L << (COALESCE - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PLUS - 193)) | (1L << (MINUS - 193)) | (1L << (STRING - 193)) | (1L << (BINARY_LITERAL - 193)) | (1L << (INTEGER_VALUE - 193)) | (1L << (DECIMAL_VALUE - 193)) | (1L << (IDENTIFIER - 193)) | (1L << (DIGIT_IDENTIFIER - 193)) | (1L << (QUOTED_IDENTIFIER - 193)) | (1L << (BACKQUOTED_IDENTIFIER - 193)))) != 0)) {
					{
					setState(705);
					_la = _input.LA(1);
					if (_la==ALL || _la==DISTINCT) {
						{
						setState(704);
						setQuantifier();
						}
					}

					setState(707);
					expression();
					setState(712);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(708);
						match(T__2);
						setState(709);
						expression();
						}
						}
						setState(714);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(717);
				match(T__3);
				setState(719);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,90,_ctx) ) {
				case 1:
					{
					setState(718);
					over();
					}
					break;
				}
				}
				break;
			case 13:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(721);
				identifier();
				setState(722);
				match(T__5);
				setState(723);
				expression();
				}
				break;
			case 14:
				{
				_localctx = new LambdaContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(725);
				match(T__1);
				setState(726);
				identifier();
				setState(731);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(727);
					match(T__2);
					setState(728);
					identifier();
					}
					}
					setState(733);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(734);
				match(T__3);
				setState(735);
				match(T__5);
				setState(736);
				expression();
				}
				break;
			case 15:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(738);
				match(T__1);
				setState(739);
				query();
				setState(740);
				match(T__3);
				}
				break;
			case 16:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(742);
				match(EXISTS);
				setState(743);
				match(T__1);
				setState(744);
				query();
				setState(745);
				match(T__3);
				}
				break;
			case 17:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(747);
				match(CASE);
				setState(748);
				valueExpression(0);
				setState(750); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(749);
					whenClause();
					}
					}
					setState(752); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(756);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(754);
					match(ELSE);
					setState(755);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(758);
				match(END);
				}
				break;
			case 18:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(760);
				match(CASE);
				setState(762); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(761);
					whenClause();
					}
					}
					setState(764); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(768);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(766);
					match(ELSE);
					setState(767);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(770);
				match(END);
				}
				break;
			case 19:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(772);
				match(CAST);
				setState(773);
				match(T__1);
				setState(774);
				expression();
				setState(775);
				match(AS);
				setState(776);
				type(0);
				setState(777);
				match(T__3);
				}
				break;
			case 20:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(779);
				match(TRY_CAST);
				setState(780);
				match(T__1);
				setState(781);
				expression();
				setState(782);
				match(AS);
				setState(783);
				type(0);
				setState(784);
				match(T__3);
				}
				break;
			case 21:
				{
				_localctx = new ArrayConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(786);
				match(ARRAY);
				setState(787);
				match(T__6);
				setState(796);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << ADD) | (1L << APPROXIMATE) | (1L << AT) | (1L << CONFIDENCE) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << SUBSTRING) | (1L << POSITION) | (1L << TINYINT) | (1L << SMALLINT) | (1L << INTEGER) | (1L << DATE) | (1L << TIME) | (1L << TIMESTAMP) | (1L << INTERVAL) | (1L << YEAR) | (1L << MONTH) | (1L << DAY) | (1L << HOUR) | (1L << MINUTE))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (SECOND - 64)) | (1L << (ZONE - 64)) | (1L << (CURRENT_DATE - 64)) | (1L << (CURRENT_TIME - 64)) | (1L << (CURRENT_TIMESTAMP - 64)) | (1L << (LOCALTIME - 64)) | (1L << (LOCALTIMESTAMP - 64)) | (1L << (EXTRACT - 64)) | (1L << (CASE - 64)) | (1L << (OVER - 64)) | (1L << (PARTITION - 64)) | (1L << (RANGE - 64)) | (1L << (ROWS - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (GRANT - 64)) | (1L << (REVOKE - 64)) | (1L << (PRIVILEGES - 64)) | (1L << (PUBLIC - 64)) | (1L << (OPTION - 64)) | (1L << (EXPLAIN - 64)) | (1L << (ANALYZE - 64)) | (1L << (FORMAT - 64)) | (1L << (TYPE - 64)) | (1L << (TEXT - 64)) | (1L << (GRAPHVIZ - 64)) | (1L << (LOGICAL - 64)) | (1L << (DISTRIBUTED - 64)) | (1L << (TRY - 64)) | (1L << (CAST - 64)) | (1L << (TRY_CAST - 64)) | (1L << (SHOW - 64)))) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & ((1L << (TABLES - 128)) | (1L << (SCHEMAS - 128)) | (1L << (CATALOGS - 128)) | (1L << (COLUMNS - 128)) | (1L << (COLUMN - 128)) | (1L << (USE - 128)) | (1L << (PARTITIONS - 128)) | (1L << (FUNCTIONS - 128)) | (1L << (TO - 128)) | (1L << (SYSTEM - 128)) | (1L << (BERNOULLI - 128)) | (1L << (POISSONIZED - 128)) | (1L << (TABLESAMPLE - 128)) | (1L << (RESCALED - 128)) | (1L << (ARRAY - 128)) | (1L << (MAP - 128)) | (1L << (SET - 128)) | (1L << (RESET - 128)) | (1L << (SESSION - 128)) | (1L << (DATA - 128)) | (1L << (START - 128)) | (1L << (TRANSACTION - 128)) | (1L << (COMMIT - 128)) | (1L << (ROLLBACK - 128)) | (1L << (WORK - 128)) | (1L << (ISOLATION - 128)) | (1L << (LEVEL - 128)) | (1L << (SERIALIZABLE - 128)) | (1L << (REPEATABLE - 128)) | (1L << (COMMITTED - 128)) | (1L << (UNCOMMITTED - 128)) | (1L << (READ - 128)) | (1L << (WRITE - 128)) | (1L << (ONLY - 128)) | (1L << (CALL - 128)) | (1L << (NORMALIZE - 128)) | (1L << (NFD - 128)) | (1L << (NFC - 128)) | (1L << (NFKD - 128)) | (1L << (NFKC - 128)) | (1L << (IF - 128)) | (1L << (NULLIF - 128)) | (1L << (COALESCE - 128)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 && ((1L << (_la - 193)) & ((1L << (PLUS - 193)) | (1L << (MINUS - 193)) | (1L << (STRING - 193)) | (1L << (BINARY_LITERAL - 193)) | (1L << (INTEGER_VALUE - 193)) | (1L << (DECIMAL_VALUE - 193)) | (1L << (IDENTIFIER - 193)) | (1L << (DIGIT_IDENTIFIER - 193)) | (1L << (QUOTED_IDENTIFIER - 193)) | (1L << (BACKQUOTED_IDENTIFIER - 193)))) != 0)) {
					{
					setState(788);
					expression();
					setState(793);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(789);
						match(T__2);
						setState(790);
						expression();
						}
						}
						setState(795);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(798);
				match(T__7);
				}
				break;
			case 22:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(799);
				identifier();
				}
				break;
			case 23:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(800);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_DATE);
				}
				break;
			case 24:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(801);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIME);
				setState(805);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(802);
					match(T__1);
					setState(803);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(804);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 25:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(807);
				((SpecialDateTimeFunctionContext)_localctx).name = match(CURRENT_TIMESTAMP);
				setState(811);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,99,_ctx) ) {
				case 1:
					{
					setState(808);
					match(T__1);
					setState(809);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(810);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 26:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(813);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIME);
				setState(817);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
				case 1:
					{
					setState(814);
					match(T__1);
					setState(815);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(816);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 27:
				{
				_localctx = new SpecialDateTimeFunctionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(819);
				((SpecialDateTimeFunctionContext)_localctx).name = match(LOCALTIMESTAMP);
				setState(823);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
				case 1:
					{
					setState(820);
					match(T__1);
					setState(821);
					((SpecialDateTimeFunctionContext)_localctx).precision = match(INTEGER_VALUE);
					setState(822);
					match(T__3);
					}
					break;
				}
				}
				break;
			case 28:
				{
				_localctx = new SubstringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(825);
				match(SUBSTRING);
				setState(826);
				match(T__1);
				setState(827);
				valueExpression(0);
				setState(828);
				match(FROM);
				setState(829);
				valueExpression(0);
				setState(832);
				_la = _input.LA(1);
				if (_la==FOR) {
					{
					setState(830);
					match(FOR);
					setState(831);
					valueExpression(0);
					}
				}

				setState(834);
				match(T__3);
				}
				break;
			case 29:
				{
				_localctx = new NormalizeContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(836);
				match(NORMALIZE);
				setState(837);
				match(T__1);
				setState(838);
				valueExpression(0);
				setState(841);
				_la = _input.LA(1);
				if (_la==T__2) {
					{
					setState(839);
					match(T__2);
					setState(840);
					normalForm();
					}
				}

				setState(843);
				match(T__3);
				}
				break;
			case 30:
				{
				_localctx = new ExtractContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(845);
				match(EXTRACT);
				setState(846);
				match(T__1);
				setState(847);
				identifier();
				setState(848);
				match(FROM);
				setState(849);
				valueExpression(0);
				setState(850);
				match(T__3);
				}
				break;
			case 31:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(852);
				match(T__1);
				setState(853);
				expression();
				setState(854);
				match(T__3);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(868);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(866);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(858);
						if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
						setState(859);
						match(T__6);
						setState(860);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(861);
						match(T__7);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(863);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(864);
						match(T__4);
						setState(865);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(870);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class TimeZoneSpecifierContext extends ParserRuleContext {
		public TimeZoneSpecifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timeZoneSpecifier; }
	 
		public TimeZoneSpecifierContext() { }
		public void copyFrom(TimeZoneSpecifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TimeZoneIntervalContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public TimeZoneIntervalContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneInterval(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TimeZoneStringContext extends TimeZoneSpecifierContext {
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TimeZoneStringContext(TimeZoneSpecifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeZoneString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeZoneString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeZoneString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TimeZoneSpecifierContext timeZoneSpecifier() throws RecognitionException {
		TimeZoneSpecifierContext _localctx = new TimeZoneSpecifierContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_timeZoneSpecifier);
		try {
			setState(877);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,107,_ctx) ) {
			case 1:
				_localctx = new TimeZoneIntervalContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(871);
				match(TIME);
				setState(872);
				match(ZONE);
				setState(873);
				interval();
				}
				break;
			case 2:
				_localctx = new TimeZoneStringContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(874);
				match(TIME);
				setState(875);
				match(ZONE);
				setState(876);
				match(STRING);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(879);
			_la = _input.LA(1);
			if ( !(((((_la - 187)) & ~0x3f) == 0 && ((1L << (_la - 187)) & ((1L << (EQ - 187)) | (1L << (NEQ - 187)) | (1L << (LT - 187)) | (1L << (LTE - 187)) | (1L << (GT - 187)) | (1L << (GTE - 187)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(881);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public Token sign;
		public IntervalFieldContext from;
		public IntervalFieldContext to;
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_interval);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(883);
			match(INTERVAL);
			setState(885);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(884);
				((IntervalContext)_localctx).sign = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
					((IntervalContext)_localctx).sign = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(887);
			match(STRING);
			setState(888);
			((IntervalContext)_localctx).from = intervalField();
			setState(891);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				{
				setState(889);
				match(TO);
				setState(890);
				((IntervalContext)_localctx).to = intervalField();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalFieldContext extends ParserRuleContext {
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_intervalField);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(893);
			_la = _input.LA(1);
			if ( !(((((_la - 59)) & ~0x3f) == 0 && ((1L << (_la - 59)) & ((1L << (YEAR - 59)) | (1L << (MONTH - 59)) | (1L << (DAY - 59)) | (1L << (HOUR - 59)) | (1L << (MINUTE - 59)) | (1L << (SECOND - 59)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeContext extends ParserRuleContext {
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public BaseTypeContext baseType() {
			return getRuleContext(BaseTypeContext.class,0);
		}
		public List<TypeParameterContext> typeParameter() {
			return getRuleContexts(TypeParameterContext.class);
		}
		public TypeParameterContext typeParameter(int i) {
			return getRuleContext(TypeParameterContext.class,i);
		}
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitType(this);
			else return visitor.visitChildren(this);
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
		int _startState = 80;
		enterRecursionRule(_localctx, 80, RULE_type, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(937);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
			case 1:
				{
				setState(896);
				match(ARRAY);
				setState(897);
				match(LT);
				setState(898);
				type(0);
				setState(899);
				match(GT);
				}
				break;
			case 2:
				{
				setState(901);
				match(MAP);
				setState(902);
				match(LT);
				setState(903);
				type(0);
				setState(904);
				match(T__2);
				setState(905);
				type(0);
				setState(906);
				match(GT);
				}
				break;
			case 3:
				{
				setState(908);
				match(ROW);
				setState(909);
				match(T__1);
				setState(910);
				identifier();
				setState(911);
				type(0);
				setState(918);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(912);
					match(T__2);
					setState(913);
					identifier();
					setState(914);
					type(0);
					}
					}
					setState(920);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(921);
				match(T__3);
				}
				break;
			case 4:
				{
				setState(923);
				baseType();
				setState(935);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
				case 1:
					{
					setState(924);
					match(T__1);
					setState(925);
					typeParameter();
					setState(930);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(926);
						match(T__2);
						setState(927);
						typeParameter();
						}
						}
						setState(932);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(933);
					match(T__3);
					}
					break;
				}
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(943);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,114,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new TypeContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_type);
					setState(939);
					if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
					setState(940);
					match(ARRAY);
					}
					} 
				}
				setState(945);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,114,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class TypeParameterContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public TypeParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typeParameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeParameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeParameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeParameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TypeParameterContext typeParameter() throws RecognitionException {
		TypeParameterContext _localctx = new TypeParameterContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_typeParameter);
		try {
			setState(948);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(946);
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
				setState(947);
				type(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BaseTypeContext extends ParserRuleContext {
		public TerminalNode TIME_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIME_WITH_TIME_ZONE, 0); }
		public TerminalNode TIMESTAMP_WITH_TIME_ZONE() { return getToken(SqlBaseParser.TIMESTAMP_WITH_TIME_ZONE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public BaseTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_baseType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBaseType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBaseType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBaseType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BaseTypeContext baseType() throws RecognitionException {
		BaseTypeContext _localctx = new BaseTypeContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_baseType);
		try {
			setState(953);
			switch (_input.LA(1)) {
			case TIME_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 1);
				{
				setState(950);
				match(TIME_WITH_TIME_ZONE);
				}
				break;
			case TIMESTAMP_WITH_TIME_ZONE:
				enterOuterAlt(_localctx, 2);
				{
				setState(951);
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
				setState(952);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(955);
			match(WHEN);
			setState(956);
			((WhenClauseContext)_localctx).condition = expression();
			setState(957);
			match(THEN);
			setState(958);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OverContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public OverContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_over; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOver(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOver(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOver(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OverContext over() throws RecognitionException {
		OverContext _localctx = new OverContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_over);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(960);
			match(OVER);
			setState(961);
			match(T__1);
			setState(972);
			_la = _input.LA(1);
			if (_la==PARTITION) {
				{
				setState(962);
				match(PARTITION);
				setState(963);
				match(BY);
				setState(964);
				((OverContext)_localctx).expression = expression();
				((OverContext)_localctx).partition.add(((OverContext)_localctx).expression);
				setState(969);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(965);
					match(T__2);
					setState(966);
					((OverContext)_localctx).expression = expression();
					((OverContext)_localctx).partition.add(((OverContext)_localctx).expression);
					}
					}
					setState(971);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(984);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(974);
				match(ORDER);
				setState(975);
				match(BY);
				setState(976);
				sortItem();
				setState(981);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(977);
					match(T__2);
					setState(978);
					sortItem();
					}
					}
					setState(983);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(987);
			_la = _input.LA(1);
			if (_la==RANGE || _la==ROWS) {
				{
				setState(986);
				windowFrame();
				}
			}

			setState(989);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_windowFrame);
		try {
			setState(1007);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,122,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(991);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(992);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(993);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(994);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(995);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(996);
				match(BETWEEN);
				setState(997);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(998);
				match(AND);
				setState(999);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1001);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(1002);
				match(BETWEEN);
				setState(1003);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(1004);
				match(AND);
				setState(1005);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
	 
		public FrameBoundContext() { }
		public void copyFrom(FrameBoundContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BoundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public BoundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBoundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBoundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBoundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnboundedFrameContext extends FrameBoundContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public UnboundedFrameContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnboundedFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnboundedFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnboundedFrame(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CurrentRowBoundContext extends FrameBoundContext {
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public CurrentRowBoundContext(FrameBoundContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCurrentRowBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCurrentRowBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCurrentRowBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_frameBound);
		int _la;
		try {
			setState(1018);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,123,_ctx) ) {
			case 1:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1009);
				match(UNBOUNDED);
				setState(1010);
				((UnboundedFrameContext)_localctx).boundType = match(PRECEDING);
				}
				break;
			case 2:
				_localctx = new UnboundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1011);
				match(UNBOUNDED);
				setState(1012);
				((UnboundedFrameContext)_localctx).boundType = match(FOLLOWING);
				}
				break;
			case 3:
				_localctx = new CurrentRowBoundContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1013);
				match(CURRENT);
				setState(1014);
				match(ROW);
				}
				break;
			case 4:
				_localctx = new BoundedFrameContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1015);
				expression();
				setState(1016);
				((BoundedFrameContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((BoundedFrameContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExplainOptionContext extends ParserRuleContext {
		public ExplainOptionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explainOption; }
	 
		public ExplainOptionContext() { }
		public void copyFrom(ExplainOptionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExplainFormatContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public ExplainFormatContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExplainTypeContext extends ExplainOptionContext {
		public Token value;
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public ExplainTypeContext(ExplainOptionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplainType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplainType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplainType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExplainOptionContext explainOption() throws RecognitionException {
		ExplainOptionContext _localctx = new ExplainOptionContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_explainOption);
		int _la;
		try {
			setState(1024);
			switch (_input.LA(1)) {
			case FORMAT:
				_localctx = new ExplainFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1020);
				match(FORMAT);
				setState(1021);
				((ExplainFormatContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==TEXT || _la==GRAPHVIZ) ) {
					((ExplainFormatContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case TYPE:
				_localctx = new ExplainTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1022);
				match(TYPE);
				setState(1023);
				((ExplainTypeContext)_localctx).value = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LOGICAL || _la==DISTRIBUTED) ) {
					((ExplainTypeContext)_localctx).value = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TransactionModeContext extends ParserRuleContext {
		public TransactionModeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transactionMode; }
	 
		public TransactionModeContext() { }
		public void copyFrom(TransactionModeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TransactionAccessModeContext extends TransactionModeContext {
		public Token accessMode;
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TransactionAccessModeContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTransactionAccessMode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTransactionAccessMode(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTransactionAccessMode(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IsolationLevelContext extends TransactionModeContext {
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public LevelOfIsolationContext levelOfIsolation() {
			return getRuleContext(LevelOfIsolationContext.class,0);
		}
		public IsolationLevelContext(TransactionModeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIsolationLevel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIsolationLevel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIsolationLevel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TransactionModeContext transactionMode() throws RecognitionException {
		TransactionModeContext _localctx = new TransactionModeContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_transactionMode);
		int _la;
		try {
			setState(1031);
			switch (_input.LA(1)) {
			case ISOLATION:
				_localctx = new IsolationLevelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1026);
				match(ISOLATION);
				setState(1027);
				match(LEVEL);
				setState(1028);
				levelOfIsolation();
				}
				break;
			case READ:
				_localctx = new TransactionAccessModeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1029);
				match(READ);
				setState(1030);
				((TransactionAccessModeContext)_localctx).accessMode = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==WRITE || _la==ONLY) ) {
					((TransactionAccessModeContext)_localctx).accessMode = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LevelOfIsolationContext extends ParserRuleContext {
		public LevelOfIsolationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_levelOfIsolation; }
	 
		public LevelOfIsolationContext() { }
		public void copyFrom(LevelOfIsolationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ReadUncommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public ReadUncommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadUncommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadUncommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadUncommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SerializableContext extends LevelOfIsolationContext {
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public SerializableContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSerializable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSerializable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSerializable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ReadCommittedContext extends LevelOfIsolationContext {
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public ReadCommittedContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterReadCommitted(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitReadCommitted(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitReadCommitted(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepeatableReadContext extends LevelOfIsolationContext {
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public RepeatableReadContext(LevelOfIsolationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepeatableRead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepeatableRead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepeatableRead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LevelOfIsolationContext levelOfIsolation() throws RecognitionException {
		LevelOfIsolationContext _localctx = new LevelOfIsolationContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_levelOfIsolation);
		try {
			setState(1040);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,126,_ctx) ) {
			case 1:
				_localctx = new ReadUncommittedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1033);
				match(READ);
				setState(1034);
				match(UNCOMMITTED);
				}
				break;
			case 2:
				_localctx = new ReadCommittedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1035);
				match(READ);
				setState(1036);
				match(COMMITTED);
				}
				break;
			case 3:
				_localctx = new RepeatableReadContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1037);
				match(REPEATABLE);
				setState(1038);
				match(READ);
				}
				break;
			case 4:
				_localctx = new SerializableContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1039);
				match(SERIALIZABLE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CallArgumentContext extends ParserRuleContext {
		public CallArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_callArgument; }
	 
		public CallArgumentContext() { }
		public void copyFrom(CallArgumentContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PositionalArgumentContext extends CallArgumentContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPositionalArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPositionalArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPositionalArgument(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NamedArgumentContext extends CallArgumentContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CallArgumentContext callArgument() throws RecognitionException {
		CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_callArgument);
		try {
			setState(1047);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,127,_ctx) ) {
			case 1:
				_localctx = new PositionalArgumentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1042);
				expression();
				}
				break;
			case 2:
				_localctx = new NamedArgumentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1043);
				identifier();
				setState(1044);
				match(T__8);
				setState(1045);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrivilegeContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PrivilegeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_privilege; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrivilege(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrivilege(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrivilege(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrivilegeContext privilege() throws RecognitionException {
		PrivilegeContext _localctx = new PrivilegeContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_privilege);
		try {
			setState(1053);
			switch (_input.LA(1)) {
			case SELECT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1049);
				match(SELECT);
				}
				break;
			case DELETE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1050);
				match(DELETE);
				}
				break;
			case INSERT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1051);
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
				setState(1052);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1055);
			identifier();
			setState(1060);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,129,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1056);
					match(T__4);
					setState(1057);
					identifier();
					}
					} 
				}
				setState(1062);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,129,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BackQuotedIdentifierContext extends IdentifierContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public BackQuotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBackQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBackQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBackQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends IdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DigitIdentifierContext extends IdentifierContext {
		public TerminalNode DIGIT_IDENTIFIER() { return getToken(SqlBaseParser.DIGIT_IDENTIFIER, 0); }
		public DigitIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDigitIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDigitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDigitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_identifier);
		try {
			setState(1068);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1063);
				match(IDENTIFIER);
				}
				break;
			case QUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1064);
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
				setState(1065);
				nonReserved();
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new BackQuotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1066);
				match(BACKQUOTED_IDENTIFIER);
				}
				break;
			case DIGIT_IDENTIFIER:
				_localctx = new DigitIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1067);
				match(DIGIT_IDENTIFIER);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(SqlBaseParser.QUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1070);
			match(QUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_number);
		try {
			setState(1074);
			switch (_input.LA(1)) {
			case DECIMAL_VALUE:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1072);
				match(DECIMAL_VALUE);
				}
				break;
			case INTEGER_VALUE:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1073);
				match(INTEGER_VALUE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode SCHEMAS() { return getToken(SqlBaseParser.SCHEMAS, 0); }
		public TerminalNode CATALOGS() { return getToken(SqlBaseParser.CATALOGS, 0); }
		public TerminalNode SESSION() { return getToken(SqlBaseParser.SESSION, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode TINYINT() { return getToken(SqlBaseParser.TINYINT, 0); }
		public TerminalNode SMALLINT() { return getToken(SqlBaseParser.SMALLINT, 0); }
		public TerminalNode INTEGER() { return getToken(SqlBaseParser.INTEGER, 0); }
		public TerminalNode DATE() { return getToken(SqlBaseParser.DATE, 0); }
		public TerminalNode TIME() { return getToken(SqlBaseParser.TIME, 0); }
		public TerminalNode TIMESTAMP() { return getToken(SqlBaseParser.TIMESTAMP, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode ZONE() { return getToken(SqlBaseParser.ZONE, 0); }
		public TerminalNode YEAR() { return getToken(SqlBaseParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SqlBaseParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SqlBaseParser.DAY, 0); }
		public TerminalNode HOUR() { return getToken(SqlBaseParser.HOUR, 0); }
		public TerminalNode MINUTE() { return getToken(SqlBaseParser.MINUTE, 0); }
		public TerminalNode SECOND() { return getToken(SqlBaseParser.SECOND, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode TYPE() { return getToken(SqlBaseParser.TYPE, 0); }
		public TerminalNode TEXT() { return getToken(SqlBaseParser.TEXT, 0); }
		public TerminalNode GRAPHVIZ() { return getToken(SqlBaseParser.GRAPHVIZ, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode DISTRIBUTED() { return getToken(SqlBaseParser.DISTRIBUTED, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode SYSTEM() { return getToken(SqlBaseParser.SYSTEM, 0); }
		public TerminalNode BERNOULLI() { return getToken(SqlBaseParser.BERNOULLI, 0); }
		public TerminalNode POISSONIZED() { return getToken(SqlBaseParser.POISSONIZED, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode RESCALED() { return getToken(SqlBaseParser.RESCALED, 0); }
		public TerminalNode APPROXIMATE() { return getToken(SqlBaseParser.APPROXIMATE, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode CONFIDENCE() { return getToken(SqlBaseParser.CONFIDENCE, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NULLIF() { return getToken(SqlBaseParser.NULLIF, 0); }
		public TerminalNode COALESCE() { return getToken(SqlBaseParser.COALESCE, 0); }
		public TerminalNode TRY() { return getToken(SqlBaseParser.TRY, 0); }
		public NormalFormContext normalForm() {
			return getRuleContext(NormalFormContext.class,0);
		}
		public TerminalNode POSITION() { return getToken(SqlBaseParser.POSITION, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode WORK() { return getToken(SqlBaseParser.WORK, 0); }
		public TerminalNode ISOLATION() { return getToken(SqlBaseParser.ISOLATION, 0); }
		public TerminalNode LEVEL() { return getToken(SqlBaseParser.LEVEL, 0); }
		public TerminalNode SERIALIZABLE() { return getToken(SqlBaseParser.SERIALIZABLE, 0); }
		public TerminalNode REPEATABLE() { return getToken(SqlBaseParser.REPEATABLE, 0); }
		public TerminalNode COMMITTED() { return getToken(SqlBaseParser.COMMITTED, 0); }
		public TerminalNode UNCOMMITTED() { return getToken(SqlBaseParser.UNCOMMITTED, 0); }
		public TerminalNode READ() { return getToken(SqlBaseParser.READ, 0); }
		public TerminalNode WRITE() { return getToken(SqlBaseParser.WRITE, 0); }
		public TerminalNode ONLY() { return getToken(SqlBaseParser.ONLY, 0); }
		public TerminalNode CALL() { return getToken(SqlBaseParser.CALL, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode PRIVILEGES() { return getToken(SqlBaseParser.PRIVILEGES, 0); }
		public TerminalNode PUBLIC() { return getToken(SqlBaseParser.PUBLIC, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode SUBSTRING() { return getToken(SqlBaseParser.SUBSTRING, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_nonReserved);
		try {
			setState(1161);
			switch (_input.LA(1)) {
			case SHOW:
				enterOuterAlt(_localctx, 1);
				{
				setState(1076);
				match(SHOW);
				}
				break;
			case TABLES:
				enterOuterAlt(_localctx, 2);
				{
				setState(1077);
				match(TABLES);
				}
				break;
			case COLUMNS:
				enterOuterAlt(_localctx, 3);
				{
				setState(1078);
				match(COLUMNS);
				}
				break;
			case COLUMN:
				enterOuterAlt(_localctx, 4);
				{
				setState(1079);
				match(COLUMN);
				}
				break;
			case PARTITIONS:
				enterOuterAlt(_localctx, 5);
				{
				setState(1080);
				match(PARTITIONS);
				}
				break;
			case FUNCTIONS:
				enterOuterAlt(_localctx, 6);
				{
				setState(1081);
				match(FUNCTIONS);
				}
				break;
			case SCHEMAS:
				enterOuterAlt(_localctx, 7);
				{
				setState(1082);
				match(SCHEMAS);
				}
				break;
			case CATALOGS:
				enterOuterAlt(_localctx, 8);
				{
				setState(1083);
				match(CATALOGS);
				}
				break;
			case SESSION:
				enterOuterAlt(_localctx, 9);
				{
				setState(1084);
				match(SESSION);
				}
				break;
			case ADD:
				enterOuterAlt(_localctx, 10);
				{
				setState(1085);
				match(ADD);
				}
				break;
			case OVER:
				enterOuterAlt(_localctx, 11);
				{
				setState(1086);
				match(OVER);
				}
				break;
			case PARTITION:
				enterOuterAlt(_localctx, 12);
				{
				setState(1087);
				match(PARTITION);
				}
				break;
			case RANGE:
				enterOuterAlt(_localctx, 13);
				{
				setState(1088);
				match(RANGE);
				}
				break;
			case ROWS:
				enterOuterAlt(_localctx, 14);
				{
				setState(1089);
				match(ROWS);
				}
				break;
			case PRECEDING:
				enterOuterAlt(_localctx, 15);
				{
				setState(1090);
				match(PRECEDING);
				}
				break;
			case FOLLOWING:
				enterOuterAlt(_localctx, 16);
				{
				setState(1091);
				match(FOLLOWING);
				}
				break;
			case CURRENT:
				enterOuterAlt(_localctx, 17);
				{
				setState(1092);
				match(CURRENT);
				}
				break;
			case ROW:
				enterOuterAlt(_localctx, 18);
				{
				setState(1093);
				match(ROW);
				}
				break;
			case MAP:
				enterOuterAlt(_localctx, 19);
				{
				setState(1094);
				match(MAP);
				}
				break;
			case ARRAY:
				enterOuterAlt(_localctx, 20);
				{
				setState(1095);
				match(ARRAY);
				}
				break;
			case TINYINT:
				enterOuterAlt(_localctx, 21);
				{
				setState(1096);
				match(TINYINT);
				}
				break;
			case SMALLINT:
				enterOuterAlt(_localctx, 22);
				{
				setState(1097);
				match(SMALLINT);
				}
				break;
			case INTEGER:
				enterOuterAlt(_localctx, 23);
				{
				setState(1098);
				match(INTEGER);
				}
				break;
			case DATE:
				enterOuterAlt(_localctx, 24);
				{
				setState(1099);
				match(DATE);
				}
				break;
			case TIME:
				enterOuterAlt(_localctx, 25);
				{
				setState(1100);
				match(TIME);
				}
				break;
			case TIMESTAMP:
				enterOuterAlt(_localctx, 26);
				{
				setState(1101);
				match(TIMESTAMP);
				}
				break;
			case INTERVAL:
				enterOuterAlt(_localctx, 27);
				{
				setState(1102);
				match(INTERVAL);
				}
				break;
			case ZONE:
				enterOuterAlt(_localctx, 28);
				{
				setState(1103);
				match(ZONE);
				}
				break;
			case YEAR:
				enterOuterAlt(_localctx, 29);
				{
				setState(1104);
				match(YEAR);
				}
				break;
			case MONTH:
				enterOuterAlt(_localctx, 30);
				{
				setState(1105);
				match(MONTH);
				}
				break;
			case DAY:
				enterOuterAlt(_localctx, 31);
				{
				setState(1106);
				match(DAY);
				}
				break;
			case HOUR:
				enterOuterAlt(_localctx, 32);
				{
				setState(1107);
				match(HOUR);
				}
				break;
			case MINUTE:
				enterOuterAlt(_localctx, 33);
				{
				setState(1108);
				match(MINUTE);
				}
				break;
			case SECOND:
				enterOuterAlt(_localctx, 34);
				{
				setState(1109);
				match(SECOND);
				}
				break;
			case EXPLAIN:
				enterOuterAlt(_localctx, 35);
				{
				setState(1110);
				match(EXPLAIN);
				}
				break;
			case ANALYZE:
				enterOuterAlt(_localctx, 36);
				{
				setState(1111);
				match(ANALYZE);
				}
				break;
			case FORMAT:
				enterOuterAlt(_localctx, 37);
				{
				setState(1112);
				match(FORMAT);
				}
				break;
			case TYPE:
				enterOuterAlt(_localctx, 38);
				{
				setState(1113);
				match(TYPE);
				}
				break;
			case TEXT:
				enterOuterAlt(_localctx, 39);
				{
				setState(1114);
				match(TEXT);
				}
				break;
			case GRAPHVIZ:
				enterOuterAlt(_localctx, 40);
				{
				setState(1115);
				match(GRAPHVIZ);
				}
				break;
			case LOGICAL:
				enterOuterAlt(_localctx, 41);
				{
				setState(1116);
				match(LOGICAL);
				}
				break;
			case DISTRIBUTED:
				enterOuterAlt(_localctx, 42);
				{
				setState(1117);
				match(DISTRIBUTED);
				}
				break;
			case TABLESAMPLE:
				enterOuterAlt(_localctx, 43);
				{
				setState(1118);
				match(TABLESAMPLE);
				}
				break;
			case SYSTEM:
				enterOuterAlt(_localctx, 44);
				{
				setState(1119);
				match(SYSTEM);
				}
				break;
			case BERNOULLI:
				enterOuterAlt(_localctx, 45);
				{
				setState(1120);
				match(BERNOULLI);
				}
				break;
			case POISSONIZED:
				enterOuterAlt(_localctx, 46);
				{
				setState(1121);
				match(POISSONIZED);
				}
				break;
			case USE:
				enterOuterAlt(_localctx, 47);
				{
				setState(1122);
				match(USE);
				}
				break;
			case TO:
				enterOuterAlt(_localctx, 48);
				{
				setState(1123);
				match(TO);
				}
				break;
			case RESCALED:
				enterOuterAlt(_localctx, 49);
				{
				setState(1124);
				match(RESCALED);
				}
				break;
			case APPROXIMATE:
				enterOuterAlt(_localctx, 50);
				{
				setState(1125);
				match(APPROXIMATE);
				}
				break;
			case AT:
				enterOuterAlt(_localctx, 51);
				{
				setState(1126);
				match(AT);
				}
				break;
			case CONFIDENCE:
				enterOuterAlt(_localctx, 52);
				{
				setState(1127);
				match(CONFIDENCE);
				}
				break;
			case SET:
				enterOuterAlt(_localctx, 53);
				{
				setState(1128);
				match(SET);
				}
				break;
			case RESET:
				enterOuterAlt(_localctx, 54);
				{
				setState(1129);
				match(RESET);
				}
				break;
			case VIEW:
				enterOuterAlt(_localctx, 55);
				{
				setState(1130);
				match(VIEW);
				}
				break;
			case REPLACE:
				enterOuterAlt(_localctx, 56);
				{
				setState(1131);
				match(REPLACE);
				}
				break;
			case IF:
				enterOuterAlt(_localctx, 57);
				{
				setState(1132);
				match(IF);
				}
				break;
			case NULLIF:
				enterOuterAlt(_localctx, 58);
				{
				setState(1133);
				match(NULLIF);
				}
				break;
			case COALESCE:
				enterOuterAlt(_localctx, 59);
				{
				setState(1134);
				match(COALESCE);
				}
				break;
			case TRY:
				enterOuterAlt(_localctx, 60);
				{
				setState(1135);
				match(TRY);
				}
				break;
			case NFD:
			case NFC:
			case NFKD:
			case NFKC:
				enterOuterAlt(_localctx, 61);
				{
				setState(1136);
				normalForm();
				}
				break;
			case POSITION:
				enterOuterAlt(_localctx, 62);
				{
				setState(1137);
				match(POSITION);
				}
				break;
			case NO:
				enterOuterAlt(_localctx, 63);
				{
				setState(1138);
				match(NO);
				}
				break;
			case DATA:
				enterOuterAlt(_localctx, 64);
				{
				setState(1139);
				match(DATA);
				}
				break;
			case START:
				enterOuterAlt(_localctx, 65);
				{
				setState(1140);
				match(START);
				}
				break;
			case TRANSACTION:
				enterOuterAlt(_localctx, 66);
				{
				setState(1141);
				match(TRANSACTION);
				}
				break;
			case COMMIT:
				enterOuterAlt(_localctx, 67);
				{
				setState(1142);
				match(COMMIT);
				}
				break;
			case ROLLBACK:
				enterOuterAlt(_localctx, 68);
				{
				setState(1143);
				match(ROLLBACK);
				}
				break;
			case WORK:
				enterOuterAlt(_localctx, 69);
				{
				setState(1144);
				match(WORK);
				}
				break;
			case ISOLATION:
				enterOuterAlt(_localctx, 70);
				{
				setState(1145);
				match(ISOLATION);
				}
				break;
			case LEVEL:
				enterOuterAlt(_localctx, 71);
				{
				setState(1146);
				match(LEVEL);
				}
				break;
			case SERIALIZABLE:
				enterOuterAlt(_localctx, 72);
				{
				setState(1147);
				match(SERIALIZABLE);
				}
				break;
			case REPEATABLE:
				enterOuterAlt(_localctx, 73);
				{
				setState(1148);
				match(REPEATABLE);
				}
				break;
			case COMMITTED:
				enterOuterAlt(_localctx, 74);
				{
				setState(1149);
				match(COMMITTED);
				}
				break;
			case UNCOMMITTED:
				enterOuterAlt(_localctx, 75);
				{
				setState(1150);
				match(UNCOMMITTED);
				}
				break;
			case READ:
				enterOuterAlt(_localctx, 76);
				{
				setState(1151);
				match(READ);
				}
				break;
			case WRITE:
				enterOuterAlt(_localctx, 77);
				{
				setState(1152);
				match(WRITE);
				}
				break;
			case ONLY:
				enterOuterAlt(_localctx, 78);
				{
				setState(1153);
				match(ONLY);
				}
				break;
			case CALL:
				enterOuterAlt(_localctx, 79);
				{
				setState(1154);
				match(CALL);
				}
				break;
			case GRANT:
				enterOuterAlt(_localctx, 80);
				{
				setState(1155);
				match(GRANT);
				}
				break;
			case REVOKE:
				enterOuterAlt(_localctx, 81);
				{
				setState(1156);
				match(REVOKE);
				}
				break;
			case PRIVILEGES:
				enterOuterAlt(_localctx, 82);
				{
				setState(1157);
				match(PRIVILEGES);
				}
				break;
			case PUBLIC:
				enterOuterAlt(_localctx, 83);
				{
				setState(1158);
				match(PUBLIC);
				}
				break;
			case OPTION:
				enterOuterAlt(_localctx, 84);
				{
				setState(1159);
				match(OPTION);
				}
				break;
			case SUBSTRING:
				enterOuterAlt(_localctx, 85);
				{
				setState(1160);
				match(SUBSTRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NormalFormContext extends ParserRuleContext {
		public TerminalNode NFD() { return getToken(SqlBaseParser.NFD, 0); }
		public TerminalNode NFC() { return getToken(SqlBaseParser.NFC, 0); }
		public TerminalNode NFKD() { return getToken(SqlBaseParser.NFKD, 0); }
		public TerminalNode NFKC() { return getToken(SqlBaseParser.NFKC, 0); }
		public NormalFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_normalForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNormalForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNormalForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNormalForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NormalFormContext normalForm() throws RecognitionException {
		NormalFormContext _localctx = new NormalFormContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_normalForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1163);
			_la = _input.LA(1);
			if ( !(((((_la - 180)) & ~0x3f) == 0 && ((1L << (_la - 180)) & ((1L << (NFD - 180)) | (1L << (NFC - 180)) | (1L << (NFKD - 180)) | (1L << (NFKC - 180)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 10:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 30:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 33:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 34:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		case 40:
			return type_sempred((TypeContext)_localctx, predIndex);
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
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 2);
		case 3:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return precpred(_ctx, 3);
		case 5:
			return precpred(_ctx, 2);
		case 6:
			return precpred(_ctx, 1);
		case 7:
			return precpred(_ctx, 5);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 8:
			return precpred(_ctx, 12);
		case 9:
			return precpred(_ctx, 10);
		}
		return true;
	}
	private boolean type_sempred(TypeContext _localctx, int predIndex) {
		switch (predIndex) {
		case 10:
			return precpred(_ctx, 5);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u00d7\u0490\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\3\2\3\2\7\2"+
		"y\n\2\f\2\16\2|\13\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3"+
		"\5\5\5\u008b\n\5\3\5\3\5\5\5\u008f\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u00a7\n\5"+
		"\3\5\3\5\3\5\3\5\3\5\7\5\u00ae\n\5\f\5\16\5\u00b1\13\5\3\5\3\5\3\5\5\5"+
		"\u00b6\n\5\3\5\3\5\3\5\3\5\5\5\u00bc\n\5\3\5\5\5\u00bf\n\5\3\6\5\6\u00c2"+
		"\n\6\3\6\3\6\3\7\3\7\5\7\u00c8\n\7\3\7\3\7\3\7\7\7\u00cd\n\7\f\7\16\7"+
		"\u00d0\13\7\3\b\3\b\3\b\3\t\3\t\3\t\3\t\7\t\u00d9\n\t\f\t\16\t\u00dc\13"+
		"\t\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\7\13\u00ea\n"+
		"\13\f\13\16\13\u00ed\13\13\5\13\u00ef\n\13\3\13\3\13\5\13\u00f3\n\13\3"+
		"\13\3\13\3\13\3\13\3\13\5\13\u00fa\n\13\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u0102"+
		"\n\f\3\f\3\f\3\f\3\f\5\f\u0108\n\f\3\f\7\f\u010b\n\f\f\f\16\f\u010e\13"+
		"\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\7\r\u0117\n\r\f\r\16\r\u011a\13\r\3\r\3"+
		"\r\3\r\3\r\5\r\u0120\n\r\3\16\3\16\5\16\u0124\n\16\3\16\3\16\5\16\u0128"+
		"\n\16\3\17\3\17\5\17\u012c\n\17\3\17\3\17\3\17\7\17\u0131\n\17\f\17\16"+
		"\17\u0134\13\17\3\17\3\17\5\17\u0138\n\17\3\17\3\17\3\17\3\17\7\17\u013e"+
		"\n\17\f\17\16\17\u0141\13\17\5\17\u0143\n\17\3\17\3\17\5\17\u0147\n\17"+
		"\3\17\3\17\3\17\5\17\u014c\n\17\3\17\3\17\5\17\u0150\n\17\3\20\5\20\u0153"+
		"\n\20\3\20\3\20\3\20\7\20\u0158\n\20\f\20\16\20\u015b\13\20\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\7\21\u0163\n\21\f\21\16\21\u0166\13\21\5\21\u0168"+
		"\n\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u0170\n\21\f\21\16\21\u0173\13"+
		"\21\5\21\u0175\n\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u017e\n\21"+
		"\f\21\16\21\u0181\13\21\3\21\3\21\5\21\u0185\n\21\3\22\3\22\3\22\3\22"+
		"\7\22\u018b\n\22\f\22\16\22\u018e\13\22\5\22\u0190\n\22\3\22\3\22\5\22"+
		"\u0194\n\22\3\23\3\23\3\23\3\23\7\23\u019a\n\23\f\23\16\23\u019d\13\23"+
		"\5\23\u019f\n\23\3\23\3\23\5\23\u01a3\n\23\3\24\3\24\5\24\u01a7\n\24\3"+
		"\24\3\24\3\24\3\24\3\24\3\25\3\25\3\26\3\26\5\26\u01b2\n\26\3\26\5\26"+
		"\u01b5\n\26\3\26\3\26\3\26\3\26\3\26\5\26\u01bc\n\26\3\27\3\27\3\30\5"+
		"\30\u01c1\n\30\3\30\3\30\5\30\u01c5\n\30\3\30\3\30\5\30\u01c9\n\30\3\30"+
		"\3\30\5\30\u01cd\n\30\5\30\u01cf\n\30\3\31\3\31\3\31\3\31\3\31\3\31\3"+
		"\31\7\31\u01d8\n\31\f\31\16\31\u01db\13\31\3\31\3\31\5\31\u01df\n\31\3"+
		"\32\3\32\3\32\3\32\3\32\3\32\3\32\5\32\u01e8\n\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\7\32\u01f0\n\32\f\32\16\32\u01f3\13\32\3\32\3\32\5\32\u01f7"+
		"\n\32\5\32\u01f9\n\32\3\33\3\33\3\34\3\34\5\34\u01ff\n\34\3\34\3\34\5"+
		"\34\u0203\n\34\5\34\u0205\n\34\3\35\3\35\3\35\3\35\7\35\u020b\n\35\f\35"+
		"\16\35\u020e\13\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3"+
		"\36\3\36\7\36\u021c\n\36\f\36\16\36\u021f\13\36\3\36\3\36\3\36\5\36\u0224"+
		"\n\36\3\36\3\36\3\36\3\36\5\36\u022a\n\36\3\37\3\37\3 \3 \3 \3 \5 \u0232"+
		"\n \3 \3 \3 \3 \3 \3 \7 \u023a\n \f \16 \u023d\13 \3!\3!\5!\u0241\n!\3"+
		"\"\3\"\3\"\3\"\5\"\u0247\n\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u024f\n\"\3\""+
		"\3\"\3\"\3\"\3\"\7\"\u0256\n\"\f\"\16\"\u0259\13\"\3\"\3\"\3\"\5\"\u025e"+
		"\n\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u0266\n\"\3\"\3\"\3\"\3\"\5\"\u026c\n"+
		"\"\3\"\3\"\5\"\u0270\n\"\3\"\3\"\3\"\5\"\u0275\n\"\3\"\3\"\3\"\5\"\u027a"+
		"\n\"\3#\3#\3#\3#\5#\u0280\n#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\7#\u028e"+
		"\n#\f#\16#\u0291\13#\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3"+
		"$\3$\3$\3$\3$\6$\u02a8\n$\r$\16$\u02a9\3$\3$\3$\3$\3$\3$\3$\7$\u02b3\n"+
		"$\f$\16$\u02b6\13$\3$\3$\3$\3$\3$\3$\3$\5$\u02bf\n$\3$\3$\3$\5$\u02c4"+
		"\n$\3$\3$\3$\7$\u02c9\n$\f$\16$\u02cc\13$\5$\u02ce\n$\3$\3$\5$\u02d2\n"+
		"$\3$\3$\3$\3$\3$\3$\3$\3$\7$\u02dc\n$\f$\16$\u02df\13$\3$\3$\3$\3$\3$"+
		"\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\6$\u02f1\n$\r$\16$\u02f2\3$\3$\5$\u02f7"+
		"\n$\3$\3$\3$\3$\6$\u02fd\n$\r$\16$\u02fe\3$\3$\5$\u0303\n$\3$\3$\3$\3"+
		"$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\3$\7$\u031a\n$\f$\16"+
		"$\u031d\13$\5$\u031f\n$\3$\3$\3$\3$\3$\3$\3$\5$\u0328\n$\3$\3$\3$\3$\5"+
		"$\u032e\n$\3$\3$\3$\3$\5$\u0334\n$\3$\3$\3$\3$\5$\u033a\n$\3$\3$\3$\3"+
		"$\3$\3$\3$\5$\u0343\n$\3$\3$\3$\3$\3$\3$\3$\5$\u034c\n$\3$\3$\3$\3$\3"+
		"$\3$\3$\3$\3$\3$\3$\3$\3$\5$\u035b\n$\3$\3$\3$\3$\3$\3$\3$\3$\7$\u0365"+
		"\n$\f$\16$\u0368\13$\3%\3%\3%\3%\3%\3%\5%\u0370\n%\3&\3&\3\'\3\'\3(\3"+
		"(\5(\u0378\n(\3(\3(\3(\3(\5(\u037e\n(\3)\3)\3*\3*\3*\3*\3*\3*\3*\3*\3"+
		"*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\3*\7*\u0397\n*\f*\16*\u039a\13*\3*"+
		"\3*\3*\3*\3*\3*\3*\7*\u03a3\n*\f*\16*\u03a6\13*\3*\3*\5*\u03aa\n*\5*\u03ac"+
		"\n*\3*\3*\7*\u03b0\n*\f*\16*\u03b3\13*\3+\3+\5+\u03b7\n+\3,\3,\3,\5,\u03bc"+
		"\n,\3-\3-\3-\3-\3-\3.\3.\3.\3.\3.\3.\3.\7.\u03ca\n.\f.\16.\u03cd\13.\5"+
		".\u03cf\n.\3.\3.\3.\3.\3.\7.\u03d6\n.\f.\16.\u03d9\13.\5.\u03db\n.\3."+
		"\5.\u03de\n.\3.\3.\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\3/\5/"+
		"\u03f2\n/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u03fd\n\60"+
		"\3\61\3\61\3\61\3\61\5\61\u0403\n\61\3\62\3\62\3\62\3\62\3\62\5\62\u040a"+
		"\n\62\3\63\3\63\3\63\3\63\3\63\3\63\3\63\5\63\u0413\n\63\3\64\3\64\3\64"+
		"\3\64\3\64\5\64\u041a\n\64\3\65\3\65\3\65\3\65\5\65\u0420\n\65\3\66\3"+
		"\66\3\66\7\66\u0425\n\66\f\66\16\66\u0428\13\66\3\67\3\67\3\67\3\67\3"+
		"\67\5\67\u042f\n\67\38\38\39\39\59\u0435\n9\3:\3:\3:\3:\3:\3:\3:\3:\3"+
		":\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3"+
		":\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3"+
		":\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3:\3"+
		":\3:\3:\3:\3:\3:\3:\3:\5:\u048c\n:\3;\3;\3;\2\7\26>DFR<\2\4\6\b\n\f\16"+
		"\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bd"+
		"fhjlnprt\2\23\4\2\r\r##\4\2\20\20\u00cb\u00cb\3\2\u008f\u0090\3\2\61\62"+
		"\3\2./\4\2\20\20\23\23\3\2\u0093\u0095\3\2\u00c3\u00c4\3\2\u00c5\u00c7"+
		"\3\2\u00bd\u00c2\3\2+,\3\2=B\3\2^_\3\2z{\3\2|}\3\2\u00af\u00b0\3\2\u00b6"+
		"\u00b9\u0571\2v\3\2\2\2\4\177\3\2\2\2\6\u0082\3\2\2\2\b\u00be\3\2\2\2"+
		"\n\u00c1\3\2\2\2\f\u00c5\3\2\2\2\16\u00d1\3\2\2\2\20\u00d4\3\2\2\2\22"+
		"\u00df\3\2\2\2\24\u00e3\3\2\2\2\26\u00fb\3\2\2\2\30\u011f\3\2\2\2\32\u0121"+
		"\3\2\2\2\34\u0129\3\2\2\2\36\u0152\3\2\2\2 \u0184\3\2\2\2\"\u0193\3\2"+
		"\2\2$\u01a2\3\2\2\2&\u01a4\3\2\2\2(\u01ad\3\2\2\2*\u01bb\3\2\2\2,\u01bd"+
		"\3\2\2\2.\u01ce\3\2\2\2\60\u01de\3\2\2\2\62\u01e0\3\2\2\2\64\u01fa\3\2"+
		"\2\2\66\u01fc\3\2\2\28\u0206\3\2\2\2:\u0229\3\2\2\2<\u022b\3\2\2\2>\u0231"+
		"\3\2\2\2@\u023e\3\2\2\2B\u0279\3\2\2\2D\u027f\3\2\2\2F\u035a\3\2\2\2H"+
		"\u036f\3\2\2\2J\u0371\3\2\2\2L\u0373\3\2\2\2N\u0375\3\2\2\2P\u037f\3\2"+
		"\2\2R\u03ab\3\2\2\2T\u03b6\3\2\2\2V\u03bb\3\2\2\2X\u03bd\3\2\2\2Z\u03c2"+
		"\3\2\2\2\\\u03f1\3\2\2\2^\u03fc\3\2\2\2`\u0402\3\2\2\2b\u0409\3\2\2\2"+
		"d\u0412\3\2\2\2f\u0419\3\2\2\2h\u041f\3\2\2\2j\u0421\3\2\2\2l\u042e\3"+
		"\2\2\2n\u0430\3\2\2\2p\u0434\3\2\2\2r\u048b\3\2\2\2t\u048d\3\2\2\2vz\5"+
		"\4\3\2wy\5\4\3\2xw\3\2\2\2y|\3\2\2\2zx\3\2\2\2z{\3\2\2\2{}\3\2\2\2|z\3"+
		"\2\2\2}~\7\2\2\3~\3\3\2\2\2\177\u0080\5\b\5\2\u0080\u0081\7\3\2\2\u0081"+
		"\5\3\2\2\2\u0082\u0083\5<\37\2\u0083\u0084\7\2\2\3\u0084\7\3\2\2\2\u0085"+
		"\u00bf\5\n\6\2\u0086\u0087\7\u0081\2\2\u0087\u008a\7\u0082\2\2\u0088\u0089"+
		"\t\2\2\2\u0089\u008b\5j\66\2\u008a\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b"+
		"\u008e\3\2\2\2\u008c\u008d\7(\2\2\u008d\u008f\7\u00c9\2\2\u008e\u008c"+
		"\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u00bf\3\2\2\2\u0090\u0091\7\u0081\2"+
		"\2\u0091\u00bf\7\u0083\2\2\u0092\u0093\7o\2\2\u0093\u00bf\5j\66\2\u0094"+
		"\u0095\7p\2\2\u0095\u00bf\5j\66\2\u0096\u0097\7\u0081\2\2\u0097\u00bf"+
		"\7\u0084\2\2\u0098\u0099\7\u0085\2\2\u0099\u00bf\5j\66\2\u009a\u009b\7"+
		"\u009f\2\2\u009b\u009c\5j\66\2\u009c\u009d\7\u00bd\2\2\u009d\u009e\5<"+
		"\37\2\u009e\u00bf\3\2\2\2\u009f\u00a0\7\u0086\2\2\u00a0\u00bf\5<\37\2"+
		"\u00a1\u00a2\7e\2\2\u00a2\u00a6\7g\2\2\u00a3\u00a4\7\u00ba\2\2\u00a4\u00a5"+
		"\7$\2\2\u00a5\u00a7\7&\2\2\u00a6\u00a3\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7"+
		"\u00a8\3\2\2\2\u00a8\u00a9\5j\66\2\u00a9\u00aa\7\4\2\2\u00aa\u00af\5\16"+
		"\b\2\u00ab\u00ac\7\5\2\2\u00ac\u00ae\5\16\b\2\u00ad\u00ab\3\2\2\2\u00ae"+
		"\u00b1\3\2\2\2\u00af\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00b2\3\2"+
		"\2\2\u00b1\u00af\3\2\2\2\u00b2\u00b5\7\6\2\2\u00b3\u00b4\7b\2\2\u00b4"+
		"\u00b6\5\20\t\2\u00b5\u00b3\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00bf\3"+
		"\2\2\2\u00b7\u00b8\7\u008e\2\2\u00b8\u00bb\7g\2\2\u00b9\u00ba\7\u00ba"+
		"\2\2\u00ba\u00bc\7&\2\2\u00bb\u00b9\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc"+
		"\u00bd\3\2\2\2\u00bd\u00bf\5j\66\2\u00be\u0085\3\2\2\2\u00be\u0086\3\2"+
		"\2\2\u00be\u0090\3\2\2\2\u00be\u0092\3\2\2\2\u00be\u0094\3\2\2\2\u00be"+
		"\u0096\3\2\2\2\u00be\u0098\3\2\2\2\u00be\u009a\3\2\2\2\u00be\u009f\3\2"+
		"\2\2\u00be\u00a1\3\2\2\2\u00be\u00b7\3\2\2\2\u00bf\t\3\2\2\2\u00c0\u00c2"+
		"\5\f\7\2\u00c1\u00c0\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3"+
		"\u00c4\5\24\13\2\u00c4\13\3\2\2\2\u00c5\u00c7\7b\2\2\u00c6\u00c8\7c\2"+
		"\2\u00c7\u00c6\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9\u00ce"+
		"\5&\24\2\u00ca\u00cb\7\5\2\2\u00cb\u00cd\5&\24\2\u00cc\u00ca\3\2\2\2\u00cd"+
		"\u00d0\3\2\2\2\u00ce\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\r\3\2\2\2"+
		"\u00d0\u00ce\3\2\2\2\u00d1\u00d2\5l\67\2\u00d2\u00d3\5R*\2\u00d3\17\3"+
		"\2\2\2\u00d4\u00d5\7\4\2\2\u00d5\u00da\5\22\n\2\u00d6\u00d7\7\5\2\2\u00d7"+
		"\u00d9\5\22\n\2\u00d8\u00d6\3\2\2\2\u00d9\u00dc\3\2\2\2\u00da\u00d8\3"+
		"\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dd\3\2\2\2\u00dc\u00da\3\2\2\2\u00dd"+
		"\u00de\7\6\2\2\u00de\21\3\2\2\2\u00df\u00e0\5l\67\2\u00e0\u00e1\7\u00bd"+
		"\2\2\u00e1\u00e2\5<\37\2\u00e2\23\3\2\2\2\u00e3\u00ee\5\26\f\2\u00e4\u00e5"+
		"\7\33\2\2\u00e5\u00e6\7\26\2\2\u00e6\u00eb\5\32\16\2\u00e7\u00e8\7\5\2"+
		"\2\u00e8\u00ea\5\32\16\2\u00e9\u00e7\3\2\2\2\u00ea\u00ed\3\2\2\2\u00eb"+
		"\u00e9\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ef\3\2\2\2\u00ed\u00eb\3\2"+
		"\2\2\u00ee\u00e4\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00f2\3\2\2\2\u00f0"+
		"\u00f1\7\35\2\2\u00f1\u00f3\t\3\2\2\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3"+
		"\2\2\2\u00f3\u00f9\3\2\2\2\u00f4\u00f5\7\36\2\2\u00f5\u00f6\7\37\2\2\u00f6"+
		"\u00f7\5p9\2\u00f7\u00f8\7 \2\2\u00f8\u00fa\3\2\2\2\u00f9\u00f4\3\2\2"+
		"\2\u00f9\u00fa\3\2\2\2\u00fa\25\3\2\2\2\u00fb\u00fc\b\f\1\2\u00fc\u00fd"+
		"\5\30\r\2\u00fd\u010c\3\2\2\2\u00fe\u00ff\f\4\2\2\u00ff\u0101\7\u0091"+
		"\2\2\u0100\u0102\5(\25\2\u0101\u0100\3\2\2\2\u0101\u0102\3\2\2\2\u0102"+
		"\u0103\3\2\2\2\u0103\u010b\5\26\f\5\u0104\u0105\f\3\2\2\u0105\u0107\t"+
		"\4\2\2\u0106\u0108\5(\25\2\u0107\u0106\3\2\2\2\u0107\u0108\3\2\2\2\u0108"+
		"\u0109\3\2\2\2\u0109\u010b\5\26\f\4\u010a\u00fe\3\2\2\2\u010a\u0104\3"+
		"\2\2\2\u010b\u010e\3\2\2\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2\2\2\u010d"+
		"\27\3\2\2\2\u010e\u010c\3\2\2\2\u010f\u0120\5\34\17\2\u0110\u0111\7f\2"+
		"\2\u0111\u0120\5j\66\2\u0112\u0113\7d\2\2\u0113\u0118\5<\37\2\u0114\u0115"+
		"\7\5\2\2\u0115\u0117\5<\37\2\u0116\u0114\3\2\2\2\u0117\u011a\3\2\2\2\u0118"+
		"\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u0120\3\2\2\2\u011a\u0118\3\2"+
		"\2\2\u011b\u011c\7\4\2\2\u011c\u011d\5\24\13\2\u011d\u011e\7\6\2\2\u011e"+
		"\u0120\3\2\2\2\u011f\u010f\3\2\2\2\u011f\u0110\3\2\2\2\u011f\u0112\3\2"+
		"\2\2\u011f\u011b\3\2\2\2\u0120\31\3\2\2\2\u0121\u0123\5<\37\2\u0122\u0124"+
		"\t\5\2\2\u0123\u0122\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0127\3\2\2\2\u0125"+
		"\u0126\7-\2\2\u0126\u0128\t\6\2\2\u0127\u0125\3\2\2\2\u0127\u0128\3\2"+
		"\2\2\u0128\33\3\2\2\2\u0129\u012b\7\f\2\2\u012a\u012c\5(\25\2\u012b\u012a"+
		"\3\2\2\2\u012b\u012c\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u0132\5*\26\2\u012e"+
		"\u012f\7\5\2\2\u012f\u0131\5*\26\2\u0130\u012e\3\2\2\2\u0131\u0134\3\2"+
		"\2\2\u0132\u0130\3\2\2\2\u0132\u0133\3\2\2\2\u0133\u0137\3\2\2\2\u0134"+
		"\u0132\3\2\2\2\u0135\u0136\7m\2\2\u0136\u0138\5,\27\2\u0137\u0135\3\2"+
		"\2\2\u0137\u0138\3\2\2\2\u0138\u0142\3\2\2\2\u0139\u013a\7\r\2\2\u013a"+
		"\u013f\5,\27\2\u013b\u013c\7\5\2\2\u013c\u013e\5,\27\2\u013d\u013b\3\2"+
		"\2\2\u013e\u0141\3\2\2\2\u013f\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140"+
		"\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0142\u0139\3\2\2\2\u0142\u0143\3\2"+
		"\2\2\u0143\u0146\3\2\2\2\u0144\u0145\7\24\2\2\u0145\u0147\5> \2\u0146"+
		"\u0144\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u014b\3\2\2\2\u0148\u0149\7\25"+
		"\2\2\u0149\u014a\7\26\2\2\u014a\u014c\5\36\20\2\u014b\u0148\3\2\2\2\u014b"+
		"\u014c\3\2\2\2\u014c\u014f\3\2\2\2\u014d\u014e\7\34\2\2\u014e\u0150\5"+
		"> \2\u014f\u014d\3\2\2\2\u014f\u0150\3\2\2\2\u0150\35\3\2\2\2\u0151\u0153"+
		"\5(\25\2\u0152\u0151\3\2\2\2\u0152\u0153\3\2\2\2\u0153\u0154\3\2\2\2\u0154"+
		"\u0159\5 \21\2\u0155\u0156\7\5\2\2\u0156\u0158\5 \21\2\u0157\u0155\3\2"+
		"\2\2\u0158\u015b\3\2\2\2\u0159\u0157\3\2\2\2\u0159\u015a\3\2\2\2\u015a"+
		"\37\3\2\2\2\u015b\u0159\3\2\2\2\u015c\u0185\5\"\22\2\u015d\u015e\7\32"+
		"\2\2\u015e\u0167\7\4\2\2\u015f\u0164\5j\66\2\u0160\u0161\7\5\2\2\u0161"+
		"\u0163\5j\66\2\u0162\u0160\3\2\2\2\u0163\u0166\3\2\2\2\u0164\u0162\3\2"+
		"\2\2\u0164\u0165\3\2\2\2\u0165\u0168\3\2\2\2\u0166\u0164\3\2\2\2\u0167"+
		"\u015f\3\2\2\2\u0167\u0168\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u0185\7\6"+
		"\2\2\u016a\u016b\7\31\2\2\u016b\u0174\7\4\2\2\u016c\u0171\5j\66\2\u016d"+
		"\u016e\7\5\2\2\u016e\u0170\5j\66\2\u016f\u016d\3\2\2\2\u0170\u0173\3\2"+
		"\2\2\u0171\u016f\3\2\2\2\u0171\u0172\3\2\2\2\u0172\u0175\3\2\2\2\u0173"+
		"\u0171\3\2\2\2\u0174\u016c\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0176\3\2"+
		"\2\2\u0176\u0185\7\6\2\2\u0177\u0178\7\27\2\2\u0178\u0179\7\30\2\2\u0179"+
		"\u017a\7\4\2\2\u017a\u017f\5$\23\2\u017b\u017c\7\5\2\2\u017c\u017e\5$"+
		"\23\2\u017d\u017b\3\2\2\2\u017e\u0181\3\2\2\2\u017f\u017d\3\2\2\2\u017f"+
		"\u0180\3\2\2\2\u0180\u0182\3\2\2\2\u0181\u017f\3\2\2\2\u0182\u0183\7\6"+
		"\2\2\u0183\u0185\3\2\2\2\u0184\u015c\3\2\2\2\u0184\u015d\3\2\2\2\u0184"+
		"\u016a\3\2\2\2\u0184\u0177\3\2\2\2\u0185!\3\2\2\2\u0186\u018f\7\4\2\2"+
		"\u0187\u018c\5<\37\2\u0188\u0189\7\5\2\2\u0189\u018b\5<\37\2\u018a\u0188"+
		"\3\2\2\2\u018b\u018e\3\2\2\2\u018c\u018a\3\2\2\2\u018c\u018d\3\2\2\2\u018d"+
		"\u0190\3\2\2\2\u018e\u018c\3\2\2\2\u018f\u0187\3\2\2\2\u018f\u0190\3\2"+
		"\2\2\u0190\u0191\3\2\2\2\u0191\u0194\7\6\2\2\u0192\u0194\5<\37\2\u0193"+
		"\u0186\3\2\2\2\u0193\u0192\3\2\2\2\u0194#\3\2\2\2\u0195\u019e\7\4\2\2"+
		"\u0196\u019b\5j\66\2\u0197\u0198\7\5\2\2\u0198\u019a\5j\66\2\u0199\u0197"+
		"\3\2\2\2\u019a\u019d\3\2\2\2\u019b\u0199\3\2\2\2\u019b\u019c\3\2\2\2\u019c"+
		"\u019f\3\2\2\2\u019d\u019b\3\2\2\2\u019e\u0196\3\2\2\2\u019e\u019f\3\2"+
		"\2\2\u019f\u01a0\3\2\2\2\u01a0\u01a3\7\6\2\2\u01a1\u01a3\5j\66\2\u01a2"+
		"\u0195\3\2\2\2\u01a2\u01a1\3\2\2\2\u01a3%\3\2\2\2\u01a4\u01a6\5l\67\2"+
		"\u01a5\u01a7\58\35\2\u01a6\u01a5\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a8"+
		"\3\2\2\2\u01a8\u01a9\7\17\2\2\u01a9\u01aa\7\4\2\2\u01aa\u01ab\5\n\6\2"+
		"\u01ab\u01ac\7\6\2\2\u01ac\'\3\2\2\2\u01ad\u01ae\t\7\2\2\u01ae)\3\2\2"+
		"\2\u01af\u01b4\5<\37\2\u01b0\u01b2\7\17\2\2\u01b1\u01b0\3\2\2\2\u01b1"+
		"\u01b2\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01b5\5l\67\2\u01b4\u01b1\3\2"+
		"\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01bc\3\2\2\2\u01b6\u01b7\5j\66\2\u01b7"+
		"\u01b8\7\7\2\2\u01b8\u01b9\7\u00c5\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01bc"+
		"\7\u00c5\2\2\u01bb\u01af\3\2\2\2\u01bb\u01b6\3\2\2\2\u01bb\u01ba\3\2\2"+
		"\2\u01bc+\3\2\2\2\u01bd\u01be\5:\36\2\u01be-\3\2\2\2\u01bf\u01c1\7R\2"+
		"\2\u01c0\u01bf\3\2\2\2\u01c0\u01c1\3\2\2\2\u01c1\u01cf\3\2\2\2\u01c2\u01c4"+
		"\7S\2\2\u01c3\u01c5\7Q\2\2\u01c4\u01c3\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5"+
		"\u01cf\3\2\2\2\u01c6\u01c8\7T\2\2\u01c7\u01c9\7Q\2\2\u01c8\u01c7\3\2\2"+
		"\2\u01c8\u01c9\3\2\2\2\u01c9\u01cf\3\2\2\2\u01ca\u01cc\7U\2\2\u01cb\u01cd"+
		"\7Q\2\2\u01cc\u01cb\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\u01cf\3\2\2\2\u01ce"+
		"\u01c0\3\2\2\2\u01ce\u01c2\3\2\2\2\u01ce\u01c6\3\2\2\2\u01ce\u01ca\3\2"+
		"\2\2\u01cf/\3\2\2\2\u01d0\u01d1\7X\2\2\u01d1\u01df\5> \2\u01d2\u01d3\7"+
		"W\2\2\u01d3\u01d4\7\4\2\2\u01d4\u01d9\5l\67\2\u01d5\u01d6\7\5\2\2\u01d6"+
		"\u01d8\5l\67\2\u01d7\u01d5\3\2\2\2\u01d8\u01db\3\2\2\2\u01d9\u01d7\3\2"+
		"\2\2\u01d9\u01da\3\2\2\2\u01da\u01dc\3\2\2\2\u01db\u01d9\3\2\2\2\u01dc"+
		"\u01dd\7\6\2\2\u01dd\u01df\3\2\2\2\u01de\u01d0\3\2\2\2\u01de\u01d2\3\2"+
		"\2\2\u01df\61\3\2\2\2\u01e0\u01f8\5\66\34\2\u01e1\u01e2\7\u0096\2\2\u01e2"+
		"\u01e3\5\64\33\2\u01e3\u01e4\7\4\2\2\u01e4\u01e5\5<\37\2\u01e5\u01e7\7"+
		"\6\2\2\u01e6\u01e8\7\u0097\2\2\u01e7\u01e6\3\2\2\2\u01e7\u01e8\3\2\2\2"+
		"\u01e8\u01f6\3\2\2\2\u01e9\u01ea\7\u0098\2\2\u01ea\u01eb\7X\2\2\u01eb"+
		"\u01ec\7\4\2\2\u01ec\u01f1\5<\37\2\u01ed\u01ee\7\5\2\2\u01ee\u01f0\5<"+
		"\37\2\u01ef\u01ed\3\2\2\2\u01f0\u01f3\3\2\2\2\u01f1\u01ef\3\2\2\2\u01f1"+
		"\u01f2\3\2\2\2\u01f2\u01f4\3\2\2\2\u01f3\u01f1\3\2\2\2\u01f4\u01f5\7\6"+
		"\2\2\u01f5\u01f7\3\2\2\2\u01f6\u01e9\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7"+
		"\u01f9\3\2\2\2\u01f8\u01e1\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\63\3\2\2"+
		"\2\u01fa\u01fb\t\b\2\2\u01fb\65\3\2\2\2\u01fc\u0204\5:\36\2\u01fd\u01ff"+
		"\7\17\2\2\u01fe\u01fd\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0200\3\2\2\2"+
		"\u0200\u0202\5l\67\2\u0201\u0203\58\35\2\u0202\u0201\3\2\2\2\u0202\u0203"+
		"\3\2\2\2\u0203\u0205\3\2\2\2\u0204\u01fe\3\2\2\2\u0204\u0205\3\2\2\2\u0205"+
		"\67\3\2\2\2\u0206\u0207\7\4\2\2\u0207\u020c\5l\67\2\u0208\u0209\7\5\2"+
		"\2\u0209\u020b\5l\67\2\u020a\u0208\3\2\2\2\u020b\u020e\3\2\2\2\u020c\u020a"+
		"\3\2\2\2\u020c\u020d\3\2\2\2\u020d\u020f\3\2\2\2\u020e\u020c\3\2\2\2\u020f"+
		"\u0210\7\6\2\2\u02109\3\2\2\2\u0211\u022a\5j\66\2\u0212\u0213\7\4\2\2"+
		"\u0213\u0214\5\n\6\2\u0214\u0215\7\6\2\2\u0215\u022a\3\2\2\2\u0216\u0217"+
		"\7\u009b\2\2\u0217\u0218\7\4\2\2\u0218\u021d\5<\37\2\u0219\u021a\7\5\2"+
		"\2\u021a\u021c\5<\37\2\u021b\u0219\3\2\2\2\u021c\u021f\3\2\2\2\u021d\u021b"+
		"\3\2\2\2\u021d\u021e\3\2\2\2\u021e\u0220\3\2\2\2\u021f\u021d\3\2\2\2\u0220"+
		"\u0223\7\6\2\2\u0221\u0222\7b\2\2\u0222\u0224\7\u009c\2\2\u0223\u0221"+
		"\3\2\2\2\u0223\u0224\3\2\2\2\u0224\u022a\3\2\2\2\u0225\u0226\7\4\2\2\u0226"+
		"\u0227\5,\27\2\u0227\u0228\7\6\2\2\u0228\u022a\3\2\2\2\u0229\u0211\3\2"+
		"\2\2\u0229\u0212\3\2\2\2\u0229\u0216\3\2\2\2\u0229\u0225\3\2\2\2\u022a"+
		";\3\2\2\2\u022b\u022c\5> \2\u022c=\3\2\2\2\u022d\u022e\b \1\2\u022e\u0232"+
		"\5@!\2\u022f\u0230\7$\2\2\u0230\u0232\5> \5\u0231\u022d\3\2\2\2\u0231"+
		"\u022f\3\2\2\2\u0232\u023b\3\2\2\2\u0233\u0234\f\4\2\2\u0234\u0235\7\""+
		"\2\2\u0235\u023a\5> \5\u0236\u0237\f\3\2\2\u0237\u0238\7!\2\2\u0238\u023a"+
		"\5> \4\u0239\u0233\3\2\2\2\u0239\u0236\3\2\2\2\u023a\u023d\3\2\2\2\u023b"+
		"\u0239\3\2\2\2\u023b\u023c\3\2\2\2\u023c?\3\2\2\2\u023d\u023b\3\2\2\2"+
		"\u023e\u0240\5D#\2\u023f\u0241\5B\"\2\u0240\u023f\3\2\2\2\u0240\u0241"+
		"\3\2\2\2\u0241A\3\2\2\2\u0242\u0243\5J&\2\u0243\u0244\5D#\2\u0244\u027a"+
		"\3\2\2\2\u0245\u0247\7$\2\2\u0246\u0245\3\2\2\2\u0246\u0247\3\2\2\2\u0247"+
		"\u0248\3\2\2\2\u0248\u0249\7\'\2\2\u0249\u024a\5D#\2\u024a\u024b\7\"\2"+
		"\2\u024b\u024c\5D#\2\u024c\u027a\3\2\2\2\u024d\u024f\7$\2\2\u024e\u024d"+
		"\3\2\2\2\u024e\u024f\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0251\7#\2\2\u0251"+
		"\u0252\7\4\2\2\u0252\u0257\5<\37\2\u0253\u0254\7\5\2\2\u0254\u0256\5<"+
		"\37\2\u0255\u0253\3\2\2\2\u0256\u0259\3\2\2\2\u0257\u0255\3\2\2\2\u0257"+
		"\u0258\3\2\2\2\u0258\u025a\3\2\2\2\u0259\u0257\3\2\2\2\u025a\u025b\7\6"+
		"\2\2\u025b\u027a\3\2\2\2\u025c\u025e\7$\2\2\u025d\u025c\3\2\2\2\u025d"+
		"\u025e\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u0260\7#\2\2\u0260\u0261\7\4"+
		"\2\2\u0261\u0262\5\n\6\2\u0262\u0263\7\6\2\2\u0263\u027a\3\2\2\2\u0264"+
		"\u0266\7$\2\2\u0265\u0264\3\2\2\2\u0265\u0266\3\2\2\2\u0266\u0267\3\2"+
		"\2\2\u0267\u0268\7(\2\2\u0268\u026b\5D#\2\u0269\u026a\7\60\2\2\u026a\u026c"+
		"\5D#\2\u026b\u0269\3\2\2\2\u026b\u026c\3\2\2\2\u026c\u027a\3\2\2\2\u026d"+
		"\u026f\7)\2\2\u026e\u0270\7$\2\2\u026f\u026e\3\2\2\2\u026f\u0270\3\2\2"+
		"\2\u0270\u0271\3\2\2\2\u0271\u027a\7*\2\2\u0272\u0274\7)\2\2\u0273\u0275"+
		"\7$\2\2\u0274\u0273\3\2\2\2\u0274\u0275\3\2\2\2\u0275\u0276\3\2\2\2\u0276"+
		"\u0277\7\23\2\2\u0277\u0278\7\r\2\2\u0278\u027a\5D#\2\u0279\u0242\3\2"+
		"\2\2\u0279\u0246\3\2\2\2\u0279\u024e\3\2\2\2\u0279\u025d\3\2\2\2\u0279"+
		"\u0265\3\2\2\2\u0279\u026d\3\2\2\2\u0279\u0272\3\2\2\2\u027aC\3\2\2\2"+
		"\u027b\u027c\b#\1\2\u027c\u0280\5F$\2\u027d\u027e\t\t\2\2\u027e\u0280"+
		"\5D#\6\u027f\u027b\3\2\2\2\u027f\u027d\3\2\2\2\u0280\u028f\3\2\2\2\u0281"+
		"\u0282\f\5\2\2\u0282\u0283\t\n\2\2\u0283\u028e\5D#\6\u0284\u0285\f\4\2"+
		"\2\u0285\u0286\t\t\2\2\u0286\u028e\5D#\5\u0287\u0288\f\3\2\2\u0288\u0289"+
		"\7\u00c8\2\2\u0289\u028e\5D#\4\u028a\u028b\f\7\2\2\u028b\u028c\7\37\2"+
		"\2\u028c\u028e\5H%\2\u028d\u0281\3\2\2\2\u028d\u0284\3\2\2\2\u028d\u0287"+
		"\3\2\2\2\u028d\u028a\3\2\2\2\u028e\u0291\3\2\2\2\u028f\u028d\3\2\2\2\u028f"+
		"\u0290\3\2\2\2\u0290E\3\2\2\2\u0291\u028f\3\2\2\2\u0292\u0293\b$\1\2\u0293"+
		"\u035b\7*\2\2\u0294\u035b\5N(\2\u0295\u0296\5l\67\2\u0296\u0297\7\u00c9"+
		"\2\2\u0297\u035b\3\2\2\2\u0298\u035b\5p9\2\u0299\u035b\5L\'\2\u029a\u035b"+
		"\7\u00c9\2\2\u029b\u035b\7\u00ca\2\2\u029c\u029d\7\64\2\2\u029d\u029e"+
		"\7\4\2\2\u029e\u029f\5D#\2\u029f\u02a0\7#\2\2\u02a0\u02a1\5D#\2\u02a1"+
		"\u02a2\7\6\2\2\u02a2\u035b\3\2\2\2\u02a3\u02a4\7\4\2\2\u02a4\u02a7\5<"+
		"\37\2\u02a5\u02a6\7\5\2\2\u02a6\u02a8\5<\37\2\u02a7\u02a5\3\2\2\2\u02a8"+
		"\u02a9\3\2\2\2\u02a9\u02a7\3\2\2\2\u02a9\u02aa\3\2\2\2\u02aa\u02ab\3\2"+
		"\2\2\u02ab\u02ac\7\6\2\2\u02ac\u035b\3\2\2\2\u02ad\u02ae\7a\2\2\u02ae"+
		"\u02af\7\4\2\2\u02af\u02b4\5<\37\2\u02b0\u02b1\7\5\2\2\u02b1\u02b3\5<"+
		"\37\2\u02b2\u02b0\3\2\2\2\u02b3\u02b6\3\2\2\2\u02b4\u02b2\3\2\2\2\u02b4"+
		"\u02b5\3\2\2\2\u02b5\u02b7\3\2\2\2\u02b6\u02b4\3\2\2\2\u02b7\u02b8\7\6"+
		"\2\2\u02b8\u035b\3\2\2\2\u02b9\u02ba\5j\66\2\u02ba\u02bb\7\4\2\2\u02bb"+
		"\u02bc\7\u00c5\2\2\u02bc\u02be\7\6\2\2\u02bd\u02bf\5Z.\2\u02be\u02bd\3"+
		"\2\2\2\u02be\u02bf\3\2\2\2\u02bf\u035b\3\2\2\2\u02c0\u02c1\5j\66\2\u02c1"+
		"\u02cd\7\4\2\2\u02c2\u02c4\5(\25\2\u02c3\u02c2\3\2\2\2\u02c3\u02c4\3\2"+
		"\2\2\u02c4\u02c5\3\2\2\2\u02c5\u02ca\5<\37\2\u02c6\u02c7\7\5\2\2\u02c7"+
		"\u02c9\5<\37\2\u02c8\u02c6\3\2\2\2\u02c9\u02cc\3\2\2\2\u02ca\u02c8\3\2"+
		"\2\2\u02ca\u02cb\3\2\2\2\u02cb\u02ce\3\2\2\2\u02cc\u02ca\3\2\2\2\u02cd"+
		"\u02c3\3\2\2\2\u02cd\u02ce\3\2\2\2\u02ce\u02cf\3\2\2\2\u02cf\u02d1\7\6"+
		"\2\2\u02d0\u02d2\5Z.\2\u02d1\u02d0\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2\u035b"+
		"\3\2\2\2\u02d3\u02d4\5l\67\2\u02d4\u02d5\7\b\2\2\u02d5\u02d6\5<\37\2\u02d6"+
		"\u035b\3\2\2\2\u02d7\u02d8\7\4\2\2\u02d8\u02dd\5l\67\2\u02d9\u02da\7\5"+
		"\2\2\u02da\u02dc\5l\67\2\u02db\u02d9\3\2\2\2\u02dc\u02df\3\2\2\2\u02dd"+
		"\u02db\3\2\2\2\u02dd\u02de\3\2\2\2\u02de\u02e0\3\2\2\2\u02df\u02dd\3\2"+
		"\2\2\u02e0\u02e1\7\6\2\2\u02e1\u02e2\7\b\2\2\u02e2\u02e3\5<\37\2\u02e3"+
		"\u035b\3\2\2\2\u02e4\u02e5\7\4\2\2\u02e5\u02e6\5\n\6\2\u02e6\u02e7\7\6"+
		"\2\2\u02e7\u035b\3\2\2\2\u02e8\u02e9\7&\2\2\u02e9\u02ea\7\4\2\2\u02ea"+
		"\u02eb\5\n\6\2\u02eb\u02ec\7\6\2\2\u02ec\u035b\3\2\2\2\u02ed\u02ee\7J"+
		"\2\2\u02ee\u02f0\5D#\2\u02ef\u02f1\5X-\2\u02f0\u02ef\3\2\2\2\u02f1\u02f2"+
		"\3\2\2\2\u02f2\u02f0\3\2\2\2\u02f2\u02f3\3\2\2\2\u02f3\u02f6\3\2\2\2\u02f4"+
		"\u02f5\7M\2\2\u02f5\u02f7\5<\37\2\u02f6\u02f4\3\2\2\2\u02f6\u02f7\3\2"+
		"\2\2\u02f7\u02f8\3\2\2\2\u02f8\u02f9\7N\2\2\u02f9\u035b\3\2\2\2\u02fa"+
		"\u02fc\7J\2\2\u02fb\u02fd\5X-\2\u02fc\u02fb\3\2\2\2\u02fd\u02fe\3\2\2"+
		"\2\u02fe\u02fc\3\2\2\2\u02fe\u02ff\3\2\2\2\u02ff\u0302\3\2\2\2\u0300\u0301"+
		"\7M\2\2\u0301\u0303\5<\37\2\u0302\u0300\3\2\2\2\u0302\u0303\3\2\2\2\u0303"+
		"\u0304\3\2\2\2\u0304\u0305\7N\2\2\u0305\u035b\3\2\2\2\u0306\u0307\7\177"+
		"\2\2\u0307\u0308\7\4\2\2\u0308\u0309\5<\37\2\u0309\u030a\7\17\2\2\u030a"+
		"\u030b\5R*\2\u030b\u030c\7\6\2\2\u030c\u035b\3\2\2\2\u030d\u030e\7\u0080"+
		"\2\2\u030e\u030f\7\4\2\2\u030f\u0310\5<\37\2\u0310\u0311\7\17\2\2\u0311"+
		"\u0312\5R*\2\u0312\u0313\7\6\2\2\u0313\u035b\3\2\2\2\u0314\u0315\7\u009d"+
		"\2\2\u0315\u031e\7\t\2\2\u0316\u031b\5<\37\2\u0317\u0318\7\5\2\2\u0318"+
		"\u031a\5<\37\2\u0319\u0317\3\2\2\2\u031a\u031d\3\2\2\2\u031b\u0319\3\2"+
		"\2\2\u031b\u031c\3\2\2\2\u031c\u031f\3\2\2\2\u031d\u031b\3\2\2\2\u031e"+
		"\u0316\3\2\2\2\u031e\u031f\3\2\2\2\u031f\u0320\3\2\2\2\u0320\u035b\7\n"+
		"\2\2\u0321\u035b\5l\67\2\u0322\u035b\7D\2\2\u0323\u0327\7E\2\2\u0324\u0325"+
		"\7\4\2\2\u0325\u0326\7\u00cb\2\2\u0326\u0328\7\6\2\2\u0327\u0324\3\2\2"+
		"\2\u0327\u0328\3\2\2\2\u0328\u035b\3\2\2\2\u0329\u032d\7F\2\2\u032a\u032b"+
		"\7\4\2\2\u032b\u032c\7\u00cb\2\2\u032c\u032e\7\6\2\2\u032d\u032a\3\2\2"+
		"\2\u032d\u032e\3\2\2\2\u032e\u035b\3\2\2\2\u032f\u0333\7G\2\2\u0330\u0331"+
		"\7\4\2\2\u0331\u0332\7\u00cb\2\2\u0332\u0334\7\6\2\2\u0333\u0330\3\2\2"+
		"\2\u0333\u0334\3\2\2\2\u0334\u035b\3\2\2\2\u0335\u0339\7H\2\2\u0336\u0337"+
		"\7\4\2\2\u0337\u0338\7\u00cb\2\2\u0338\u033a\7\6\2\2\u0339\u0336\3\2\2"+
		"\2\u0339\u033a\3\2\2\2\u033a\u035b\3\2\2\2\u033b\u033c\7\63\2\2\u033c"+
		"\u033d\7\4\2\2\u033d\u033e\5D#\2\u033e\u033f\7\r\2\2\u033f\u0342\5D#\2"+
		"\u0340\u0341\7\65\2\2\u0341\u0343\5D#\2\u0342\u0340\3\2\2\2\u0342\u0343"+
		"\3\2\2\2\u0343\u0344\3\2\2\2\u0344\u0345\7\6\2\2\u0345\u035b\3\2\2\2\u0346"+
		"\u0347\7\u00b5\2\2\u0347\u0348\7\4\2\2\u0348\u034b\5D#\2\u0349\u034a\7"+
		"\5\2\2\u034a\u034c\5t;\2\u034b\u0349\3\2\2\2\u034b\u034c\3\2\2\2\u034c"+
		"\u034d\3\2\2\2\u034d\u034e\7\6\2\2\u034e\u035b\3\2\2\2\u034f\u0350\7I"+
		"\2\2\u0350\u0351\7\4\2\2\u0351\u0352\5l\67\2\u0352\u0353\7\r\2\2\u0353"+
		"\u0354\5D#\2\u0354\u0355\7\6\2\2\u0355\u035b\3\2\2\2\u0356\u0357\7\4\2"+
		"\2\u0357\u0358\5<\37\2\u0358\u0359\7\6\2\2\u0359\u035b\3\2\2\2\u035a\u0292"+
		"\3\2\2\2\u035a\u0294\3\2\2\2\u035a\u0295\3\2\2\2\u035a\u0298\3\2\2\2\u035a"+
		"\u0299\3\2\2\2\u035a\u029a\3\2\2\2\u035a\u029b\3\2\2\2\u035a\u029c\3\2"+
		"\2\2\u035a\u02a3\3\2\2\2\u035a\u02ad\3\2\2\2\u035a\u02b9\3\2\2\2\u035a"+
		"\u02c0\3\2\2\2\u035a\u02d3\3\2\2\2\u035a\u02d7\3\2\2\2\u035a\u02e4\3\2"+
		"\2\2\u035a\u02e8\3\2\2\2\u035a\u02ed\3\2\2\2\u035a\u02fa\3\2\2\2\u035a"+
		"\u0306\3\2\2\2\u035a\u030d\3\2\2\2\u035a\u0314\3\2\2\2\u035a\u0321\3\2"+
		"\2\2\u035a\u0322\3\2\2\2\u035a\u0323\3\2\2\2\u035a\u0329\3\2\2\2\u035a"+
		"\u032f\3\2\2\2\u035a\u0335\3\2\2\2\u035a\u033b\3\2\2\2\u035a\u0346\3\2"+
		"\2\2\u035a\u034f\3\2\2\2\u035a\u0356\3\2\2\2\u035b\u0366\3\2\2\2\u035c"+
		"\u035d\f\16\2\2\u035d\u035e\7\t\2\2\u035e\u035f\5D#\2\u035f\u0360\7\n"+
		"\2\2\u0360\u0365\3\2\2\2\u0361\u0362\f\f\2\2\u0362\u0363\7\7\2\2\u0363"+
		"\u0365\5l\67\2\u0364\u035c\3\2\2\2\u0364\u0361\3\2\2\2\u0365\u0368\3\2"+
		"\2\2\u0366\u0364\3\2\2\2\u0366\u0367\3\2\2\2\u0367G\3\2\2\2\u0368\u0366"+
		"\3\2\2\2\u0369\u036a\7:\2\2\u036a\u036b\7C\2\2\u036b\u0370\5N(\2\u036c"+
		"\u036d\7:\2\2\u036d\u036e\7C\2\2\u036e\u0370\7\u00c9\2\2\u036f\u0369\3"+
		"\2\2\2\u036f\u036c\3\2\2\2\u0370I\3\2\2\2\u0371\u0372\t\13\2\2\u0372K"+
		"\3\2\2\2\u0373\u0374\t\f\2\2\u0374M\3\2\2\2\u0375\u0377\7<\2\2\u0376\u0378"+
		"\t\t\2\2\u0377\u0376\3\2\2\2\u0377\u0378\3\2\2\2\u0378\u0379\3\2\2\2\u0379"+
		"\u037a\7\u00c9\2\2\u037a\u037d\5P)\2\u037b\u037c\7\u0092\2\2\u037c\u037e"+
		"\5P)\2\u037d\u037b\3\2\2\2\u037d\u037e\3\2\2\2\u037eO\3\2\2\2\u037f\u0380"+
		"\t\r\2\2\u0380Q\3\2\2\2\u0381\u0382\b*\1\2\u0382\u0383\7\u009d\2\2\u0383"+
		"\u0384\7\u00bf\2\2\u0384\u0385\5R*\2\u0385\u0386\7\u00c1\2\2\u0386\u03ac"+
		"\3\2\2\2\u0387\u0388\7\u009e\2\2\u0388\u0389\7\u00bf\2\2\u0389\u038a\5"+
		"R*\2\u038a\u038b\7\5\2\2\u038b\u038c\5R*\2\u038c\u038d\7\u00c1\2\2\u038d"+
		"\u03ac\3\2\2\2\u038e\u038f\7a\2\2\u038f\u0390\7\4\2\2\u0390\u0391\5l\67"+
		"\2\u0391\u0398\5R*\2\u0392\u0393\7\5\2\2\u0393\u0394\5l\67\2\u0394\u0395"+
		"\5R*\2\u0395\u0397\3\2\2\2\u0396\u0392\3\2\2\2\u0397\u039a\3\2\2\2\u0398"+
		"\u0396\3\2\2\2\u0398\u0399\3\2\2\2\u0399\u039b\3\2\2\2\u039a\u0398\3\2"+
		"\2\2\u039b\u039c\7\6\2\2\u039c\u03ac\3\2\2\2\u039d\u03a9\5V,\2\u039e\u039f"+
		"\7\4\2\2\u039f\u03a4\5T+\2\u03a0\u03a1\7\5\2\2\u03a1\u03a3\5T+\2\u03a2"+
		"\u03a0\3\2\2\2\u03a3\u03a6\3\2\2\2\u03a4\u03a2\3\2\2\2\u03a4\u03a5\3\2"+
		"\2\2\u03a5\u03a7\3\2\2\2\u03a6\u03a4\3\2\2\2\u03a7\u03a8\7\6\2\2\u03a8"+
		"\u03aa\3\2\2\2\u03a9\u039e\3\2\2\2\u03a9\u03aa\3\2\2\2\u03aa\u03ac\3\2"+
		"\2\2\u03ab\u0381\3\2\2\2\u03ab\u0387\3\2\2\2\u03ab\u038e\3\2\2\2\u03ab"+
		"\u039d\3\2\2\2\u03ac\u03b1\3\2\2\2\u03ad\u03ae\f\7\2\2\u03ae\u03b0\7\u009d"+
		"\2\2\u03af\u03ad\3\2\2\2\u03b0\u03b3\3\2\2\2\u03b1\u03af\3\2\2\2\u03b1"+
		"\u03b2\3\2\2\2\u03b2S\3\2\2\2\u03b3\u03b1\3\2\2\2\u03b4\u03b7\7\u00cb"+
		"\2\2\u03b5\u03b7\5R*\2\u03b6\u03b4\3\2\2\2\u03b6\u03b5\3\2\2\2\u03b7U"+
		"\3\2\2\2\u03b8\u03bc\7\u00d1\2\2\u03b9\u03bc\7\u00d2\2\2\u03ba\u03bc\5"+
		"l\67\2\u03bb\u03b8\3\2\2\2\u03bb\u03b9\3\2\2\2\u03bb\u03ba\3\2\2\2\u03bc"+
		"W\3\2\2\2\u03bd\u03be\7K\2\2\u03be\u03bf\5<\37\2\u03bf\u03c0\7L\2\2\u03c0"+
		"\u03c1\5<\37\2\u03c1Y\3\2\2\2\u03c2\u03c3\7Y\2\2\u03c3\u03ce\7\4\2\2\u03c4"+
		"\u03c5\7Z\2\2\u03c5\u03c6\7\26\2\2\u03c6\u03cb\5<\37\2\u03c7\u03c8\7\5"+
		"\2\2\u03c8\u03ca\5<\37\2\u03c9\u03c7\3\2\2\2\u03ca\u03cd\3\2\2\2\u03cb"+
		"\u03c9\3\2\2\2\u03cb\u03cc\3\2\2\2\u03cc\u03cf\3\2\2\2\u03cd\u03cb\3\2"+
		"\2\2\u03ce\u03c4\3\2\2\2\u03ce\u03cf\3\2\2\2\u03cf\u03da\3\2\2\2\u03d0"+
		"\u03d1\7\33\2\2\u03d1\u03d2\7\26\2\2\u03d2\u03d7\5\32\16\2\u03d3\u03d4"+
		"\7\5\2\2\u03d4\u03d6\5\32\16\2\u03d5\u03d3\3\2\2\2\u03d6\u03d9\3\2\2\2"+
		"\u03d7\u03d5\3\2\2\2\u03d7\u03d8\3\2\2\2\u03d8\u03db\3\2\2\2\u03d9\u03d7"+
		"\3\2\2\2\u03da\u03d0\3\2\2\2\u03da\u03db\3\2\2\2\u03db\u03dd\3\2\2\2\u03dc"+
		"\u03de\5\\/\2\u03dd\u03dc\3\2\2\2\u03dd\u03de\3\2\2\2\u03de\u03df\3\2"+
		"\2\2\u03df\u03e0\7\6\2\2\u03e0[\3\2\2\2\u03e1\u03e2\7[\2\2\u03e2\u03f2"+
		"\5^\60\2\u03e3\u03e4\7\\\2\2\u03e4\u03f2\5^\60\2\u03e5\u03e6\7[\2\2\u03e6"+
		"\u03e7\7\'\2\2\u03e7\u03e8\5^\60\2\u03e8\u03e9\7\"\2\2\u03e9\u03ea\5^"+
		"\60\2\u03ea\u03f2\3\2\2\2\u03eb\u03ec\7\\\2\2\u03ec\u03ed\7\'\2\2\u03ed"+
		"\u03ee\5^\60\2\u03ee\u03ef\7\"\2\2\u03ef\u03f0\5^\60\2\u03f0\u03f2\3\2"+
		"\2\2\u03f1\u03e1\3\2\2\2\u03f1\u03e3\3\2\2\2\u03f1\u03e5\3\2\2\2\u03f1"+
		"\u03eb\3\2\2\2\u03f2]\3\2\2\2\u03f3\u03f4\7]\2\2\u03f4\u03fd\7^\2\2\u03f5"+
		"\u03f6\7]\2\2\u03f6\u03fd\7_\2\2\u03f7\u03f8\7`\2\2\u03f8\u03fd\7a\2\2"+
		"\u03f9\u03fa\5<\37\2\u03fa\u03fb\t\16\2\2\u03fb\u03fd\3\2\2\2\u03fc\u03f3"+
		"\3\2\2\2\u03fc\u03f5\3\2\2\2\u03fc\u03f7\3\2\2\2\u03fc\u03f9\3\2\2\2\u03fd"+
		"_\3\2\2\2\u03fe\u03ff\7x\2\2\u03ff\u0403\t\17\2\2\u0400\u0401\7y\2\2\u0401"+
		"\u0403\t\20\2\2\u0402\u03fe\3\2\2\2\u0402\u0400\3\2\2\2\u0403a\3\2\2\2"+
		"\u0404\u0405\7\u00a8\2\2\u0405\u0406\7\u00a9\2\2\u0406\u040a\5d\63\2\u0407"+
		"\u0408\7\u00ae\2\2\u0408\u040a\t\21\2\2\u0409\u0404\3\2\2\2\u0409\u0407"+
		"\3\2\2\2\u040ac\3\2\2\2\u040b\u040c\7\u00ae\2\2\u040c\u0413\7\u00ad\2"+
		"\2\u040d\u040e\7\u00ae\2\2\u040e\u0413\7\u00ac\2\2\u040f\u0410\7\u00ab"+
		"\2\2\u0410\u0413\7\u00ae\2\2\u0411\u0413\7\u00aa\2\2\u0412\u040b\3\2\2"+
		"\2\u0412\u040d\3\2\2\2\u0412\u040f\3\2\2\2\u0412\u0411\3\2\2\2\u0413e"+
		"\3\2\2\2\u0414\u041a\5<\37\2\u0415\u0416\5l\67\2\u0416\u0417\7\13\2\2"+
		"\u0417\u0418\5<\37\2\u0418\u041a\3\2\2\2\u0419\u0414\3\2\2\2\u0419\u0415"+
		"\3\2\2\2\u041ag\3\2\2\2\u041b\u0420\7\f\2\2\u041c\u0420\7l\2\2\u041d\u0420"+
		"\7k\2\2\u041e\u0420\5l\67\2\u041f\u041b\3\2\2\2\u041f\u041c\3\2\2\2\u041f"+
		"\u041d\3\2\2\2\u041f\u041e\3\2\2\2\u0420i\3\2\2\2\u0421\u0426\5l\67\2"+
		"\u0422\u0423\7\7\2\2\u0423\u0425\5l\67\2\u0424\u0422\3\2\2\2\u0425\u0428"+
		"\3\2\2\2\u0426\u0424\3\2\2\2\u0426\u0427\3\2\2\2\u0427k\3\2\2\2\u0428"+
		"\u0426\3\2\2\2\u0429\u042f\7\u00cd\2\2\u042a\u042f\5n8\2\u042b\u042f\5"+
		"r:\2\u042c\u042f\7\u00d0\2\2\u042d\u042f\7\u00ce\2\2\u042e\u0429\3\2\2"+
		"\2\u042e\u042a\3\2\2\2\u042e\u042b\3\2\2\2\u042e\u042c\3\2\2\2\u042e\u042d"+
		"\3\2\2\2\u042fm\3\2\2\2\u0430\u0431\7\u00cf\2\2\u0431o\3\2\2\2\u0432\u0435"+
		"\7\u00cc\2\2\u0433\u0435\7\u00cb\2\2\u0434\u0432\3\2\2\2\u0434\u0433\3"+
		"\2\2\2\u0435q\3\2\2\2\u0436\u048c\7\u0081\2\2\u0437\u048c\7\u0082\2\2"+
		"\u0438\u048c\7\u0089\2\2\u0439\u048c\7\u008a\2\2\u043a\u048c\7\u008c\2"+
		"\2\u043b\u048c\7\u008d\2\2\u043c\u048c\7\u0087\2\2\u043d\u048c\7\u0088"+
		"\2\2\u043e\u048c\7\u00a1\2\2\u043f\u048c\7\16\2\2\u0440\u048c\7Y\2\2\u0441"+
		"\u048c\7Z\2\2\u0442\u048c\7[\2\2\u0443\u048c\7\\\2\2\u0444\u048c\7^\2"+
		"\2\u0445\u048c\7_\2\2\u0446\u048c\7`\2\2\u0447\u048c\7a\2\2\u0448\u048c"+
		"\7\u009e\2\2\u0449\u048c\7\u009d\2\2\u044a\u048c\7\66\2\2\u044b\u048c"+
		"\7\67\2\2\u044c\u048c\78\2\2\u044d\u048c\79\2\2\u044e\u048c\7:\2\2\u044f"+
		"\u048c\7;\2\2\u0450\u048c\7<\2\2\u0451\u048c\7C\2\2\u0452\u048c\7=\2\2"+
		"\u0453\u048c\7>\2\2\u0454\u048c\7?\2\2\u0455\u048c\7@\2\2\u0456\u048c"+
		"\7A\2\2\u0457\u048c\7B\2\2\u0458\u048c\7v\2\2\u0459\u048c\7w\2\2\u045a"+
		"\u048c\7x\2\2\u045b\u048c\7y\2\2\u045c\u048c\7z\2\2\u045d\u048c\7{\2\2"+
		"\u045e\u048c\7|\2\2\u045f\u048c\7}\2\2\u0460\u048c\7\u0096\2\2\u0461\u048c"+
		"\7\u0093\2\2\u0462\u048c\7\u0094\2\2\u0463\u048c\7\u0095\2\2\u0464\u048c"+
		"\7\u008b\2\2\u0465\u048c\7\u0092\2\2\u0466\u048c\7\u0097\2\2\u0467\u048c"+
		"\7\36\2\2\u0468\u048c\7\37\2\2\u0469\u048c\7 \2\2\u046a\u048c\7\u009f"+
		"\2\2\u046b\u048c\7\u00a0\2\2\u046c\u048c\7i\2\2\u046d\u048c\7j\2\2\u046e"+
		"\u048c\7\u00ba\2\2\u046f\u048c\7\u00bb\2\2\u0470\u048c\7\u00bc\2\2\u0471"+
		"\u048c\7~\2\2\u0472\u048c\5t;\2\u0473\u048c\7\64\2\2\u0474\u048c\7%\2"+
		"\2\u0475\u048c\7\u00a2\2\2\u0476\u048c\7\u00a3\2\2\u0477\u048c\7\u00a4"+
		"\2\2\u0478\u048c\7\u00a5\2\2\u0479\u048c\7\u00a6\2\2\u047a\u048c\7\u00a7"+
		"\2\2\u047b\u048c\7\u00a8\2\2\u047c\u048c\7\u00a9\2\2\u047d\u048c\7\u00aa"+
		"\2\2\u047e\u048c\7\u00ab\2\2\u047f\u048c\7\u00ac\2\2\u0480\u048c\7\u00ad"+
		"\2\2\u0481\u048c\7\u00ae\2\2\u0482\u048c\7\u00af\2\2\u0483\u048c\7\u00b0"+
		"\2\2\u0484\u048c\7\u00b1\2\2\u0485\u048c\7q\2\2\u0486\u048c\7r\2\2\u0487"+
		"\u048c\7s\2\2\u0488\u048c\7t\2\2\u0489\u048c\7u\2\2\u048a\u048c\7\63\2"+
		"\2\u048b\u0436\3\2\2\2\u048b\u0437\3\2\2\2\u048b\u0438\3\2\2\2\u048b\u0439"+
		"\3\2\2\2\u048b\u043a\3\2\2\2\u048b\u043b\3\2\2\2\u048b\u043c\3\2\2\2\u048b"+
		"\u043d\3\2\2\2\u048b\u043e\3\2\2\2\u048b\u043f\3\2\2\2\u048b\u0440\3\2"+
		"\2\2\u048b\u0441\3\2\2\2\u048b\u0442\3\2\2\2\u048b\u0443\3\2\2\2\u048b"+
		"\u0444\3\2\2\2\u048b\u0445\3\2\2\2\u048b\u0446\3\2\2\2\u048b\u0447\3\2"+
		"\2\2\u048b\u0448\3\2\2\2\u048b\u0449\3\2\2\2\u048b\u044a\3\2\2\2\u048b"+
		"\u044b\3\2\2\2\u048b\u044c\3\2\2\2\u048b\u044d\3\2\2\2\u048b\u044e\3\2"+
		"\2\2\u048b\u044f\3\2\2\2\u048b\u0450\3\2\2\2\u048b\u0451\3\2\2\2\u048b"+
		"\u0452\3\2\2\2\u048b\u0453\3\2\2\2\u048b\u0454\3\2\2\2\u048b\u0455\3\2"+
		"\2\2\u048b\u0456\3\2\2\2\u048b\u0457\3\2\2\2\u048b\u0458\3\2\2\2\u048b"+
		"\u0459\3\2\2\2\u048b\u045a\3\2\2\2\u048b\u045b\3\2\2\2\u048b\u045c\3\2"+
		"\2\2\u048b\u045d\3\2\2\2\u048b\u045e\3\2\2\2\u048b\u045f\3\2\2\2\u048b"+
		"\u0460\3\2\2\2\u048b\u0461\3\2\2\2\u048b\u0462\3\2\2\2\u048b\u0463\3\2"+
		"\2\2\u048b\u0464\3\2\2\2\u048b\u0465\3\2\2\2\u048b\u0466\3\2\2\2\u048b"+
		"\u0467\3\2\2\2\u048b\u0468\3\2\2\2\u048b\u0469\3\2\2\2\u048b\u046a\3\2"+
		"\2\2\u048b\u046b\3\2\2\2\u048b\u046c\3\2\2\2\u048b\u046d\3\2\2\2\u048b"+
		"\u046e\3\2\2\2\u048b\u046f\3\2\2\2\u048b\u0470\3\2\2\2\u048b\u0471\3\2"+
		"\2\2\u048b\u0472\3\2\2\2\u048b\u0473\3\2\2\2\u048b\u0474\3\2\2\2\u048b"+
		"\u0475\3\2\2\2\u048b\u0476\3\2\2\2\u048b\u0477\3\2\2\2\u048b\u0478\3\2"+
		"\2\2\u048b\u0479\3\2\2\2\u048b\u047a\3\2\2\2\u048b\u047b\3\2\2\2\u048b"+
		"\u047c\3\2\2\2\u048b\u047d\3\2\2\2\u048b\u047e\3\2\2\2\u048b\u047f\3\2"+
		"\2\2\u048b\u0480\3\2\2\2\u048b\u0481\3\2\2\2\u048b\u0482\3\2\2\2\u048b"+
		"\u0483\3\2\2\2\u048b\u0484\3\2\2\2\u048b\u0485\3\2\2\2\u048b\u0486\3\2"+
		"\2\2\u048b\u0487\3\2\2\2\u048b\u0488\3\2\2\2\u048b\u0489\3\2\2\2\u048b"+
		"\u048a\3\2\2\2\u048cs\3\2\2\2\u048d\u048e\t\22\2\2\u048eu\3\2\2\2\u0087"+
		"z\u008a\u008e\u00a6\u00af\u00b5\u00bb\u00be\u00c1\u00c7\u00ce\u00da\u00eb"+
		"\u00ee\u00f2\u00f9\u0101\u0107\u010a\u010c\u0118\u011f\u0123\u0127\u012b"+
		"\u0132\u0137\u013f\u0142\u0146\u014b\u014f\u0152\u0159\u0164\u0167\u0171"+
		"\u0174\u017f\u0184\u018c\u018f\u0193\u019b\u019e\u01a2\u01a6\u01b1\u01b4"+
		"\u01bb\u01c0\u01c4\u01c8\u01cc\u01ce\u01d9\u01de\u01e7\u01f1\u01f6\u01f8"+
		"\u01fe\u0202\u0204\u020c\u021d\u0223\u0229\u0231\u0239\u023b\u0240\u0246"+
		"\u024e\u0257\u025d\u0265\u026b\u026f\u0274\u0279\u027f\u028d\u028f\u02a9"+
		"\u02b4\u02be\u02c3\u02ca\u02cd\u02d1\u02dd\u02f2\u02f6\u02fe\u0302\u031b"+
		"\u031e\u0327\u032d\u0333\u0339\u0342\u034b\u035a\u0364\u0366\u036f\u0377"+
		"\u037d\u0398\u03a4\u03a9\u03ab\u03b1\u03b6\u03bb\u03cb\u03ce\u03d7\u03da"+
		"\u03dd\u03f1\u03fc\u0402\u0409\u0412\u0419\u041f\u0426\u042e\u0434\u048b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}