/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

grammar SqlBase;

tokens {
    DELIMITER
}

statements
    : (singleStatement)* EOF
    ;

singleStatement
    : statement ';'
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                                 #querystatement
    | (LIST | SHOW) PROPERTIES                                              #listProperties
    | (LIST | SHOW) TOPICS                                                  #listTopics
    | (LIST | SHOW) REGISTERED TOPICS                                       #listRegisteredTopics
    | (LIST | SHOW) STREAMS                                                 #listStreams
    | (LIST | SHOW) TABLES                                                  #listTables
    | DESCRIBE EXTENDED? (qualifiedName | TOPIC qualifiedName)              #showColumns
    | PRINT (qualifiedName | STRING) (FROM BEGINNING)? ((INTERVAL | SAMPLE) number)?   #printTopic
    | (LIST | SHOW) QUERIES                                                 #listQueries
    | TERMINATE QUERY? qualifiedName                                        #terminateQuery
    | SET STRING EQ STRING                                                  #setProperty
    | UNSET STRING                                                          #unsetProperty
    | LOAD expression                                                       #loadProperties
    | REGISTER TOPIC (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)?                                         #registerTopic
    | CREATE STREAM (IF NOT EXISTS)? qualifiedName
                ('(' tableElement (',' tableElement)* ')')?
                (WITH tableProperties)?                                     #createStream
    | CREATE STREAM (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)? AS query
                                       (PARTITION BY identifier)?           #createStreamAs
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
                    ('(' tableElement (',' tableElement)* ')')?
                    (WITH tableProperties)?                                 #createTable
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)? AS query                                #createTableAs
    | DROP TOPIC (IF EXISTS)? qualifiedName                                 #dropTopic
    | DROP STREAM (IF EXISTS)? qualifiedName                                #dropStream
    | DROP TABLE (IF EXISTS)? qualifiedName                                 #dropTable
    | EXPLAIN ANALYZE?
            ('(' explainOption (',' explainOption)* ')')? (statement | qualifiedName)         #explain
    | EXPORT CATALOG TO STRING                                              #exportCatalog
    | RUN SCRIPT STRING                                                     #runScript
    ;

query
    :  queryNoWith
    ;

tableElement
    : identifier type
    ;

tableProperties
    : '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : identifier EQ expression
    ;

queryNoWith:
      queryTerm
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith  ')'                 #subquery
    ;

querySpecification
    : SELECT STREAM? selectItem (',' selectItem)*
      (INTO into=relationPrimary)?
      (FROM from=relation (',' relation)*)?
      (WINDOW  windowExpression)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

windowExpression
    : (IDENTIFIER)?
     ( tumblingWindowExpression | hoppingWindowExpression | sessionWindowExpression)
    ;

tumblingWindowExpression
    : TUMBLING '(' SIZE number windowUnit')'
    ;

hoppingWindowExpression
    : HOPPING '(' SIZE number windowUnit ',' ADVANCE BY number windowUnit ')'
    ;

sessionWindowExpression
    : SESSION '(' number windowUnit ')'
    ;

windowUnit
    : DAY
    | HOUR
    | MINUTE
    | SECOND
    | MILLISECOND
    | DAYS
    | HOURS
    | MINUTES
    | SECONDS
    | MILLISECONDS
    ;

groupBy
    : groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingExpressions                                               #singleGroupingSet
    ;

groupingExpressions
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

groupingSet
    : '(' (qualifiedName (',' qualifiedName)*)? ')'
    | qualifiedName
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;


relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      )                                           #joinRelation
    | aliasedRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;


sampleType
    : BERNOULLI
    | SYSTEM
    | POISSONIZED
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    :
    qualifiedName (WITH tableProperties)?                             #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : predicated                                                   #booleanDefault
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                           #nullLiteral
    | interval                                                                       #intervalLiteral
    | identifier STRING                                                              #typeConstructor
    | number                                                                         #numericLiteral
    | booleanValue                                                                   #booleanLiteral
    | STRING                                                                         #stringLiteral
    | BINARY_LITERAL                                                                 #binaryLiteral
    | POSITION '(' valueExpression IN valueExpression ')'                            #position
    | '(' expression (',' expression)+ ')'                                           #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                       #rowConstructor
    | qualifiedName '(' ASTERISK ')' over?                                           #functionCall
    | qualifiedName '(' (expression (',' expression)*)? ')' over?     #functionCall
    | identifier '->' expression                                                     #lambda
    | '(' identifier (',' identifier)* ')' '->' expression                           #lambda
    | '(' query ')'                                                                  #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                           #exists
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END         #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                         #searchedCase
    | CAST '(' expression AS type ')'                                                #cast
    | TRY_CAST '(' expression AS type ')'                                            #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                  #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                          #subscript
    | identifier                                                                     #columnReference
    | base=primaryExpression '.' fieldName=identifier                                #dereference
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'  #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                            #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                #extract
    | '(' expression ')'                                                             #parenthesizedExpression
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE STRING    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? STRING from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

type
    : type ARRAY
    | ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | ROW '(' identifier type (',' identifier type)* ')'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        windowFrame?
      ')'
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame // expression should be unsignedLiteral
    ;


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ)         #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED)     #explainType
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

privilege
    : SELECT | DELETE | INSERT | identifier
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

quotedIdentifier
    : QUOTED_IDENTIFIER
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS | SESSION
    | ADD
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | MAP | ARRAY
    | TINYINT | SMALLINT | INTEGER | DATE | TIME | TIMESTAMP | INTERVAL | ZONE
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | ANALYZE | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | DISTRIBUTED
    | TABLESAMPLE | SYSTEM | BERNOULLI | POISSONIZED | USE | TO
    | RESCALED | APPROXIMATE | AT | CONFIDENCE
    | SET | RESET
    | VIEW | REPLACE
    | IF | NULLIF | COALESCE
    | TRY
    | normalForm
    | POSITION
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK | WORK | ISOLATION | LEVEL
    | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | ONLY
    | CALL
    | GRANT | REVOKE | PRIVILEGES | PUBLIC | OPTION
    | SUBSTRING
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

SELECT: 'SELECT';
FROM: 'FROM';
ADD: 'ADD';
AS: 'AS';
ALL: 'ALL';
SOME: 'SOME';
ANY: 'ANY';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
WINDOW: 'WINDOW';
GROUP: 'GROUP';
BY: 'BY';
GROUPING: 'GROUPING';
SETS: 'SETS';
CUBE: 'CUBE';
ROLLUP: 'ROLLUP';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
APPROXIMATE: 'APPROXIMATE';
AT: 'AT';
CONFIDENCE: 'CONFIDENCE';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
NO: 'NO';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
FIRST: 'FIRST';
LAST: 'LAST';
ESCAPE: 'ESCAPE';
ASC: 'ASC';
DESC: 'DESC';
SUBSTRING: 'SUBSTRING';
POSITION: 'POSITION';
FOR: 'FOR';
TINYINT: 'TINYINT';
SMALLINT: 'SMALLINT';
INTEGER: 'INTEGER';
DATE: 'DATE';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
INTERVAL: 'INTERVAL';
YEAR: 'YEAR';
MONTH: 'MONTH';
DAY: 'DAY';
HOUR: 'HOUR';
MINUTE: 'MINUTE';
SECOND: 'SECOND';
MILLISECOND: 'MILLISECOND';
YEARS: 'YEARS';
MONTHS: 'MONTHS';
DAYS: 'DAYS';
HOURS: 'HOURS';
MINUTES: 'MINUTES';
SECONDS: 'SECONDS';
MILLISECONDS: 'MILLISECONDS';
ZONE: 'ZONE';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
EXTRACT: 'EXTRACT';
TUMBLING: 'TUMBLING';
HOPPING: 'HOPPING';
SIZE: 'SIZE';
ADVANCE: 'ADVANCE';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
OVER: 'OVER';
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
RECURSIVE: 'RECURSIVE';
VALUES: 'VALUES';
CREATE: 'CREATE';
REGISTER: 'REGISTER';
TABLE: 'TABLE';
TOPIC: 'TOPIC';
STREAM: 'STREAM';
STREAMS: 'STREAMS';
VIEW: 'VIEW';
REPLACE: 'REPLACE';
INSERT: 'INSERT';
DELETE: 'DELETE';
INTO: 'INTO';
CONSTRAINT: 'CONSTRAINT';
DESCRIBE: 'DESCRIBE';
EXTENDED: 'EXTENDED';
PRINT: 'PRINT';
GRANT: 'GRANT';
REVOKE: 'REVOKE';
PRIVILEGES: 'PRIVILEGES';
PUBLIC: 'PUBLIC';
OPTION: 'OPTION';
EXPLAIN: 'EXPLAIN';
ANALYZE: 'ANALYZE';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
GRAPHVIZ: 'GRAPHVIZ';
LOGICAL: 'LOGICAL';
DISTRIBUTED: 'DISTRIBUTED';
TRY: 'TRY';
CAST: 'CAST';
TRY_CAST: 'TRY_CAST';
SHOW: 'SHOW';
LIST: 'LIST';
TABLES: 'TABLES';
TOPICS: 'TOPICS';
REGISTERED: 'REGISTERED';
QUERY: 'QUERY';
QUERIES: 'QUERIES';
TERMINATE: 'TERMINATE';
LOAD: 'LOAD';
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
COLUMN: 'COLUMN';
USE: 'USE';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
DROP: 'DROP';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
TO: 'TO';
SYSTEM: 'SYSTEM';
BERNOULLI: 'BERNOULLI';
POISSONIZED: 'POISSONIZED';
TABLESAMPLE: 'TABLESAMPLE';
RESCALED: 'RESCALED';
STRATIFY: 'STRATIFY';
ALTER: 'ALTER';
RENAME: 'RENAME';
UNNEST: 'UNNEST';
ORDINALITY: 'ORDINALITY';
ARRAY: 'ARRAY';
MAP: 'MAP';
SET: 'SET';
RESET: 'RESET';
SESSION: 'SESSION';
DATA: 'DATA';
START: 'START';
TRANSACTION: 'TRANSACTION';
COMMIT: 'COMMIT';
ROLLBACK: 'ROLLBACK';
WORK: 'WORK';
ISOLATION: 'ISOLATION';
LEVEL: 'LEVEL';
SERIALIZABLE: 'SERIALIZABLE';
REPEATABLE: 'REPEATABLE';
COMMITTED: 'COMMITTED';
UNCOMMITTED: 'UNCOMMITTED';
READ: 'READ';
WRITE: 'WRITE';
ONLY: 'ONLY';
CALL: 'CALL';
PREPARE: 'PREPARE';
DEALLOCATE: 'DEALLOCATE';
EXECUTE: 'EXECUTE';
SAMPLE: 'SAMPLE';
EXPORT: 'EXPORT';
CATALOG: 'CATALOG';
PROPERTIES: 'PROPERTIES';
BEGINNING: 'BEGINNING';
UNSET: 'UNSET';
RUN: 'RUN';
SCRIPT: 'SCRIPT';

NORMALIZE: 'NORMALIZE';
NFD : 'NFD';
NFC : 'NFC';
NFKD : 'NFKD';
NFKC : 'NFKC';

IF: 'IF';
NULLIF: 'NULLIF';
COALESCE: 'COALESCE';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
