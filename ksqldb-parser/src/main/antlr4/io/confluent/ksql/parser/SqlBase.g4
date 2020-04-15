/*
 * Copyright 2018 Confluent Inc.
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
    : query                                                                 #queryStatement
    | (LIST | SHOW) PROPERTIES                                              #listProperties
    | (LIST | SHOW) ALL? TOPICS EXTENDED?                                   #listTopics
    | (LIST | SHOW) STREAMS EXTENDED?                                       #listStreams
    | (LIST | SHOW) TABLES EXTENDED?                                        #listTables
    | (LIST | SHOW) FUNCTIONS                                               #listFunctions
    | (LIST | SHOW) (SOURCE | SINK)? CONNECTORS                             #listConnectors
    | (LIST | SHOW) TYPES                                                   #listTypes
    | DESCRIBE EXTENDED? sourceName                                         #showColumns
    | DESCRIBE FUNCTION identifier                                          #describeFunction
    | DESCRIBE CONNECTOR identifier                                         #describeConnector
    | PRINT (identifier| STRING) printClause                                #printTopic
    | (LIST | SHOW) QUERIES EXTENDED?                                       #listQueries
    | TERMINATE identifier                                                  #terminateQuery
    | TERMINATE ALL                                                         #terminateQuery
    | SET STRING EQ STRING                                                  #setProperty
    | UNSET STRING                                                          #unsetProperty
    | CREATE STREAM (IF NOT EXISTS)? sourceName
                (tableElements)?
                (WITH tableProperties)?                                     #createStream
    | CREATE STREAM (IF NOT EXISTS)? sourceName
            (WITH tableProperties)? AS query                                #createStreamAs
    | CREATE TABLE (IF NOT EXISTS)? sourceName
                    (tableElements)?
                    (WITH tableProperties)?                                 #createTable
    | CREATE TABLE (IF NOT EXISTS)? sourceName
            (WITH tableProperties)? AS query                                #createTableAs
    | CREATE (SINK | SOURCE) CONNECTOR identifier WITH tableProperties      #createConnector
    | INSERT INTO sourceName query                                          #insertInto
    | INSERT INTO sourceName (columns)? VALUES values                       #insertValues
    | DROP STREAM (IF EXISTS)? sourceName (DELETE TOPIC)?                   #dropStream
    | DROP TABLE (IF EXISTS)? sourceName (DELETE TOPIC)?                    #dropTable
    | DROP CONNECTOR identifier                                             #dropConnector
    | EXPLAIN  (statement | identifier)                                     #explain
    | CREATE TYPE identifier AS type                                        #registerType
    | DROP TYPE identifier                                                  #dropType
    ;

query
    : SELECT selectItem (',' selectItem)*
      FROM from=relation
      (WINDOW  windowExpression)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (PARTITION BY partitionBy)?
      (HAVING having=booleanExpression)?
      (EMIT resultMaterialization)?
      limitClause?
    ;

resultMaterialization
    : CHANGES
    ;

tableElements
    : '(' tableElement (',' tableElement)* ')'
    ;

tableElement
    : identifier type ((PRIMARY)? KEY)?
    ;

tableProperties
    : '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : (identifier | STRING) EQ literal
    ;

printClause
      : (FROM BEGINNING)? intervalClause? limitClause?
      ;

intervalClause
    : (INTERVAL | SAMPLE) number
    ;

limitClause
    : LIMIT number
    ;

retentionClause
    : ',' RETENTION number windowUnit
    ;

gracePeriodClause
    : ',' GRACE PERIOD number windowUnit
    ;

windowExpression
    : (IDENTIFIER)?
     ( tumblingWindowExpression | hoppingWindowExpression | sessionWindowExpression )
    ;

tumblingWindowExpression
    : TUMBLING '(' SIZE number windowUnit (retentionClause)? (gracePeriodClause)?')'
    ;

hoppingWindowExpression
    : HOPPING '(' SIZE number windowUnit ',' ADVANCE BY number windowUnit (retentionClause)? (gracePeriodClause)?')'
    ;

sessionWindowExpression
    : SESSION '(' number windowUnit (retentionClause)? (gracePeriodClause)?')'
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

partitionBy
    : valueExpression (AS? identifier)?
    ;

groupBy
    : valueExpression (AS? identifier)?
    | valueExpression (',' valueExpression)*
    | '(' (valueExpression (',' valueExpression)*)? ')' (AS? identifier)?
    ;

values
    : '(' (valueExpression (',' valueExpression)*)? ')'
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | identifier '.' ASTERISK       #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=aliasedRelation joinedSource+  #joinRelation
    | aliasedRelation                     #relationDefault
    ;

joinedSource
    : joinType JOIN aliasedRelation joinWindow? joinCriteria
    ;

joinType
    : INNER? #innerJoin
    | FULL OUTER? #outerJoin
    | LEFT OUTER? #leftJoin
    ;

joinWindow
    : WITHIN withinExpression
    ;

withinExpression
    : '(' joinWindowSize ',' joinWindowSize ')' # joinWindowWithBeforeAndAfter
    | joinWindowSize # singleJoinWindow
    ;

joinWindowSize
    : number windowUnit
    ;

joinCriteria
    : ON booleanExpression
    ;

aliasedRelation
    : relationPrimary (AS? sourceName)?
    ;

columns
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : sourceName                                                  #tableName
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
    | NOT? LIKE pattern=valueExpression	(ESCAPE escape=STRING)?   		    #like
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
    : literal                                                                             #literalExpression
    | identifier STRING                                                                   #typeConstructor
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END              #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | MAP '(' (expression ASSIGN expression (',' expression ASSIGN expression)*)? ')'     #mapConstructor
    | STRUCT '(' (identifier ASSIGN expression (',' identifier ASSIGN expression)*)? ')'  #structConstructor
    | identifier '(' ASTERISK ')'                              		                        #functionCall
    | identifier'(' (expression (',' expression)*)? ')' 						                      #functionCall
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | identifier '.' identifier                                                           #qualifiedColumnReference
    | base=primaryExpression STRUCT_FIELD_REF fieldName=identifier                        #dereference
    | '(' expression ')'                                                                  #parenthesizedExpression
    ;

timeZoneSpecifier
    : TIME ZONE STRING    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

type
    : type ARRAY
    | ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | STRUCT '<' (identifier type (',' identifier type)*)? '>'
    | DECIMAL '(' number ',' number ')'
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

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

sourceName
    : identifier
    ;

number
    : DECIMAL_VALUE         #decimalLiteral
    | FLOATING_POINT_VALUE  #floatLiteral
    | INTEGER_VALUE         #integerLiteral
    ;

literal
    : NULL                                                                           #nullLiteral
    | number                                                                         #numericLiteral
    | booleanValue                                                                   #booleanLiteral
    | STRING                                                                         #stringLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | FUNCTION | SESSION
    | STRUCT | MAP | ARRAY | PARTITION
    | INTEGER | DATE | TIME | TIMESTAMP | INTERVAL | ZONE
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | ANALYZE | TYPE | TYPES
    | SET | RESET
    | IF
    | SOURCE | SINK
    | PRIMARY | KEY
    | EMIT
    | CHANGES
    | ESCAPE
    ;

EMIT: 'EMIT';
CHANGES: 'CHANGES';
SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
WITHIN: 'WITHIN';
WINDOW: 'WINDOW';
GROUP: 'GROUP';
BY: 'BY';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
AT: 'AT';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
ESCAPE: 'ESCAPE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
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
TUMBLING: 'TUMBLING';
HOPPING: 'HOPPING';
SIZE: 'SIZE';
ADVANCE: 'ADVANCE';
RETENTION: 'RETENTION';
GRACE: 'GRACE';
PERIOD: 'PERIOD';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
JOIN: 'JOIN';
FULL: 'FULL';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
ON: 'ON';
PARTITION: 'PARTITION';
STRUCT: 'STRUCT';
WITH: 'WITH';
VALUES: 'VALUES';
CREATE: 'CREATE';
TABLE: 'TABLE';
TOPIC: 'TOPIC';
STREAM: 'STREAM';
STREAMS: 'STREAMS';
INSERT: 'INSERT';
DELETE: 'DELETE';
INTO: 'INTO';
DESCRIBE: 'DESCRIBE';
EXTENDED: 'EXTENDED';
PRINT: 'PRINT';
EXPLAIN: 'EXPLAIN';
ANALYZE: 'ANALYZE';
TYPE: 'TYPE';
TYPES: 'TYPES';
CAST: 'CAST';
SHOW: 'SHOW';
LIST: 'LIST';
TABLES: 'TABLES';
TOPICS: 'TOPICS';
QUERY: 'QUERY';
QUERIES: 'QUERIES';
TERMINATE: 'TERMINATE';
LOAD: 'LOAD';
COLUMNS: 'COLUMNS';
COLUMN: 'COLUMN';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
FUNCTION: 'FUNCTION';
DROP: 'DROP';
TO: 'TO';
RENAME: 'RENAME';
ARRAY: 'ARRAY';
MAP: 'MAP';
SET: 'SET';
RESET: 'RESET';
SESSION: 'SESSION';
SAMPLE: 'SAMPLE';
EXPORT: 'EXPORT';
CATALOG: 'CATALOG';
PROPERTIES: 'PROPERTIES';
BEGINNING: 'BEGINNING';
UNSET: 'UNSET';
RUN: 'RUN';
SCRIPT: 'SCRIPT';
DECIMAL: 'DECIMAL';
KEY: 'KEY';
CONNECTOR: 'CONNECTOR';
CONNECTORS: 'CONNECTORS';
SINK: 'SINK';
SOURCE: 'SOURCE';
NAMESPACE: 'NAMESPACE';
MATERIALIZED: 'MATERIALIZED';
VIEW: 'VIEW';
PRIMARY: 'PRIMARY';

IF: 'IF';

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

ASSIGN: ':=';
STRUCT_FIELD_REF: '->';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

FLOATING_POINT_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' )*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' )+
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
