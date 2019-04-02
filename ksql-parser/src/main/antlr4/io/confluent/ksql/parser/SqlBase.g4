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
    : query                                                                 #querystatement
    | (LIST | SHOW) PROPERTIES                                              #listProperties
    | (LIST | SHOW) TOPICS                                                  #listTopics
    | (LIST | SHOW) REGISTERED TOPICS                                       #listRegisteredTopics
    | (LIST | SHOW) STREAMS EXTENDED?                                       #listStreams
    | (LIST | SHOW) TABLES EXTENDED?                                        #listTables
    | (LIST | SHOW) FUNCTIONS                                               #listFunctions
    | DESCRIBE EXTENDED? (qualifiedName | TOPIC qualifiedName)              #showColumns
    | DESCRIBE FUNCTION qualifiedName                                       #describeFunction
    | PRINT (qualifiedName | STRING) printClause                            #printTopic
    | (LIST | SHOW) QUERIES EXTENDED?                                       #listQueries
    | TERMINATE QUERY? qualifiedName                                        #terminateQuery
    | SET STRING EQ STRING                                                  #setProperty
    | UNSET STRING                                                          #unsetProperty
    | REGISTER TOPIC (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)?                                         #registerTopic
    | CREATE (OR REPLACE)? FUNCTION qualifiedName
                ('(' tableElement (',' tableElement)* ')')?
                RETURNS type
                LANGUAGE languageName
                (WITH functionProperties)?
                AS
                udfScript                                                   #createFunction
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
    | INSERT INTO qualifiedName query (PARTITION BY identifier)?            #insertInto
    | DROP FUNCTION (IF EXISTS)? qualifiedName                              #dropFunction
    | DROP TOPIC (IF EXISTS)? qualifiedName                                 #dropTopic
    | DROP STREAM (IF EXISTS)? qualifiedName (DELETE TOPIC)?                #dropStream
    | DROP TABLE (IF EXISTS)? qualifiedName  (DELETE TOPIC)?                #dropTable
    | EXPLAIN  (statement | qualifiedName)                                  #explain
    | RUN SCRIPT STRING                                                     #runScript
    ;

query
    : SELECT selectItem (',' selectItem)*
      FROM from=relation
      (WINDOW  windowExpression)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      limitClause?
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

functionProperties
    : '(' functionProperty (',' functionProperty)* ')'
    ;

functionProperty
    : identifier EQ expression
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

windowExpression
    : (IDENTIFIER)?
     ( tumblingWindowExpression | hoppingWindowExpression | sessionWindowExpression )
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

/*
 * Dropped `namedQuery` as we don't support them.
 */

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=aliasedRelation joinType JOIN right=aliasedRelation joinWindow? joinCriteria #joinRelation
    | aliasedRelation                                                                   #relationDefault
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
    : relationPrimary (AS? identifier)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
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
    | NOT? LIKE pattern=valueExpression									                  #like
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
    | identifier STRING                                                              #typeConstructor
    | number                                                                         #numericLiteral
    | booleanValue                                                                   #booleanLiteral
    | STRING                                                                         #stringLiteral
    | qualifiedName '(' ASTERISK ')'                              		               #functionCall
    | qualifiedName '(' (expression (',' expression)*)? ')' 						             #functionCall
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END         #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                         #searchedCase
    | CAST '(' expression AS type ')'                                                #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                  #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                          #subscript
    | identifier                                                                     #columnReference
    | identifier '.' identifier                                                      #columnReference
    | base=primaryExpression STRUCT_FIELD_REF fieldName=identifier                   #dereference
    | '(' expression ')'                                                             #parenthesizedExpression
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
    | STRUCT '<' identifier type (',' identifier type)* '>'
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

qualifiedName
    : identifier ('.' identifier)*
    ;

languageName
    : identifier
    ;

udfScript
    : UDF_SCRIPT
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | SESSION
    | FUNCTIONS | FUNCTION | LANGUAGE | RETURNS
    | STRUCT | MAP | ARRAY | PARTITION
    | INTEGER | DATE | TIME | TIMESTAMP | INTERVAL | ZONE
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    | EXPLAIN | ANALYZE | TYPE
    | SET | RESET
    | IF
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
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
REGISTER: 'REGISTER';
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
CAST: 'CAST';
SHOW: 'SHOW';
LIST: 'LIST';
TABLES: 'TABLES';
TOPICS: 'TOPICS';
REGISTERED: 'REGISTERED';
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
RETURNS: 'RETURNS';
LANGUAGE: 'LANGUAGE';
REPLACE: 'REPLACE';
HEREDOC: '$$';

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
    | DIGIT+ ('.' DIGIT*)? EXPONENT
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

UDF_SCRIPT
    : HEREDOC ( ~'$'~'$' | ';' | WS)* HEREDOC
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
