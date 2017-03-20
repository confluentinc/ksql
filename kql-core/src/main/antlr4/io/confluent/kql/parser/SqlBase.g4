/*
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

//@header {
//package io.confluent.kql.parser;
//}

tokens {
    DELIMITER
}

statements
    : singleStatement (singleStatement)* EOF
    ;


singleStatement
    : statement ';'
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                            #querystatement
    | SHOW TABLES ((FROM | IN) qualifiedName)? (LIKE pattern=STRING)?  #showTables
    | SHOW STREAMS                                                     #showTopics
    | LIST TOPICS                                                      #listTopics
    | LIST STREAMS                                                     #listStreams
    | LIST TABLES                                                      #listTables
    | DESCRIBE qualifiedName                                           #showColumns
    | PRINT qualifiedName ((INTERVAL | SAMPLE) number)?                #printTopic
    | SHOW QUERIES                                                     #showQueries
    | TERMINATE qualifiedName                                          #terminateQuery
    | SET qualifiedName EQ expression                                  #setProperty
    | LOAD expression                                                  #loadProperties
    | CREATE TOPIC (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)?                                    #createTopic
    | CREATE STREAM (IF NOT EXISTS)? qualifiedName
                '(' tableElement (',' tableElement)* ')'
                (WITH tableProperties)?                                #createStream
    | CREATE STREAM (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)? AS query                           #createStreamAs
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
                    '(' tableElement (',' tableElement)* ')'
                    (WITH tableProperties)?                            #createTable
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
            (WITH tableProperties)? AS query                           #createTableAs
    | DROP TOPIC (IF EXISTS)? qualifiedName                            #dropTable
    | EXPORT CATALOG TO STRING                                         #exportCatalog
    ;

query
    :  with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
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
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      (APPROXIMATE AT confidence=number CONFIDENCE)?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith  ')'                 #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (INTO into=relationPrimary)?
      (FROM from=relation (',' relation)*)?
      (WINDOW  windowExpression)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

windowExpression
    : (IDENTIFIER)?
     ( tumblingWindowExpression | hoppingWindowExpression)
    ;

tumblingWindowExpression
    : TUMBLING '(' SIZE number windowUnit')'
    ;

hoppingWindowExpression
    : HOPPING '(' SIZE number windowUnit ',' ADVANCE BY number windowUnit ')'
    ;

windowUnit
    : DAY
    | HOUR
    | MINUTE
    | SECOND
    | MILLISECOND
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingExpressions                                               #singleGroupingSet
    | ROLLUP '(' (qualifiedName (',' qualifiedName)*)? ')'              #rollup
    | CUBE '(' (qualifiedName (',' qualifiedName)*)? ')'                #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'              #multipleGroupingSets
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

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

//relation
//    : relationPrimary
//    ;

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
    : qualifiedName                                                   #tableName
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
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' over?     #functionCall
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
    | name=CURRENT_DATE                                                              #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?                           #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?                      #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?                              #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?                         #specialDateTimeFunction
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
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

over
    : OVER '('
        (PARTITION BY partition+=expression (',' partition+=expression)*)?
        (ORDER BY sortItem (',' sortItem)*)?
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

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
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

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

nonReserved
    : ADD
    | ANALYZE
    | APPROXIMATE
    | ARRAY
    | AT
    | BERNOULLI
    | CALL
    | CATALOGS
    | COALESCE
    | COLUMN
    | COLUMNS
    | COMMIT
    | COMMITTED
    | CONFIDENCE
    | CURRENT
    | DATA
    | DATE
    | DAY
    | DISTRIBUTED
    | EXPLAIN
    | FOLLOWING
    | FORMAT
    | FUNCTIONS
    | GRANT
    | GRAPHVIZ
    | HOUR
    | IF
    | INTEGER
    | INTERVAL
    | ISOLATION
    | LEVEL
    | LOGICAL
    | MAP
    | MINUTE
    | MONTH
    | NFC
    | NFD
    | NFKC
    | NFKD
    | NO
    | NULLIF
    | ONLY
    | OPTION
    | OVER
    | PARTITION
    | PARTITIONS
    | POISSONIZED
    | POSITION
    | PRECEDING
    | PRIVILEGES
    | PUBLIC
    | RANGE
    | READ
    | REPEATABLE
    | REPLACE
    | RESCALED
    | RESET
    | REVOKE
    | ROLLBACK
    | ROW
    | ROWS
    | SCHEMAS
    | SECOND
    | SERIALIZABLE
    | SESSION
    | SET
    | SHOW
    | SMALLINT
    | START
    | SUBSTRING
    | SYSTEM
    | TABLES
    | TABLESAMPLE
    | TEXT
    | TIME
    | TIMESTAMP
    | TINYINT
    | TO
    | TRANSACTION
    | TRY
    | TYPE
    | UNCOMMITTED
    | USE
    | VIEW
    | WORK
    | WRITE
    | YEAR
    | ZONE
    ;

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

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

// BEGIN CASE-INSENSITIVE TOKENS

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\'' { setText(getText().toUpperCase()); }
    ;

TIME_WITH_TIME_ZONE
    : TIME WS WITH WS TIME WS ZONE { setText(getText().toUpperCase()); }
    ;

TIMESTAMP_WITH_TIME_ZONE
    : TIMESTAMP WS WITH WS TIME WS ZONE { setText(getText().toUpperCase()); }
    ;

ADD: A D D { setText(getText().toUpperCase()); } ;
ADVANCE: A D V A N C E { setText(getText().toUpperCase()); } ;
ALL: A L L { setText(getText().toUpperCase()); } ;
ALTER: A L T E R { setText(getText().toUpperCase()); } ;
ANALYZE: A N A L Y Z E { setText(getText().toUpperCase()); } ;
AND: A N D { setText(getText().toUpperCase()); } ;
ANY: A N Y { setText(getText().toUpperCase()); } ;
APPROXIMATE: A P P R O X I M A T E { setText(getText().toUpperCase()); } ;
ARRAY: A R R A Y { setText(getText().toUpperCase()); } ;
AS: A S { setText(getText().toUpperCase()); } ;
ASC: A S C { setText(getText().toUpperCase()); } ;
AT: A T { setText(getText().toUpperCase()); } ;
BERNOULLI: B E R N O U L L I { setText(getText().toUpperCase()); } ;
BETWEEN: B E T W E E N { setText(getText().toUpperCase()); } ;
BY: B Y { setText(getText().toUpperCase()); } ;
CALL: C A L L { setText(getText().toUpperCase()); } ;
CASE: C A S E { setText(getText().toUpperCase()); } ;
CAST: C A S T { setText(getText().toUpperCase()); } ;
CATALOG: C A T A L O G { setText(getText().toUpperCase()); } ;
CATALOGS: C A T A L O G S { setText(getText().toUpperCase()); } ;
COALESCE: C O A L E S C E { setText(getText().toUpperCase()); } ;
COLUMN: C O L U M N { setText(getText().toUpperCase()); } ;
COLUMNS: C O L U M N S { setText(getText().toUpperCase()); } ;
COMMIT: C O M M I T { setText(getText().toUpperCase()); } ;
COMMITTED: C O M M I T T E D { setText(getText().toUpperCase()); } ;
CONFIDENCE: C O N F I D E N C E { setText(getText().toUpperCase()); } ;
CONSTRAINT: C O N S T R A I N T { setText(getText().toUpperCase()); } ;
CREATE: C R E A T E { setText(getText().toUpperCase()); } ;
CROSS: C R O S S { setText(getText().toUpperCase()); } ;
CUBE: C U B E { setText(getText().toUpperCase()); } ;
CURRENT: C U R R E N T { setText(getText().toUpperCase()); } ;
CURRENT_DATE: C U R R E N T '_' D A T E { setText(getText().toUpperCase()); } ;
CURRENT_TIME: C U R R E N T '_' T I M E { setText(getText().toUpperCase()); } ;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P { setText(getText().toUpperCase()); } ;
DATA: D A T A { setText(getText().toUpperCase()); } ;
DATE: D A T E { setText(getText().toUpperCase()); } ;
DAY: D A Y { setText(getText().toUpperCase()); } ;
DEALLOCATE: D E A L L O C A T E { setText(getText().toUpperCase()); } ;
DELETE: D E L E T E { setText(getText().toUpperCase()); } ;
DESC: D E S C { setText(getText().toUpperCase()); } ;
DESCRIBE: D E S C R I B E { setText(getText().toUpperCase()); } ;
DISTINCT: D I S T I N C T { setText(getText().toUpperCase()); } ;
DISTRIBUTED: D I S T R I B U T E D { setText(getText().toUpperCase()); } ;
DROP: D R O P { setText(getText().toUpperCase()); } ;
ELSE: E L S E { setText(getText().toUpperCase()); } ;
END: E N D { setText(getText().toUpperCase()); } ;
ESCAPE: E S C A P E { setText(getText().toUpperCase()); } ;
EXCEPT: E X C E P T { setText(getText().toUpperCase()); } ;
EXECUTE: E X E C U T E { setText(getText().toUpperCase()); } ;
EXISTS: E X I S T S { setText(getText().toUpperCase()); } ;
EXPLAIN: E X P L A I N { setText(getText().toUpperCase()); } ;
EXPORT: E X P O R T { setText(getText().toUpperCase()); } ;
EXTRACT: E X T R A C T { setText(getText().toUpperCase()); } ;
FALSE: F A L S E { setText(getText().toUpperCase()); } ;
FIRST: F I R S T { setText(getText().toUpperCase()); } ;
FOLLOWING: F O L L O W I N G { setText(getText().toUpperCase()); } ;
FOR: F O R { setText(getText().toUpperCase()); } ;
FORMAT: F O R M A T { setText(getText().toUpperCase()); } ;
FROM: F R O M { setText(getText().toUpperCase()); } ;
FULL: F U L L { setText(getText().toUpperCase()); } ;
FUNCTIONS: F U N C T I O N S { setText(getText().toUpperCase()); } ;
GRANT: G R A N T { setText(getText().toUpperCase()); } ;
GRAPHVIZ: G R A P H V I Z { setText(getText().toUpperCase()); } ;
GROUP: G R O U P { setText(getText().toUpperCase()); } ;
GROUPING: G R O U P I N G { setText(getText().toUpperCase()); } ;
HAVING: H A V I N G { setText(getText().toUpperCase()); } ;
HOPPING: H O P P I N G { setText(getText().toUpperCase()); } ;
HOUR: H O U R { setText(getText().toUpperCase()); } ;
IF: I F { setText(getText().toUpperCase()); } ;
IN: I N { setText(getText().toUpperCase()); } ;
INNER: I N N E R { setText(getText().toUpperCase()); } ;
INSERT: I N S E R T { setText(getText().toUpperCase()); } ;
INTEGER: I N T E G E R { setText(getText().toUpperCase()); } ;
INTERSECT: I N T E R S E C T { setText(getText().toUpperCase()); } ;
INTERVAL: I N T E R V A L { setText(getText().toUpperCase()); } ;
INTO: I N T O { setText(getText().toUpperCase()); } ;
IS: I S { setText(getText().toUpperCase()); } ;
ISOLATION: I S O L A T I O N { setText(getText().toUpperCase()); } ;
JOIN: J O I N { setText(getText().toUpperCase()); } ;
LAST: L A S T { setText(getText().toUpperCase()); } ;
LEFT: L E F T { setText(getText().toUpperCase()); } ;
LEVEL: L E V E L { setText(getText().toUpperCase()); } ;
LIKE: L I K E { setText(getText().toUpperCase()); } ;
LIMIT: L I M I T { setText(getText().toUpperCase()); } ;
LIST: L I S T { setText(getText().toUpperCase()); } ;
LOAD: L O A D { setText(getText().toUpperCase()); } ;
LOCALTIME: L O C A L T I M E { setText(getText().toUpperCase()); } ;
LOCALTIMESTAMP: L O C A L T I M E S T A M P { setText(getText().toUpperCase()); } ;
LOGICAL: L O G I C A L { setText(getText().toUpperCase()); } ;
MAP: M A P { setText(getText().toUpperCase()); } ;
MILLISECOND: M I L L I S E C O N D { setText(getText().toUpperCase()); } ;
MINUTE: M I N U T E { setText(getText().toUpperCase()); } ;
MONTH: M O N T H { setText(getText().toUpperCase()); } ;
NATURAL: N A T U R A L { setText(getText().toUpperCase()); } ;
NFC : N F C { setText(getText().toUpperCase()); } ;
NFD : N F D { setText(getText().toUpperCase()); } ;
NFKC : N F K C { setText(getText().toUpperCase()); } ;
NFKD : N F K D { setText(getText().toUpperCase()); } ;
NO: N O { setText(getText().toUpperCase()); } ;
NORMALIZE: N O R M A L I Z E { setText(getText().toUpperCase()); } ;
NOT: N O T { setText(getText().toUpperCase()); } ;
NULL: N U L L { setText(getText().toUpperCase()); } ;
NULLIF: N U L L I F { setText(getText().toUpperCase()); } ;
NULLS: N U L L S { setText(getText().toUpperCase()); } ;
ON: O N { setText(getText().toUpperCase()); } ;
ONLY: O N L Y { setText(getText().toUpperCase()); } ;
OPTION: O P T I O N { setText(getText().toUpperCase()); } ;
OR: O R { setText(getText().toUpperCase()); } ;
ORDER: O R D E R { setText(getText().toUpperCase()); } ;
ORDINALITY: O R D I N A L I T Y { setText(getText().toUpperCase()); } ;
OUTER: O U T E R { setText(getText().toUpperCase()); } ;
OVER: O V E R { setText(getText().toUpperCase()); } ;
PARTITION: P A R T I T I O N { setText(getText().toUpperCase()); } ;
PARTITIONS: P A R T I T I O N S { setText(getText().toUpperCase()); } ;
POISSONIZED: P O I S S O N I Z E D { setText(getText().toUpperCase()); } ;
POSITION: P O S I T I O N { setText(getText().toUpperCase()); } ;
PRECEDING: P R E C E D I N G { setText(getText().toUpperCase()); } ;
PREPARE: P R E P A R E { setText(getText().toUpperCase()); } ;
PRINT: P R I N T { setText(getText().toUpperCase()); } ;
PRIVILEGES: P R I V I L E G E S { setText(getText().toUpperCase()); } ;
PUBLIC: P U B L I C { setText(getText().toUpperCase()); } ;
QUERIES: Q U E R I E S { setText(getText().toUpperCase()); } ;
RANGE: R A N G E { setText(getText().toUpperCase()); } ;
READ: R E A D { setText(getText().toUpperCase()); } ;
RECURSIVE: R E C U R S I V E { setText(getText().toUpperCase()); } ;
RENAME: R E N A M E { setText(getText().toUpperCase()); } ;
REPEATABLE: R E P E A T A B L E { setText(getText().toUpperCase()); } ;
REPLACE: R E P L A C E { setText(getText().toUpperCase()); } ;
RESCALED: R E S C A L E D { setText(getText().toUpperCase()); } ;
RESET: R E S E T { setText(getText().toUpperCase()); } ;
REVOKE: R E V O K E { setText(getText().toUpperCase()); } ;
RIGHT: R I G H T { setText(getText().toUpperCase()); } ;
ROLLBACK: R O L L B A C K { setText(getText().toUpperCase()); } ;
ROLLUP: R O L L U P { setText(getText().toUpperCase()); } ;
ROW: R O W { setText(getText().toUpperCase()); } ;
ROWS: R O W S { setText(getText().toUpperCase()); } ;
SAMPLE: S A M P L E { setText(getText().toUpperCase()); } ;
SCHEMAS: S C H E M A S { setText(getText().toUpperCase()); } ;
SECOND: S E C O N D { setText(getText().toUpperCase()); } ;
SELECT: S E L E C T { setText(getText().toUpperCase()); } ;
SERIALIZABLE: S E R I A L I Z A B L E { setText(getText().toUpperCase()); } ;
SESSION: S E S S I O N { setText(getText().toUpperCase()); } ;
SET: S E T { setText(getText().toUpperCase()); } ;
SETS: S E T S { setText(getText().toUpperCase()); } ;
SHOW: S H O W { setText(getText().toUpperCase()); } ;
SIZE: S I Z E { setText(getText().toUpperCase()); } ;
SMALLINT: S M A L L I N T { setText(getText().toUpperCase()); } ;
SOME: S O M E { setText(getText().toUpperCase()); } ;
START: S T A R T { setText(getText().toUpperCase()); } ;
STRATIFY: S T R A T I F Y { setText(getText().toUpperCase()); } ;
STREAM: S T R E A M { setText(getText().toUpperCase()); } ;
STREAMS: S T R E A M S { setText(getText().toUpperCase()); } ;
SUBSTRING: S U B S T R I N G { setText(getText().toUpperCase()); } ;
SYSTEM: S Y S T E M { setText(getText().toUpperCase()); } ;
TABLE: T A B L E { setText(getText().toUpperCase()); } ;
TABLES: T A B L E S { setText(getText().toUpperCase()); } ;
TABLESAMPLE: T A B L E S A M P L E { setText(getText().toUpperCase()); } ;
TERMINATE: T E R M I N A T E { setText(getText().toUpperCase()); } ;
TEXT: T E X T { setText(getText().toUpperCase()); } ;
THEN: T H E N { setText(getText().toUpperCase()); } ;
TIME: T I M E { setText(getText().toUpperCase()); } ;
TIMESTAMP: T I M E S T A M P { setText(getText().toUpperCase()); } ;
TINYINT: T I N Y I N T { setText(getText().toUpperCase()); } ;
TO: T O { setText(getText().toUpperCase()); } ;
TOPIC: T O P I C { setText(getText().toUpperCase()); } ;
TOPICS: T O P I C S { setText(getText().toUpperCase()); } ;
TRANSACTION: T R A N S A C T I O N { setText(getText().toUpperCase()); } ;
TRUE: T R U E { setText(getText().toUpperCase()); } ;
TRY: T R Y { setText(getText().toUpperCase()); } ;
TRY_CAST: T R Y '_' C A S T { setText(getText().toUpperCase()); } ;
TUMBLING: T U M B L I N G { setText(getText().toUpperCase()); } ;
TYPE: T Y P E { setText(getText().toUpperCase()); } ;
UNBOUNDED: U N B O U N D E D { setText(getText().toUpperCase()); } ;
UNCOMMITTED: U N C O M M I T T E D { setText(getText().toUpperCase()); } ;
UNION: U N I O N { setText(getText().toUpperCase()); } ;
UNNEST: U N N E S T { setText(getText().toUpperCase()); } ;
USE: U S E { setText(getText().toUpperCase()); } ;
USING: U S I N G { setText(getText().toUpperCase()); } ;
VALUES: V A L U E S { setText(getText().toUpperCase()); } ;
VIEW: V I E W { setText(getText().toUpperCase()); } ;
WHEN: W H E N { setText(getText().toUpperCase()); } ;
WHERE: W H E R E { setText(getText().toUpperCase()); } ;
WINDOW: W I N D O W { setText(getText().toUpperCase()); } ;
WITH: W I T H { setText(getText().toUpperCase()); } ;
WORK: W O R K { setText(getText().toUpperCase()); } ;
WRITE: W R I T E { setText(getText().toUpperCase()); } ;
YEAR: Y E A R { setText(getText().toUpperCase()); } ;
ZONE: Z O N E { setText(getText().toUpperCase()); } ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    { setText(getText().toUpperCase()); }
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    { setText(getText().toUpperCase()); }
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    { setText(getText().toUpperCase()); }
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    { setText(getText().toUpperCase()); }
    ;

// END CASE-INSENSITIVE TOKENS

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]|[a-z]
    ;

fragment A: 'A'|'a';
fragment B: 'B'|'b';
fragment C: 'C'|'c';
fragment D: 'D'|'d';
fragment E: 'E'|'e';
fragment F: 'F'|'f';
fragment G: 'G'|'g';
fragment H: 'H'|'h';
fragment I: 'I'|'i';
fragment J: 'J'|'j';
fragment K: 'K'|'k';
fragment L: 'L'|'l';
fragment M: 'M'|'m';
fragment N: 'N'|'n';
fragment O: 'O'|'o';
fragment P: 'P'|'p';
fragment Q: 'Q'|'q';
fragment R: 'R'|'r';
fragment S: 'S'|'s';
fragment T: 'T'|'t';
fragment U: 'U'|'u';
fragment V: 'V'|'v';
fragment W: 'W'|'w';
fragment X: 'X'|'x';
fragment Y: 'Y'|'y';
fragment Z: 'Z'|'z';

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
