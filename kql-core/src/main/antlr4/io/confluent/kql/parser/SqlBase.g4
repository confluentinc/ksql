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

TIME_WITH_TIME_ZONE
    : TIME WS WITH WS TIME WS ZONE
    ;

TIMESTAMP_WITH_TIME_ZONE
    : TIMESTAMP WS WITH WS TIME WS ZONE
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

SELECT: S E L E C T;
FROM: F R O M;
ADD: A D D;
AS: A S;
ALL: A L L;
SOME: S O M E;
ANY: A N Y;
DISTINCT: D I S T I N C T;
WHERE: W H E R E;
WINDOW: W I N D O W;
GROUP: G R O U P;
BY: B Y;
GROUPING: G R O U P I N G;
SETS: S E T S;
CUBE: C U B E;
ROLLUP: R O L L U P;
ORDER: O R D E R;
HAVING: H A V I N G;
LIMIT: L I M I T;
APPROXIMATE: A P P R O X I M A T E;
AT: A T;
CONFIDENCE: C O N F I D E N C E;
OR: O R;
AND: A N D;
IN: I N;
NOT: N O T;
NO: N O;
EXISTS: E X I S T S;
BETWEEN: B E T W E E N;
LIKE: L I K E;
IS: I S;
NULL: N U L L;
TRUE: T R U E;
FALSE: F A L S E;
NULLS: N U L L S;
FIRST: F I R S T;
LAST: L A S T;
ESCAPE: E S C A P E;
ASC: A S C;
DESC: D E S C;
SUBSTRING: S U B S T R I N G;
POSITION: P O S I T I O N;
FOR: F O R;
TINYINT: T I N Y I N T;
SMALLINT: S M A L L I N T;
INTEGER: I N T E G E R;
DATE: D A T E;
TIME: T I M E;
TIMESTAMP: T I M E S T A M P;
INTERVAL: I N T E R V A L;
YEAR: Y E A R;
MONTH: M O N T H;
DAY: D A Y;
HOUR: H O U R;
MINUTE: M I N U T E;
SECOND: S E C O N D;
MILLISECOND: M I L L I S E C O N D;
ZONE: Z O N E;
CURRENT_DATE: C U R R E N T '_' D A T E;
CURRENT_TIME: C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;
LOCALTIME: L O C A L T I M E;
LOCALTIMESTAMP: L O C A L T I M E S T A M P;
EXTRACT: E X T R A C T;
TUMBLING: T U M B L I N G;
HOPPING: H O P P I N G;
SIZE: S I Z E;
ADVANCE: A D V A N C E;
CASE: C A S E;
WHEN: W H E N;
THEN: T H E N;
ELSE: E L S E;
END: E N D;
JOIN: J O I N;
CROSS: C R O S S;
OUTER: O U T E R;
INNER: I N N E R;
LEFT: L E F T;
RIGHT: R I G H T;
FULL: F U L L;
NATURAL: N A T U R A L;
USING: U S I N G;
ON: O N;
OVER: O V E R;
PARTITION: P A R T I T I O N;
RANGE: R A N G E;
ROWS: R O W S;
UNBOUNDED: U N B O U N D E D;
PRECEDING: P R E C E D I N G;
FOLLOWING: F O L L O W I N G;
CURRENT: C U R R E N T;
ROW: R O W;
WITH: W I T H;
RECURSIVE: R E C U R S I V E;
VALUES: V A L U E S;
CREATE: C R E A T E;
TABLE: T A B L E;
TOPIC: T O P I C;
STREAM: S T R E A M;
STREAMS: S T R E A M S;
VIEW: V I E W;
REPLACE: R E P L A C E;
INSERT: I N S E R T;
DELETE: D E L E T E;
INTO: I N T O;
CONSTRAINT: C O N S T R A I N T;
DESCRIBE: D E S C R I B E;
PRINT: P R I N T;
GRANT: G R A N T;
REVOKE: R E V O K E;
PRIVILEGES: P R I V I L E G E S;
PUBLIC: P U B L I C;
OPTION: O P T I O N;
EXPLAIN: E X P L A I N;
ANALYZE: A N A L Y Z E;
FORMAT: F O R M A T;
TYPE: T Y P E;
TEXT: T E X T;
GRAPHVIZ: G R A P H V I Z;
LOGICAL: L O G I C A L;
DISTRIBUTED: D I S T R I B U T E D;
TRY: T R Y;
CAST: C A S T;
TRY_CAST: T R Y '_' C A S T;

SHOW: S H O W;
LIST: L I S T;
TABLES: T A B L E S;
TOPICS: T O P I C S;
QUERIES: Q U E R I E S;
TERMINATE: T E R M I N A T E;
LOAD: L O A D;
SCHEMAS: S C H E M A S;
CATALOGS: C A T A L O G S;
COLUMNS: C O L U M N S;
COLUMN: C O L U M N;
USE: U S E;
PARTITIONS: P A R T I T I O N S;
FUNCTIONS: F U N C T I O N S;
DROP: D R O P;
UNION: U N I O N;
EXCEPT: E X C E P T;
INTERSECT: I N T E R S E C T;
TO: T O;
SYSTEM: S Y S T E M;
BERNOULLI: B E R N O U L L I;
POISSONIZED: P O I S S O N I Z E D;
TABLESAMPLE: T A B L E S A M P L E;
RESCALED: R E S C A L E D;
STRATIFY: S T R A T I F Y;
ALTER: A L T E R;
RENAME: R E N A M E;
UNNEST: U N N E S T;
ORDINALITY: O R D I N A L I T Y;
ARRAY: A R R A Y;
MAP: M A P;
SET: S E T;
RESET: R E S E T;
SESSION: S E S S I O N;
DATA: D A T A;
START: S T A R T;
TRANSACTION: T R A N S A C T I O N;
COMMIT: C O M M I T;
ROLLBACK: R O L L B A C K;
WORK: W O R K;
ISOLATION: I S O L A T I O N;
LEVEL: L E V E L;
SERIALIZABLE: S E R I A L I Z A B L E;
REPEATABLE: R E P E A T A B L E;
COMMITTED: C O M M I T T E D;
UNCOMMITTED: U N C O M M I T T E D;
READ: R E A D;
WRITE: W R I T E;
ONLY: O N L Y;
CALL: C A L L;
PREPARE: P R E P A R E;
DEALLOCATE: D E A L L O C A T E;
EXECUTE: E X E C U T E;
SAMPLE: S A M P L E;
EXPORT: E X P O R T;
CATALOG: C A T A L O G;

NORMALIZE: N O R M A L I Z E;
NFD : N F D;
NFC : N F C;
NFKD : N F K D;
NFKC : N F K C;

IF: I F;
NULLIF: N U L L I F;
COALESCE: C O A L E S C E;

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

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

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
