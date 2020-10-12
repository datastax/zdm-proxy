grammar SimplifiedCql;

// PARSER

cqlStatement
    : selectStatement EOS?
    | otherStatement EOS?
    ;

selectStatement
    : K_SELECT K_JSON? K_DISTINCT? selectClause K_FROM tableName discardedContent
    ;

otherStatement
    : discardedContent
    ;

selectClause
    : selector ( ',' selector )*
    | '*'
    ;

selector
    : unaliasedSelector (K_AS identifier)?
    ;

unaliasedSelector
    : identifier
    | term
    | K_CAST '(' unaliasedSelector K_AS primitiveType ')'
    ;

term
    : literal
    | functionCall
    | '(' cqlType ')' term
    ;

literal
    : primitiveLiteral
    | collectionLiteral
    | K_NULL
    ;

primitiveLiteral
    : STRING_LITERAL
    | INTEGER
    | FLOAT
    | BOOLEAN
    | DURATION
    | UUID
    | HEXNUMBER
    | '-'? K_NAN
    | '-'? K_INFINITY
    ;

collectionLiteral
    : listLiteral
    | setLiteral
    | mapLiteral
    ;

listLiteral
    : '[' ( term ( ',' term )* )? ']'
    ;

setLiteral
    : '{' ( term ( ',' term )* )? '}'
    ;

mapLiteral
    : '{' ( term ':' term  ( ',' term  ':' term )* )? '}'
    ;

cqlType
    : primitiveType
    | collectionType
    | tupleType
    | userTypeName
    | K_FROZEN '<' cqlType '>'
    ;

primitiveType
    : K_ASCII
    | K_BIGINT
    | K_BLOB
    | K_BOOLEAN
    | K_COUNTER
    | K_DATE
    | K_DECIMAL
    | K_DOUBLE
    | K_DURATION
    | K_FLOAT
    | K_INET
    | K_INT
    | K_SMALLINT
    | K_TEXT
    | K_TIME
    | K_TIMESTAMP
    | K_TIMEUUID
    | K_TINYINT
    | K_UUID
    | K_VARCHAR
    | K_VARINT
    ;

collectionType
    : K_LIST '<' cqlType '>'
    | K_SET  '<' cqlType '>'
    | K_MAP  '<' cqlType ',' cqlType '>'
    ;

tupleType
    : K_TUPLE '<' cqlType (',' cqlType)* '>'
    ;

functionCall
    : functionName '(' ')'
    | functionName '(' '*' ')'
    | functionName '(' functionArgs ')'
    ;

functionArgs
    : unaliasedSelector ( ',' unaliasedSelector )*
    ;

tableName
    : qualifiedIdentifier
    ;

functionName
    : qualifiedIdentifier
    ;

userTypeName
    : qualifiedIdentifier
    ;

keyspaceName
    : identifier
    ;

qualifiedIdentifier
    : ( keyspaceName '.' )? identifier
    ;

identifier
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

discardedContent
    : unknown*
    ;

unknown
    : .
    ;

// LEXER

K_AS:          A S;
K_ASCII:       A S C I I;
K_BIGINT:      B I G I N T;
K_BLOB:        B L O B;
K_BOOLEAN:     B O O L E A N;
K_CAST:        C A S T;
K_COUNTER:     C O U N T E R;
K_DATE:        D A T E;
K_DECIMAL:     D E C I M A L;
K_DISTINCT:    D I S T I N C T;
K_DOUBLE:      D O U B L E;
K_DURATION:    D U R A T I O N;
K_FLOAT:       F L O A T;
K_FROM:        F R O M;
K_FROZEN:      F R O Z E N;
K_INET:        I N E T;
K_INFINITY:    I N F I N I T Y;
K_INT:         I N T;
K_JSON:        J S O N;
K_LIST:        L I S T;
K_MAP:         M A P;
K_NAN:         N A N;
K_NULL:        N U L L;
K_SELECT:      S E L E C T;
K_SET:         S E T;
K_SMALLINT:    S M A L L I N T;
K_TEXT:        T E X T;
K_TIME:        T I M E;
K_TIMESTAMP:   T I M E S T A M P;
K_TIMEUUID:    T I M E U U I D;
K_TINYINT:     T I N Y I N T;
K_TUPLE:       T U P L E;
K_UUID:        U U I D;
K_VARCHAR:     V A R C H A R;
K_VARINT:      V A R I N T;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

STRING_LITERAL
    : /* pg-style string literal */
      '$' '$' ( ~'$' | '$' ~'$' )* '$' '$'
    | /* conventional quoted string literal */
      '\'' ( ~'\'' | '\'' '\'' )* '\''
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

fragment EXPONENT
    : E ('+' | '-')? DIGIT+
    ;

fragment DURATION_UNIT
    : Y
    | M O
    | W
    | D
    | H
    | M
    | S
    | M S
    | U S
    | '\u00B5' S
    | N S
    ;

INTEGER
    : '-'? DIGIT+
    ;

FLOAT
    : INTEGER EXPONENT
    | INTEGER '.' DIGIT* EXPONENT?
    ;

/*
 * This has to be before UNQUOTED_IDENTIFIER so it takes precendence over it.
 */
BOOLEAN
    : T R U E | F A L S E
    ;

DURATION
    : '-'? DIGIT+ DURATION_UNIT (DIGIT+ DURATION_UNIT)*
    | '-'? 'P' (DIGIT+ 'Y')? (DIGIT+ 'M')? (DIGIT+ 'D')? ('T' (DIGIT+ 'H')? (DIGIT+ 'M')? (DIGIT+ 'S')?)? // ISO 8601 "format with designators"
    | '-'? 'P' DIGIT+ 'W'
    | '-'? 'P' DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT // ISO 8601 "alternative format"
    ;

UNQUOTED_IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    ;

HEXNUMBER
    : '0' X HEX*
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ -> channel(HIDDEN)
    ;

COMMENT
    : ('--' | '//') .*? ('\n'|'\r') -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

// End of statement
EOS
    : ';'
    ;
