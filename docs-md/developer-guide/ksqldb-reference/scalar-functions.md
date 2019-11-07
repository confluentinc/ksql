---
layout: page
title: ksqlDB Scalar Functions
tagline:  ksqlDB scalar functions for queries
description: Scalar functions to use in  ksqlDB statements and queries
keywords: ksqlDB, function, scalar
---

- [Numeric Functions](#numeric-functions)
  - [ABS](#abs)
  - [CEIL](#ceil)
  - [ENTRIES](#entries)
  - [EXP](#exp)
  - [FLOOR](#floor)
  - [GENERATE_SERIES](#generateseries)
  - [GENERATE_SERIES](#generateseries-1)
  - [GEO_DISTANCE](#geodistance)
  - [LN](#ln)
  - [RANDOM](#random)
  - [ROUND](#round)
  - [SIGN](#sign)
  - [SQRT](#sqrt)
- [Collections](#collections)
  - [ARRAYCONTAINS](#arraycontains)
  - [AS_ARRAY](#asarray)
  - [AS_MAP](#asmap)
  - [ELT](#elt)
  - [FIELD](#field)
  - [SLICE](#slice)
- [Strings](#strings)
  - [CONCAT](#concat)
  - [EXTRACTJSONFIELD](#extractjsonfield)
  - [IFNULL](#ifnull)
  - [INITCAP](#initcap)
  - [LCASE](#lcase)
  - [LEN](#len)
  - [MASK](#mask)
  - [MASK_KEEP_LEFT](#maskkeepleft)
  - [MASK_KEEP_RIGHT](#maskkeepright)
  - [MASK_LEFT](#maskleft)
  - [MASK_RIGHT](#maskright)
  - [REPLACE](#replace)
  - [SPLIT](#split)
  - [SUBSTRING](#substring)
  - [TRIM](#trim)
  - [UCASE](#ucase)
- [Date and Time](#date-and-time)
  - [UNIX_DAT](#unixdat)
  - [UNIX_TIMESTAMP](#unixtimestamp)
  - [DATETOSTRING](#datetostring)
  - [STRINGTODATE](#stringtodate)
  - [STRINGTOTIMESTAMP](#stringtotimestamp)
  - [TIMESTAMPTOSTRING](#timestamptostring)
- [URL](#url)
  - [URL_DECODE_PARAM](#urldecodeparam)
  - [URL_ENCODE_PARAM](#urlencodeparam)
  - [URL_EXTRACT_FRAGMENT](#urlextractfragment)
  - [URL_EXTRACT_HOST](#urlextracthost)
  - [URL_EXTRACT_PARAMETER](#urlextractparameter)
  - [URL_EXTRACT_PATH](#urlextractpath)
  - [URL_EXTRACT_PORT](#urlextractport)
  - [URL_EXTRACT_PROTOCOL](#urlextractprotocol)
  - [URL_EXTRACT_QUERY](#urlextractquery)

Numeric Functions
=================

ABS
---

`ABS(col1)`

The absolute value of a value.

CEIL
----

`CEIL(col1)`

The ceiling of a value.

ENTRIES
-------

`ENTRIES(map MAP, sorted BOOLEAN)`

Constructs an array of structs from the entries in a map. Each struct has
a field named `K` containing the key, which is a string, and a field named
`V`, which holds the value.

If `sorted` is true, the entries are sorted by key.                                          

EXP
---

`EXP(col1)`

The exponential of a value.

FLOOR
-----

`FLOOR(col1)`

The floor of a value.

GENERATE_SERIES
---------------

`GENERATE_SERIES(start, end)`

Constructs an array of values between `start` and `end` (inclusive).       
Parameters can be `INT` or `BIGINT`.

GENERATE_SERIES
---------------

`GENERATE_SERIES(start, end, step)`

Constructs an array of values between `start` and `end` (inclusive)
with a specified step size. The step can be positive or negative.      
Parameters `start` and `end` can be `INT` or `BIGINT`. Parameter `step`
must be an `INT`.


GEO_DISTANCE
------------

`GEO_DISTANCE(lat1, lon1, lat2, lon2, unit)`

The great-circle distance between two lat-long points, both specified
in decimal degrees. An optional final parameter specifies `KM`
(the default) or `miles`.

LN
--

`LN(col1)`

The natural logarithm of a value.

RANDOM
------

`RANDOM()`

Return a random DOUBLE value between 0.0 and 1.0.  

ROUND
-----

`ROUND(col1)` or `ROUND(col1, scale)`

Round a value to the number of decimal places
as specified by scale to the right of the decimal
point. If scale is negative then value is rounded
to the right of the decimal point.

Numbers equidistant to the nearest value are
rounded up (in the positive direction).
If the number of decimal places is not provided
it defaults to zero.

SIGN
----

`SIGN(col1)`

The sign of a numeric value as an INTEGER:

* -1 if the argument is negative
* 0 if the argument is zero
* 1 if the argument is positive
* `null` argument is null

SQRT
----

`SQRT(col1)`

The square root of a value.


Collections
===========

ARRAYCONTAINS
-------------

`ARRAYCONTAINS('[1, 2, 3]', 3)`

Given JSON or AVRO array checks if a search value contains in it.

AS_ARRAY
--------

`AS_ARRAY(col1, col2)`

Construct an array from a variable number of inputs.

AS_MAP
------

`AS_MAP(keys, vals)`

Construct a map from a list of keys and a list of values.

ELT
---

`ELT(n INTEGER, args VARCHAR[])`

Returns element `n` in the `args` list of strings, or NULL if `n` is less than
1 or greater than the number of arguments. This function is 1-indexed. ELT is
the complement to FIELD.

FIELD
-----

`FIELD(str VARCHAR, args VARCHAR[])`

Returns the 1-indexed position of `str` in `args`, or 0 if not found.
If `str` is NULL, the return value is 0, because NULL is not considered
to be equal to any value. FIELD is the complement to ELT.

SLICE
-----

`SLICE(col1, from, to)`

Slices a list based on the supplied indices. The indices start at 1 and
include both endpoints.


Strings
=======

CONCAT
------

`CONCAT(col1, '_hello')`

Concatenate two strings.

EXTRACTJSONFIELD
----------------

`EXTRACTJSONFIELD(message, '$.log.cloud')`

Given a string column in JSON format, extract the field that matches.
For example EXTRACTJSONFIELD is necessary in the following JSON:

```json
{"foo": \"{\"bar\": \"quux\"}\"}
```

In cases where the column is really an object but declared as a STRING,
you can use the `STRUCT` type, which is easier to work with. For example,
`STRUCT` works in the following case:

```json
{"foo": {"bar": "quux"}}.
```

IFNULL
------

`IFNULL(col1, retval)`

If the provided VARCHAR is NULL, return `retval`, otherwise, return the
value. Only VARCHAR values are supported for the input. The return value
must be a VARCHAR.

INITCAP
-------

`INITCAP(col1)`

Capitalize the first letter in each word and convert all other letters
to lowercase. Words are delimited by whitespace.

LCASE
-----

`LCASE(col1)`

Convert a string to lowercase.

LEN
---

`LEN(col1)`

The length of a string.

MASK
----

`MASK(col1, 'X', 'x', 'n', '-')`

Convert a string to a masked or obfuscated version of itself. The optional
arguments following the input string to be masked are the characters to be
substituted for upper-case, lower-case, numeric, and other characters of the
input, respectively.

If the mask characters are omitted then the default values, shown in the
following example, are applied.

Set a given mask character to NULL to prevent any masking of that character
type. For example: `MASK("My Test $123")` will return `Xx-Xxxx--nnn`, applying
all default masks. `MASK("My Test $123", '*', NULL, '1', NULL)` will yield
`*y *est $111`.

MASK_KEEP_LEFT
--------------

`MASK_KEEP_LEFT(col1, numChars, 'X', 'x', 'n', '-')` 

Similar to the `MASK` function above, except
that the first or left-most `numChars`
characters will not be masked in any way.
For example: `MASK_KEEP_LEFT("My Test $123", 4)`
will return `My Txxx--nnn`.

MASK_KEEP_RIGHT
---------------

`MASK_KEEP_RIGHT(col1, numChars, 'X', 'x', 'n', '-')`

Similar to the `MASK` function above, except
that the last or right-most `numChars`
characters will not be masked in any way.
For example:`MASK_KEEP_RIGHT("My Test $123", 4)`
will return `Xx-Xxxx-$123`.

MASK_LEFT
---------

`MASK_LEFT(col1, numChars, 'X', 'x', 'n', '-')`

Similar to the `MASK` function above, except
that only the first or left-most `numChars`
characters will have any masking applied to them.
For example, `MASK_LEFT("My Test $123", 4)`
will return `Xx-Xest $123`.

MASK_RIGHT
----------

`MASK_RIGHT(col1, numChars, 'X', 'x', 'n', '-')`  

Similar to the `MASK` function above, except
that only the last or right-most `numChars`
characters will have any masking applied to them.
For example: `MASK_RIGHT("My Test $123", 4)`
will return `My Test -nnn`.

REPLACE
-------

`REPLACE(col1, 'foo', 'bar')`

Replace all instances of a substring in a string with a new string.

SPLIT
-----

`SPLIT(col1, delimiter)`

Splits a string into an array of substrings based
on a delimiter. If the delimiter is not found,
then the original string is returned as the only
element in the array. If the delimiter is empty,
then all characters in the string are split.
If either, string or delimiter, are NULL, then a
NULL value is returned.
                                                  
If the delimiter is found at the beginning or end
of the string, or there are contiguous delimiters,
then an empty space is added to the array.

SUBSTRING
---------

`SUBSTRING(col1, 2, 5)`
`SUBSTRING(str, pos, [len]`

Returns a substring of `str` that starts at
`pos` (first character is at position 1) and
has length `len`, or continues to the end of
the string.

For example, `SUBSTRING("stream", 1, 4)`
returns "stre".

!!! note
    Before version 5.1 of KSQL the syntax was:
    `SUBSTRING(str, start, [end])`, where `start`
    and `end` positions where base-zero indexes
    (first character at position 0) to start
    (inclusive) and end (exclusive) the substring,
    respectively.
    For example, `SUBSTRING("stream", 1, 4)` would
    return "tre".    
    It is possible to switch back to this legacy mode 
    by setting
    `ksql.functions.substring.legacy.args` to
    `true`. We recommend against enabling this
    setting. Instead, update your queries
    accordingly.

TRIM
----

`TRIM(col1)`

Trim the spaces from the beginning and end of a string.

UCASE
-----

`UCASE(col1)`

Convert a string to uppercase.

Date and Time
=============

UNIX_DAT
--------

`UNIX_DATE()`
 
Gets an integer representing days since epoch. The returned timestamp
may differ depending on the local time of different KSQL Server instances.

UNIX_TIMESTAMP
--------------

`UNIX_TIMESTAMP()`

Gets the Unix timestamp in milliseconds, represented as a BIGINT. The returned
timestamp may differ depending on the local time of different KSQL Server instances.

DATETOSTRING
------------

`DATETOSTRING(START_DATE, 'yyyy-MM-dd')`

Converts an integer representation of a date into a string representing the
date in the given format. Single quotes in the timestamp format can be escaped
with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.
The integer represents days since epoch matching the encoding used by
{{ site.kconnect }} dates.

STRINGTODATE
------------

`STRINGTODATE(col1, 'yyyy-MM-dd')`

Converts a string representation of a date in the
given format into an integer representing days
since epoch. Single quotes in the timestamp
format can be escaped with two successive single
quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

STRINGTOTIMESTAMP
-----------------

`STRINGTOTIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])`

Converts a string value in the given
format into the BIGINT value                      
that represents the millisecond timestamp. Single
quotes in the timestamp format can be escaped with
two successive single quotes, `''`, for
example: `'yyyy-MM-dd''T''HH:mm:ssX'`.

TIMEZONE is an optional parameter and it is a
`java.util.TimeZone` ID format, for example: "UTC",
"America/Los_Angeles", "PDT", "Europe/London". For
more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).    

TIMESTAMPTOSTRING
-----------------

`TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])`

Converts a BIGINT millisecond timestamp value into
the string representation of the timestamp in
the given format. Single quotes in the
timestamp format can be escaped with two
successive single quotes, `''`, for example:
`'yyyy-MM-dd''T''HH:mm:ssX'`.
TIMEZONE is an optional parameter and it is a
java.util.TimeZone ID format, for example: "UTC",
"America/Los_Angeles", "PDT", "Europe/London". For
more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

URL
===

URL_DECODE_PARAM
----------------

`URL_DECODE_PARAM(col1)`

Unescapes the `URL-param-encoded`_ value in `col1`. This is the inverse of
`URL_ENCODE_PARAM`.

- Input: `'url%20encoded`
- Output: `url encoded`

URL_ENCODE_PARAM
----------------

`URL_ENCODE_PARAM(col1)`

Escapes the value of `col1` such that it can
safely be used in URL query parameters. Note that
this is not the same as encoding a value for use 
in the path portion of a URL.                    
                                                 
- Input: `url encoded`                             
- Output: `'url%20encoded`                         

URL_EXTRACT_FRAGMENT
--------------------

`URL_EXTRACT_FRAGMENT(url)`

Extract the fragment portion of the specified
value. Returns NULL if `url` is not a valid URL
or if the fragment does not exist. Any encoded 
value will be decoded.                         
                                               
- Input: `http://test.com#frag`,                 
- Output: `frag`                                 

- Input: `http://test.com#frag%20space`,         
- Output: `frag space`                           

URL_EXTRACT_HOST
----------------

`URL_EXTRACT_HOST(url)`

Extract the host-name portion of the specified
value. Returns NULL if the `url` is not a valid  
URI according to RFC-2396.                       
                                                 
- Input: `http://test.com:8080/path`,              
- Output: `test.com`                               

URL_EXTRACT_PARAMETER
---------------------

`URL_EXTRACT_PARAMETER(url, parameter_name)`

Extract the value of the requested parameter from
the query-string of `url`. Returns NULL
if the parameter is not present, has no value
specified for it in the query-string, or `url`
is not a valid URI. Encodes the param and decodes
the output (see examples).
                                                 
To get all of the parameter values from a
URL as a single string, see `URL_EXTRACT_QUERY.`
                                                 
- Input: `http://test.com?a%20b=c%20d`, `a b`
- Output: `c d`

- Input: `http://test.com?a=foo&b=bar`, `b`
- Output: `bar`

URL_EXTRACT_PATH
----------------

`URL_EXTRACT_PATH(url)`

Extracts the path from `url`.
Returns NULL if `url` is not a valid URI but  
returns an empty string if the path is empty. 
                                              
- Input: `http://test.com/path/to#a`            
- Output: `path/to`                             

URL_EXTRACT_PORT
----------------

`URL_EXTRACT_PORT(url)`

Extract the port number from `url`.
Returns NULL if `url` is not a valid URI or does
not contain an explicit port number.            
                                                
- Input: `http://localhost:8080/path`             
- Output: `8080`                                  

URL_EXTRACT_PROTOCOL
--------------------

`URL_EXTRACT_PROTOCOL(url)`

Extract the protocol from `url`. Returns NULL if
`url` is an invalid URI or has no protocol.
                                           
- Input: `http://test.com?a=foo&b=bar`       
- Output: `http`                             

URL_EXTRACT_QUERY
-----------------

`URL_EXTRACT_QUERY(url)`

Extract the decoded query-string portion of
`url`. Returns NULL if no query-string is  
present or `url` is not a valid URI.       
                                           
- Input: `http://test.com?a=foo%20bar&b=baz` 
- Output: `a=foo bar&b=baz`                  


!!! note
    All KSQL URL functions assume URI syntax defined in
    [RFC 39386](https://tools.ietf.org/html/rfc3986). For more information on the
    structure of a URI, including definitions of the various components, see
    Section 3 of the RFC. For encoding/decoding, the
    `application/x-www-form-urlencoded` convention is followed.

Page last revised on: {{ git_revision_date }}
