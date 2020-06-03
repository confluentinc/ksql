---
layout: page
title: ksqlDB Scalar Functions
tagline:  ksqlDB scalar functions for queries
description: Scalar functions to use in  ksqlDB statements and queries
keywords: ksqlDB, function, scalar
---

## Numeric functions

### `ABS`

```sql
ABS(col1)
```

The absolute value of a value.

### `CEIL`

```sql
CEIL(col1)
```

The ceiling of a value.

### `ENTRIES`

```sql
ENTRIES(map MAP, sorted BOOLEAN)
```

Constructs an array of structs from the entries in a map. Each struct has
a field named `K` containing the key, which is a string, and a field named
`V`, which holds the value.

If `sorted` is true, the entries are sorted by key.

### `EXP`

```sql
EXP(col1)
```

The exponential of a value.

### `FLOOR`

```sql
FLOOR(col1)
```

The floor of a value.

### `GENERATE_SERIES`

```sql
GENERATE_SERIES(start, end)
```

```sql
GENERATE_SERIES(start, end, step)
```

Constructs an array of values between `start` and `end` (inclusive).

Parameters `start` and `end` can be an `INT` or `BIGINT`.

`step`, if supplied, specifies the step size. The step can be positive or negative.
If not supplied, `step` defaults to `1`. Parameter `step` must be an `INT`.

### `GEO_DISTANCE`

```sql
GEO_DISTANCE(lat1, lon1, lat2, lon2, unit)
```

The great-circle distance between two lat-long points, both specified
in decimal degrees. An optional final parameter specifies `KM`
(the default) or `miles`.

### `LN`

```sql
LN(col1)
```

The natural logarithm of a value.

### `RANDOM`

```sql
RANDOM()
```

Return a random DOUBLE value between 0.0 and 1.0.

### `ROUND`

```sql
ROUND(col1)
```

```sql
ROUND(col1, scale)
```

Round a value to the number of decimal places
as specified by scale to the right of the decimal
point. If scale is negative then value is rounded
to the right of the decimal point.

Numbers equidistant to the nearest value are
rounded up (in the positive direction).
If the number of decimal places is not provided
it defaults to zero.

### `SIGN`

```sql
SIGN(col1)
```

The sign of a numeric value as an INTEGER:

* -1 if the argument is negative
* 0 if the argument is zero
* 1 if the argument is positive
* `null` argument is null

### `SQRT`

```sql
SQRT(col1)
```

The square root of a value.

## Collections

### `ARRAY`

```sql
ARRAY[col1, col2, ...]
```

Construct an array from a variable number of inputs.

### ``ARRAY_CONTAINS``

```sql
ARRAY_CONTAINS([1, 2, 3], 3)
```

Given an array, checks if a search value is contained in the array.

Accepts any `ARRAY` type. The type of the second param must match the element type of the `ARRAY`.

### `ARRAY_LENGTH`

```sql
ARRAY_LENGTH(ARRAY[1, 2, 3])
```

Given an array, return the number of elements in the array.

If the supplied parameter is NULL the method returns NULL.

### ``ARRAY_MAX``

```sql
ARRAY_MAX(['foo', 'bar', 'baz'])
```

Returns the maximum value from within a given array of primitive elements (not arrays of other arrays, or maps, or structs, or combinations thereof). 

Array entries are compared according to their natural sort order, which sorts the various data-types per the following examples:
- ```array_max[-1, 2, NULL, 0] -> 2```
- ```array_max[false, NULL, true] -> true```
- ```array_max['Foo', 'Bar', NULL, 'baz'] -> 'baz'``` (lower-case characters are "greater" than upper-case characters)

If the array field is NULL, or contains only NULLs, then NULL is returned.

### ``ARRAY_MIN``

```sql
ARRAY_MIN(['foo', 'bar', 'baz'])
```

Returns the minimum value from within a given array of primitive elements (not arrays of other arrays, or maps, or structs, or combinations thereof). 

Array entries are compared according to their natural sort order, which sorts the various data-types per the following examples:
- ```array_min[-1, 2, NULL, 0] -> -1```
- ```array_min[false, NULL, true] -> false```
- ```array_min['Foo', 'Bar', NULL, 'baz'] -> 'Bar'```

If the array field is NULL, or contains only NULLs, then NULL is returned.

### ``ARRAY_SORT``

```sql
ARRAY_SORT(['foo', 'bar', 'baz'], 'ASC|DESC')
```

Given an array of primitive elements (not arrays of other arrays, or maps, or structs, or combinations thereof), returns an array of the same elements sorted according to their natural sort order. Any NULLs contained in the array will always be moved to the end.

For example:
- ```array_sort[-1, 2, NULL, 0] -> [-1, 0, 2, NULL]```
- ```array_sort[false, NULL, true] -> [false, true, NULL]```
- ```array_sort['Foo', 'Bar', NULL, 'baz'] -> ['Bar', 'Foo', 'baz', NULL]```

If the array field is NULL then NULL is returned.

An optional second parameter can be used to specify whether to sort the elements in 'ASC'ending or 'DESC'ending order. If neither is specified then the default is ascending order. 

### `AS_MAP`

```sql
AS_MAP(keys, vals)
```

Construct a map from a list of keys and a list of values.

### `ELT`

```sql
ELT(n INTEGER, args VARCHAR[])
```

Returns element `n` in the `args` list of strings, or NULL if `n` is less than
1 or greater than the number of arguments. This function is 1-indexed. ELT is
the complement to FIELD.

### `FIELD`

```sql
FIELD(str VARCHAR, args VARCHAR[])
```

Returns the 1-indexed position of `str` in `args`, or 0 if not found.
If `str` is NULL, the return value is 0, because NULL is not considered
to be equal to any value. FIELD is the complement to ELT.

### `JSON_ARRAY_CONTAINS`

```sql
JSON_ARRAY_CONTAINS('[1, 2, 3]', 3)
```

Given a `STRING` containing a JSON array, checks if a search value is contained in the array.

Returns `false` if the first parameter does not contain a JSON array.

### `MAP`

```sql
MAP(key VARCHAR := value, ...)
```

Construct a map from specific key-value tuples.

### `MAP_KEYS`

```sql
MAP_KEYS(a_map)
```

Returns an array that contains all of the keys from the specified map.

Returns NULL if the input map is NULL.

Example:
```sql
map_keys( map('apple' := 10, 'banana' := 20) )  => ['apple', 'banana'] 
```

### `MAP_VALUES`

```sql
MAP_VALUES(a_map)
```

Returns an array that contains all of the values from the specified map.

Returns NULL if the input map is NULL.

Example:
```sql
map_values( map('apple' := 10, 'banana' := 20) )  => [10, 20] 
```

### `MAP_UNION`

```sql
MAP_UNION(map1, map2)
```

Returns a new map containing the union of all entries from both input maps. If a key is present in both input maps then the corresponding value from _map2_ is the one which appears in the output.

Returns NULL if all of the input maps are NULL.

Example:
```sql
map_union( map('apple' := 10, 'banana' := 20), map('cherry' := 99) )  => ['apple': 10, 'banana': 20, 'cherry': 99] 

map_union( map('apple' := 10, 'banana' := 20), map('apple' := 50) )  => ['apple': 50, 'banana': 20] 
```

### `SLICE`

```sql
SLICE(col1, from, to)
```

Slices a list based on the supplied indices. The indices start at 1 and
include both endpoints.

## Strings

### `CONCAT`

```sql
CONCAT(col1, col2, 'hello', ..., col-n)
```

Concatenate two or more string expressions. Any input strings which evaluate to NULL are replaced with empty string in the output.

### `EXTRACTJSONFIELD`

```sql
EXTRACTJSONFIELD(message, '$.log.cloud')
```

Given a STRING that contains JSON data, extract the value at the specified [JSONPath](https://jsonpath.com/).

For example, given a STRING containing the following JSON:

```json
{
   "log": {
      "cloud": "gcp836Csd",
      "app": "ksProcessor",
      "instance": 4
   }
}
```

`EXTRACTJSONFIELD(message, '$.log.cloud')` returns the STRING `gcp836Csd`.

If the requested JSONPath does not exist, the function returns NULL.

The result of EXTRACTJSONFIELD is always a STRING. Use `CAST` to convert the result to another
type. For example, `CAST(EXTRACTJSONFIELD(message, '$.log.instance') AS INT)` will extract the
instance number from the above JSON object as a INT.

!!! note
    EXTRACTJSONFIELD is useful for extracting data from JSON where either the schema of the JSON
    data is not static, or where the JSON data is embedded in a row encoded using a different
    format, for example, a JSON field within an Avro-encoded message.

    If the whole row is encoded as JSON with a known schema or structure, use the `JSON` format and

    define the structure as the source's columns.  For example, a stream of JSON objects similar to
    the example above could be defined using a statement similar to this:

    `CREATE STREAM LOGS (LOG STRUCT<CLOUD STRING, APP STRING, INSTANCE INT, ...) WITH (VALUE_FORMAT=JSON, ...)`

### `INITCAP`

```sql
INITCAP(col1)
```

Capitalize the first letter in each word and convert all other letters
to lowercase. Words are delimited by whitespace.

### `INSTR`

```sql
INSTR(string, substring, [position], [occurrence])
```

Returns the position of `substring` in `string`. The first character is at position 1.

If `position` is provided, search starts from the specified position. 
Negative `position` causes the search to work from end to start of `string`.

If `occurrence` is provided, the position of *n*-th occurrence is returned.

If `substring` is not found, the return value is 0.

Examples:

```sql
INSTR('CORPORATE FLOOR', 'OR') -> 2
INSTR('CORPORATE FLOOR', 'OR', 3) -> 5
INSTR('CORPORATE FLOOR', 'OR', 3, 2) -> 14
INSTR('CORPORATE FLOOR', 'OR', -3) -> 5
INSTR('CORPORATE FLOOR', 'OR', -3, 2) -> 2b
INSTR('CORPORATE FLOOR', 'MISSING') -> 0
```

### `LCASE`

```sql
LCASE(col1)
```

Convert a string to lowercase.

### `LEN`

```sql
LEN(col1)
```

The length of a string.

### `MASK`

```sql
MASK(col1, 'X', 'x', 'n', '-')
```

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

### `MASK_KEEP_LEFT`

```sql
MASK_KEEP_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function above, except
that the first or left-most `numChars`
characters will not be masked in any way.
For example: `MASK_KEEP_LEFT("My Test $123", 4)`
will return `My Txxx--nnn`.

### `MASK_KEEP_RIGHT`

```sql
MASK_KEEP_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function above, except
that the last or right-most `numChars`
characters will not be masked in any way.
For example:`MASK_KEEP_RIGHT("My Test $123", 4)`
will return `Xx-Xxxx-$123`.

### `MASK_LEFT`

```sql
MASK_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function above, except
that only the first or left-most `numChars`
characters will have any masking applied to them.
For example, `MASK_LEFT("My Test $123", 4)`
will return `Xx-Xest $123`.

### `MASK_RIGHT`

```sql
MASK_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function above, except
that only the last or right-most `numChars`
characters will have any masking applied to them.
For example: `MASK_RIGHT("My Test $123", 4)`
will return `My Test -nnn`.

### `REPLACE`

```sql
REPLACE(col1, 'foo', 'bar')
```

Replace all instances of a substring in a string with a new string.

### `REGEXP_EXTRACT`

```sql
REGEXP_EXTRACT('.*', col1)
```

```sql
REGEXP_EXTRACT('(([AEIOU]).)', col1, 2)
```

Extract the first subtring matched by the regex pattern from the input.

A capturing group number can also be specified in order to return that specific group. If a number isn't specified,
the entire substring is returned by default.

For example, `REGEXP_EXTRACT("(.*) (.*)", 'hello there', 2)`
returns "there".

### `REGEXP_EXTRACT_ALL`

```sql
REGEXP_EXTRACT_ALL('.*', col1)
```

```sql
REGEXP_EXTRACT_ALL('(([AEIOU]).)', col1, 2)
```

Extract all subtrings matched by the regex pattern from the input.

A capturing group number can also be specified in order to return that specific group. If a number isn't specified,
the entire substring is returned by default.

For example, `REGEXP_EXTRACT("(\\w+) (\\w+)", 'hello there nice day', 2)`
returns `['there', 'day']`.

### `REGEXP_REPLACE`

```sql
REGEXP_REPLACE(col1, 'a.b+', 'bar')
```

Replace all matches of a regex in an input string with a new string.
If either the input string, regular expression, or new string is null,
the result is null.

### `REGEXP_SPLIT_TO_ARRAY`

```sql
REGEXP_SPLIT_TO_ARRAY(col1, 'a.b+')
```

Splits a string into an array of substrings based
on a regular expression. If there is no match,
the original string is returned as the only
element in the array. If the regular expression is empty,
then all characters in the string are split.
If either the string or the regular expression is `NULL`, a
NULL value is returned.

If the regular expression is found at the beginning or end
of the string, or there are contiguous matches,
then an empty element is added to the array.

### `SPLIT`

```sql
SPLIT(col1, delimiter)
```

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

### `SUBSTRING`

```sql
SUBSTRING(col1, 2, 5)
```

```sql
SUBSTRING(str, pos, [len])
```

Returns a substring of `str` that starts at
`pos` (first character is at position 1) and
has length `len`, or continues to the end of
the string.

For example, `SUBSTRING("stream", 1, 4)`
returns "stre".

### `TRIM`

```sql
TRIM(col1)
```

Trim the spaces from the beginning and end of a string.

### `UCASE`

```sql
UCASE(col1)
```

Convert a string to uppercase.

## Nulls

### `COALESCE`

```sql
COALESCE(a, b, c, d)
```

Returns the first parameter that is not NULL. All parameters must be of the same type.

Where the parameter type is a complex type, for example `ARRAY` or `STRUCT`, the contents of the
complex type are not inspected. The behaviour is the same: the first NOT NULL element is returned.

### `IFNULL`

```sql
IFNULL(expression, altValue)
```

If the provided `expression` is NULL, returns `altValue`, otherwise, returns `expression`.

Where the parameter type is a complex type, for example `ARRAY` or `STRUCT`, the contents of the
complex type are not inspected.

## Date and time

### `UNIX_DATE`

```sql
UNIX_DATE()
```
 
Gets an integer representing days since epoch. The returned timestamp
may differ depending on the local time of different ksqlDB Server instances.

### `UNIX_TIMESTAMP`

```sql
UNIX_TIMESTAMP()
```

Gets the Unix timestamp in milliseconds, represented as a BIGINT. The returned
timestamp may differ depending on the local time of different ksqlDB Server instances.

### `DATETOSTRING`

```sql
DATETOSTRING(START_DATE, 'yyyy-MM-dd')
```

Converts an integer representation of a date into a string representing the
date in the given format. Single quotes in the timestamp format can be escaped
with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.
The integer represents days since epoch matching the encoding used by
{{ site.kconnect }} dates.

### `STRINGTODATE`

```sql
STRINGTODATE(col1, 'yyyy-MM-dd')
```

Converts a string representation of a date in the
given format into an integer representing days
since epoch. Single quotes in the timestamp
format can be escaped with two successive single
quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

### `STRINGTOTIMESTAMP`

```sql
STRINGTOTIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])
```

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

### `TIMESTAMPTOSTRING`

```sql
TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])
```

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

## URLs

!!! note
    All ksqlDB URL functions assume URI syntax defined in
    [RFC 39386](https://tools.ietf.org/html/rfc3986). For more information on the
    structure of a URI, including definitions of the various components, see
    Section 3 of the RFC. For encoding/decoding, the
    `application/x-www-form-urlencoded` convention is followed.

### `URL_DECODE_PARAM`

```sql
URL_DECODE_PARAM(col1)
```

Unescapes the `URL-param-encoded`_ value in `col1`. This is the inverse of
`URL_ENCODE_PARAM`.

- Input: `'url%20encoded`
- Output: `url encoded`

### `URL_ENCODE_PARAM`

```sql
URL_ENCODE_PARAM(col1)
```

Escapes the value of `col1` such that it can
safely be used in URL query parameters. Note that
this is not the same as encoding a value for use 
in the path portion of a URL.                    
                                                 
- Input: `url encoded`                             
- Output: `'url%20encoded`                         

### `URL_EXTRACT_FRAGMENT`

```sql
URL_EXTRACT_FRAGMENT(url)
```

Extract the fragment portion of the specified
value. Returns NULL if `url` is not a valid URL
or if the fragment does not exist. Any encoded 
value will be decoded.                         
                                               
- Input: `http://test.com#frag`,                 
- Output: `frag`                                 

- Input: `http://test.com#frag%20space`,         
- Output: `frag space`                           

### `URL_EXTRACT_HOST`

```sql
URL_EXTRACT_HOST(url)
```

Extract the host-name portion of the specified
value. Returns NULL if the `url` is not a valid  
URI according to RFC-2396.                       
                                                 
- Input: `http://test.com:8080/path`,              
- Output: `test.com`                               

### `URL_EXTRACT_PARAMETER`

```sql
URL_EXTRACT_PARAMETER(url, parameter_name)
```

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

### `URL_EXTRACT_PATH`

```sql
URL_EXTRACT_PATH(url)
```

Extracts the path from `url`.
Returns NULL if `url` is not a valid URI but  
returns an empty string if the path is empty. 
                                              
- Input: `http://test.com/path/to#a`            
- Output: `path/to`                             

### `URL_EXTRACT_PORT`

```sql
URL_EXTRACT_PORT(url)
```

Extract the port number from `url`.
Returns NULL if `url` is not a valid URI or does
not contain an explicit port number.            
                                                
- Input: `http://localhost:8080/path`             
- Output: `8080`                                  

### `URL_EXTRACT_PROTOCOL`

```sql
URL_EXTRACT_PROTOCOL(url)
```

Extract the protocol from `url`. Returns NULL if
`url` is an invalid URI or has no protocol.
                                           
- Input: `http://test.com?a=foo&b=bar`       
- Output: `http`                             

### `URL_EXTRACT_QUERY`

```sql
URL_EXTRACT_QUERY(url)
```

Extract the decoded query-string portion of
`url`. Returns NULL if no query-string is  
present or `url` is not a valid URI.       
                                           
- Input: `http://test.com?a=foo%20bar&b=baz` 
- Output: `a=foo bar&b=baz`                  
