---
layout: page
title: ksqlDB Scalar Functions
tagline:  ksqlDB scalar functions for queries
description: Scalar functions to use in SQL statements and queries
keywords: ksqlDB, SQL, function, scalar
---

## Numeric functions

### `ABS`

Since: 0.1.0

```sql
ABS(col1)
```

Returns the absolute value of a value.

### `AS_VALUE`

Since: 0.9.0

```sql
AS_VALUE(keyCol)
```

Creates a copy of a key column in the value.

For example:

```sql
CREATE TABLE AGG AS
   SELECT 
     ID,                  -- this is the grouping column and will be stored in the message key.
     AS_VALUE(ID) AS ID2  -- this creates a copy of ID, called ID2, stored in the message value.
     COUNT(*) AS COUNT
   FROM S
   GROUP BY ID;
```

!!! Tip "See AS_VALUE in action"
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#ksqldb-code)

### `CAST`

Since: 0.1.0

```sql
CAST(COL0 AS BIGINT)
```

Converts one type to another. The following casts are supported:

| from | to | notes |
|------|----|-------|
| any except `BYTES` | `STRING` | Converts the type to its string representation. |
| `VARCHAR` | `BOOLEAN` | Any string that exactly matches `true`, case-insensitive, is converted to `true`. Any other value is converted to `false`. |
| `VARCHAR` | `INT`, `BIGINT`, `DECIMAL`, `DOUBLE` | Converts string representation of numbers to number types. Conversion will fail if text does not contain a number or the number does not fit in the indicated type. |
| `VARCHAR` | `TIME` | Converts time strings to `TIME`. Conversion fails if text is not in `HH:mm:ss` format.     |
| `VARCHAR` | `DATE` | Converts date strings to `DATE`. Conversion fails if text is not in `yyyy-MM-dd` format.     |
| `VARCHAR` | `TIMESTAMP` | Converts datestrings to `TIMESTAMP`. Conversion fails if text is not in ISO-8601 format.     |
| `TIMESTAMP` | `TIME`, `DATE` | Converts a `TIMESTAMP` to `TIME` or `DATE` by extracting the time or date portion of the `TIMESTAMP`.|
| `DATE` | `TIMESTAMP` | Converts a `DATE` to `TIMESTAMP` by setting the time portion to `00:00:00.000` |
| `INT`, `BIGINT`, `DECIMAL`, `DOUBLE` | `INT`, `BIGINT`, `DECIMAL`, `DOUBLE` | Convert between numeric types. Conversion can result in rounding |
| `ARRAY` | `ARRAY` | (Since 0.14) Convert between arrays of different element types |   
| `MAP` | `MAP` | (Since 0.14) Convert between maps of different key and value types |   
| `STRUCT` | `STRUCT` | (Since 0.14) Convert between structs of different field types. Only fields that exist in the target STRUCT type are copied across. Any fields in the target type that don't exist in the source are set to `NULL`. Field name matching is case-sensitive. |

!!! Tip "See CAST in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#ksqldb-code)
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#ksqldb-code)

### `CEIL`

Since: 0.1.0

```sql
CEIL(col1)
```

Returns the the smallest integer value that's greater than or equal to `col1`.

### `ENTRIES`

Since: 0.6.0

```sql
ENTRIES(map MAP, sorted BOOLEAN)
```

Constructs an array of structs from the entries in a map. Each struct has
a field named `K` containing the key, which is a string, and a field named
`V`, which holds the value.

If `sorted` is true, the entries are sorted by key.

### `EXP`

Since: 0.6.0

```sql
EXP(col1)
```

Returns the exponential of `col1`, which is _e_ raised to the power of `col1`. 

### `FLOOR`

Since: 0.1.0

```sql
FLOOR(col1)
```

Returns the largest integer value that's less than or equal to `col1`.

### `GENERATE_SERIES`

Since: 0.6.0

```sql
GENERATE_SERIES(start, end)
GENERATE_SERIES(start, end, step)
```

Constructs an array of values between `start` and `end`, inclusive.

Parameters `start` and `end` can be an `INT` or `BIGINT`.

`step`, if supplied, specifies the step size. The step can be positive or negative.
If not supplied, `step` defaults to `1`. Parameter `step` must be an `INT`.

### `GEO_DISTANCE`

Since: 0.6.0

```sql
GEO_DISTANCE(lat1, lon1, lat2, lon2, unit)
```

The great-circle distance between two lat-long points, both specified
in decimal degrees. An optional final parameter specifies `KM`
(the default) or `miles`.

### `GREATEST`

Since: 0.20.0

```sql
GREATEST(col1, col2, …)
```

Returns the largest non-null value from a variable number of comparable columns.

If comparing columns of different numerical types, use [CAST](#cast) to first
cast them to be of the same type.

### `LEAST`

Since: 0.20.0

```sql
LEAST(col1, col2, …)
```

Returns the smallest non-null value from a variable number of comparable columns.

If comparing columns of different numerical types, use [CAST](#cast) to first
cast them to be of the same type.

### `LN`

Since: 0.6.0

```sql
LN(col1)
```

Returns the natural logarithm of `col1`, which is .

The value of `col1` must be greater than 0.

### `RANDOM`

Since: 0.1.0

```sql
RANDOM()
```

Returns a random `DOUBLE` value between 0.0 and 1.0.

### `ROUND`

Since: 0.1.0

```sql
ROUND(col1)
ROUND(col1, scale)
```

Rounds a value to the number of decimal places specified by `scale`.

If `scale` is negative, the value is rounded to the right of the decimal point.

Numbers equidistant to the nearest value are rounded up, in the positive
direction.

If the number of decimal places is not provided, it defaults to zero.

### `SIGN`

Since: 0.6.0

```sql
SIGN(col1)
```

Returns the sign of `col1` as an `INTEGER`:

* -1 if the argument is negative
* 0 if the argument is zero
* 1 if the argument is positive
* `null` argument is `null`

### `SQRT`

Since: 0.6.0

```sql
SQRT(col1)
```

Returns the square root of `col`.

## Collections

### `ARRAY`

Since: 0.7.0

```sql
ARRAY[exp1, exp2, …]
```

Constructs an array from a variable number of inputs.

All elements must be coercible to a common SQL type.
For more information, see
[Implicit type coercion](type-coercion.md#implicit-type-coercion).

### `ARRAY_CONCAT`

Since: 0.21.0

```sql
ARRAY_CONCAT(array1, array2)
```

Returns an array representing the concatenation of both input arrays.

Returns `NULL` if both input arrays are `NULL`. If only one argument is `NULL`,
the result is the other argument.

Examples:

```sql 
ARRAY_CONCAT(ARRAY[1, 2, 3, 1, 2], [4, 1])  => [1, 2, 3, 1, 2, 4, 1]
ARRAY_CONCAT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', 'apple', NULL, 'cherry', 'cherry']
```

### `ARRAY_CONTAINS`

Since: 0.6.0

```sql
ARRAY_CONTAINS(ARRAY[1, 2, 3], 3)
```

Given an array, checks if a search value is contained in the array.

Accepts any `ARRAY` type. The type of the second param must match the element
type of the `ARRAY`.

!!! Tip "See ARRAY_CONTAINS in action"
    - [Build Customer Loyalty Programs](https://developer.confluent.io/tutorials/loyalty-rewards/confluent.html#ksqldb-code)

### `ARRAY_DISTINCT`

Since: 0.10.0

```sql
ARRAY_DISTINCT([1, 2, 3])
```

Returns an array of all the distinct values, including `NULL` if present,
from the input array.

The output array elements are in order of their first occurrence in the input.

Returns `NULL` if the input array is `NULL`.

Examples:
```sql 
ARRAY_DISTINCT(ARRAY[1, 1, 2, 3, 1, 2])  => [1, 2, 3]
ARRAY_DISTINCT(ARRAY['apple', 'apple', NULL, 'cherry'])  => ['apple', NULL, 'cherry']
```

### `ARRAY_EXCEPT`

Since: 0.10.0

```sql
ARRAY_EXCEPT(array1, array2)
```

Returns an array of all the distinct elements from an array, except for those
also present in a second array.

The order of entries in the first array is preserved but duplicates are removed. 

Returns `NULL` if either input is `NULL`.

Examples:
```sql 
ARRAY_EXCEPT(ARRAY[1, 2, 3, 1, 2], [2, 3])  => [1]
ARRAY_EXCEPT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', NULL]
```

### `ARRAY_INTERSECT`

Since: 0.10.0

```sql
ARRAY_INTERSECT(array1, array2)
```

Returns an array of all the distinct elements from the intersection of both
input arrays.

The order of entries in the output is the same as in the first input array.

Returns `NULL` if either input array is `NULL`.

Examples:
```sql 
ARRAY_INTERSECT(ARRAY[1, 2, 3, 1, 2], [2, 1])  => [1, 2]
ARRAY_INTERSECT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['apple'])  => ['apple']
```

### `ARRAY_JOIN`

Since: 0.10.0

```sql
ARRAY_JOIN(col1, delimiter)
```

Creates a flat string representation of all the elements contained in an
array.

The elements in the resulting string are separated by the chosen `delimiter`, 
which is an optional parameter. The default is the comma character, `,`.

Array elements are limited to primitive ksqlDB types only.

!!! Tip "See ARRAY_JOIN in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#ksqldb-code)

### `ARRAY_LENGTH`

Since: 0.8.0

```sql
ARRAY_LENGTH(ARRAY[1, 2, 3])
```

Returns the number of elements in an array.

If the supplied parameter is `NULL`, the method returns `NULL`.

### `ARRAY_MAX`

Since: 0.10.0

```sql
ARRAY_MAX(['foo', 'bar', 'baz'])
```

Returns the maximum value from an array of primitive elements.

Arrays of other arrays, arrays of maps, arrays of structs, or combinations
of these types aren't supported.

Array entries are compared according to their natural sort order, which sorts
the various data types per the following examples:

- ```array_max[-1, 2, NULL, 0] -> 2```
- ```array_max[false, NULL, true] -> true```
- ```array_max['Foo', 'Bar', NULL, 'baz'] -> 'baz'``` (lower-case characters are "greater" than upper-case characters)

If the array field is `NULL`, or contains only `NULL` values, `NULL` is
returned.

### `ARRAY_MIN`

Since: 0.10.0

```sql
ARRAY_MIN(['foo', 'bar', 'baz'])
```

Returns the minimum value from an array of primitive elements.

Arrays of other arrays, arrays of maps, arrays of structs, or combinations
of these types aren't supported. 

Array entries are compared according to their natural sort order, which sorts
the various data types per the following examples:

- ```array_min[-1, 2, NULL, 0] -> -1```
- ```array_min[false, NULL, true] -> false```
- ```array_min['Foo', 'Bar', NULL, 'baz'] -> 'Bar'```

If the array field is `NULL`, or contains only `NULL` values, `NULL` is
returned.

### `ARRAY_REMOVE`

Since: 0.11.0

```sql
ARRAY_REMOVE(array, element)
```

Removes all elements from `array` that are equal to `element`.

If the `array` field is `NULL`, `NULL` is returned.

Examples:

```sql
ARRAY_REMOVE([1, 2, 3, 2, 1], 2) => [1, 3, 1]
ARRAY_REMOVE([false, NULL, true, true], false) => [NULL, true, true]
ARRAY_REMOVE(['Foo', 'Bar', NULL, 'baz'], null) => ['Foo', 'Bar', 'baz']
```

### `ARRAY_SORT`

Since: 0.10.0

```sql
ARRAY_SORT(['foo', 'bar', 'baz'], 'ASC|DESC')
```

Given an array of primitive elements, returns an array of the same elements
sorted according to their natural sort order.

Arrays of other arrays, arrays of maps, arrays of structs, or combinations
of these types aren't supported.

Any `NULL` values in the array are moved to the end.

If the array field is `NULL`, `NULL` is returned.

The optional second parameter specifies whether to sort the elements in ascending
(`ASC`) or descending (`DESC`) order. If neither is specified, the default is
ascending order.

Examples:

```sql
ARRAY_SORT[-1, 2, NULL, 0] -> [-1, 0, 2, NULL]
ARRAY_SORT[false, NULL, true] -> [false, true, NULL]
ARRAY_SORT['Foo', 'Bar', NULL, 'baz'] -> ['Bar', 'Foo', 'baz', NULL]
```

!!! Tip "See ARRAY_SORT in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#ksqldb-code)

### `ARRAY_UNION`

Since: 0.10.0

```sql
ARRAY_UNION(array1, array2)
```

Returns an array of all the distinct elements from both input arrays, in the
order they're encountered.

Returns `NULL` if either input array is `NULL`.

Examples:
```sql 
ARRAY_UNION(ARRAY[1, 2, 3, 1, 2], [4, 1])  => [1, 2, 3, 4]
ARRAY_UNION(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', NULL, 'cherry']
```

### `AS_MAP`

Since: 0.6.0

```sql
AS_MAP(keys, vals)
```

Constructs a map from a list of keys and a list of values.

!!! Tip "See AS_MAP in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#ksqldb-code)

### `ELT`

Since: 0.6.0

```sql
ELT(n INTEGER, args VARCHAR[])
```

Returns element `n` in the `args` list of strings, or `NULL` if `n` is less than
1 or greater than the number of arguments.

The `ELT` function is 1-indexed.

`ELT` is the complement to the `FIELD` function.

### `FIELD`

Since: 0.6.0

```sql
FIELD(str VARCHAR, args VARCHAR[])
```

Returns the 1-indexed position of `str` in `args`, or 0 if not found.

If `str` is `NULL`, the return value is 0, because `NULL` isn't considered
to be equal to any value.

`FIELD` is the complement to the `ELT` function.

### `JSON_ARRAY_CONTAINS`

Since: 0.6.0

```sql
JSON_ARRAY_CONTAINS('[1, 2, 3]', 3)
```

Given a `STRING` containing a JSON array, checks if a search value is contained
in the array.

Returns `false` if the first parameter doesn't contain a JSON array.

### `MAP`

Since: 0.7.0

```sql
MAP(key VARCHAR := value, …)
```

Constructs a map from specific key-value tuples.

All values must be coercible to a common SQL type.

For more information, see
[Implicit type coercion](type-coercion.md#implicit-type-coercion).

### `MAP_KEYS`

Since: 0.10.0

```sql
MAP_KEYS(a_map)
```

Returns an array that contains all keys from the specified map.

Returns `NULL` if the input map is `NULL`.

Example:

```sql
map_keys( map('apple' := 10, 'banana' := 20) )  => ['apple', 'banana'] 
```

### `MAP_VALUES`

Since: 0.10.0

```sql
MAP_VALUES(a_map)
```

Returns an array that contains all values from the specified map.

Returns `NULL` if the input map is `NULL`.

Example:

```sql
map_values( map('apple' := 10, 'banana' := 20) )  => [10, 20] 
```

### `MAP_UNION`

Since: 0.10.0

```sql
MAP_UNION(map1, map2)
```

Returns a new map containing the union of all entries from both input maps.

If a key is present in both input maps, the corresponding value from _map2_
is returned.

Returns `NULL` if all input maps are `NULL`.

Example:

```sql
map_union( map('apple' := 10, 'banana' := 20), map('cherry' := 99) )  => ['apple': 10, 'banana': 20, 'cherry': 99] 

map_union( map('apple' := 10, 'banana' := 20), map('apple' := 50) )  => ['apple': 50, 'banana': 20] 
```

### `SLICE`

Since: 0.6.0

```sql
SLICE(col1, from, to)
```

Slices a list based on the supplied indices.

The indices start at 1 and include both endpoints.

## Invocation Functions

Apply lambda functions to collections.

### `FILTER`

Since: 0.17.0

```sql
FILTER(array, x => …)

FILTER(map, (k,v) => …)
```

Filters a collection with a lambda function.

If the collection is an array, the lambda function must have one input argument.

If the collection is a map, the lambda function must have two input arguments.

### `REDUCE`

Since: 0.17.0

```sql
REDUCE(array, state, (s, x) => …)

REDUCE(map, state, (s, k, v) => …)
```

Reduces a collection starting from an initial state.

If the collection is an array, the lambda function must have two input arguments.

If the collection is a map, the lambda function must have three input arguments.

If the state is `NULL`, the result is `NULL`.

### `TRANSFORM`

Since: 0.17.0

```sql
TRANSFORM(array, x => …)

TRANSFORM(map, (k,v) => …, (k,v) => …)
```

Transforms a collection by using a lambda function.

If the collection is an array, the lambda function must have one input argument.

If the collection is a map, two lambda functions must be provided, and both
lambdas must have two arguments: a map entry key and a map entry value.

## Strings

### `CHR`

Since: 0.10.0

```sql
CHR(decimal_code | utf_string)
```

Returns a single-character string representing the Unicode code-point described
by the input.

The input parameter can be either a decimal character code or a string
representation of a UTF code.

Returns `NULL` if the input is `NULL` or doesn't represent a valid code-point.

Commonly used to insert control characters such as `Tab` (9), `Line Feed` (10),
or `Carriage Return` (13) into strings.

Examples:

```sql
CHR(75)        => 'K'
CHR('\u004b')  => 'K'
CHR(22909)     => '好'
CHR('\u597d')  => '好'
```

### `CONCAT`

Since: 0.1.0

```sql
CONCAT(col1, col2, 'hello', …, col-n)
CONCAT(bytes1, bytes2, …, bytes-n)
```

Concatenates two or more string or bytes expressions.

Any inputs which evaluate to `NULL` are replaced with an empty string or bytes
in the output.

!!! Tip "See CONCAT in action"
    - [Enrich orders with change data capture (CDC)](https://developer.confluent.io/tutorials/denormalization/confluent.html#ksqldb-code)

### `CONCAT_WS`

Since: 0.10.0

```sql
CONCAT_WS(separator, expr1, expr2, …)
```

Concatenates two or more string or bytes expressions, inserting a separator
string or bytes between each.

If the separator is `NULL`, this function returns `NULL`.

Any expressions which evaluate to `NULL` are skipped.

Example:

```sql
CONCAT_WS(', ', 'apple', 'banana', NULL, 'date')  ->  'apple, banana, date'
```

### `ENCODE`

Since: 0.10.0

```sql
ENCODE(col1, input_encoding, output_encoding)
```

Given a STRING that is encoded as `input_encoding`, encode it using the
`output_encoding`.

The accepted input and output encodings are:

- `hex`
- `utf8`
- `ascii`
- `base64`

Throws an exception if the provided encodings are not supported.

The following example encodes a `hex` representation of a string to a
`utf8` representation.

```sql
ENCODE(string, 'hex', 'utf8')
```

### `EXTRACTJSONFIELD`

Since: 0.11.0

```sql
EXTRACTJSONFIELD(message, '$.log.cloud')
```

Given a `STRING` that contains JSON data, extracts the value at the specified
[JSONPath](https://jsonpath.com/).

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

If the requested JSONPath does not exist, the function returns `NULL`.

The result of `EXTRACTJSONFIELD` is always a `STRING`. Use `CAST` to convert
the result to another type.

For example, `CAST(EXTRACTJSONFIELD(message, '$.log.instance') AS INT)`
extracts the instance number from the previous JSON object as a `INT`.

The return type of `EXTRACTJSONFIELD` is `STRING`, so JSONPaths that select
multiple elements, like those containing wildcards, aren't supported.

!!! note
    `EXTRACTJSONFIELD` is useful for extracting data from JSON when either the
    schema of the JSON data isn't static or the JSON data is embedded in a row
    that's encoded using a different format, for example, a JSON field within
    an Avro-encoded message.

    If the whole row is encoded as JSON with a known schema or structure, use
    the `JSON` format and define the structure as the source's columns.
    
    For example, a stream of JSON objects similar to the previous example could
    be defined using a statement similar to the following:

    ```sql
    CREATE STREAM LOGS (LOG STRUCT<CLOUD STRING, APP STRING, INSTANCE INT>, …)
      WITH (VALUE_FORMAT='JSON', …)
    ```

### `FROM_BYTES`

Since: 0.21.0

```sql
FROM_BYTES(bytes, encoding)
```

Converts a `BYTES` column to a `STRING` in the specified encoding type.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

### `INITCAP`

Since: 0.6.0

```sql
INITCAP(col1)
```

Capitalizes the first letter in each word and converts all other letters
to lowercase.

Words are delimited by whitespace.

### `INSTR`

Since: 0.10.0

```sql
INSTR(string, substring, [position], [occurrence])
```

Returns the position of `substring` in `string`.

The first character is at position 1.

If `position` is provided, search starts from the specified position.

A negative value for `position` causes the search to work from the end to the
start of `string`.

If `occurrence` is provided, the position of the *n*-th occurrence is returned.

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

Since: 0.1.0

```sql
LCASE(col1)
```

Converts a string to lowercase.

### `LEN`

Since: 0.1.0

```sql
LEN(string)
LEN(bytes)
```

Returns the length of a `STRING` or the number of bytes in a `BYTES` value.

### `LPAD`

Since: 0.10.0

```sql
LPAD(input, length, padding)
```

Pads the input string or bytes, beginning from the left, with the specified
padding of the same type, until the target length is reached.

If the input is longer than `length`, it is truncated.

If the padding string or byte array is empty or `NULL`, or the target length
is negative, `NULL` is returned.

Examples:

```sql
LPAD('Foo', 7, 'Bar')  =>  'BarBFoo'
LPAD('Foo', 2, 'Bar')  =>  'Fo'
LPAD('', 2, 'Bar')  =>  'Ba'
LPAD('123', 5, '0')  => '00123'
```

### `MASK`

Since: 0.6.0

```sql
MASK(col1, 'X', 'x', 'n', '-')
```

Convert a string to a masked or obfuscated version of itself.

The optional arguments following the input string to be masked are the
characters to be substituted for upper-case, lower-case, numeric, and
other characters of the input, respectively.

If the mask characters are omitted, the default values are applied, as shown
in the following example.

```sql
MASK("My Test $123") => "Xx-Xxxx--nnn"
```

Set a given mask character to `NULL` to prevent any masking of that character
type. 

```sql
MASK("My Test $123", '*', NULL, '1', NULL) => "*y *est $111" 
```

### `MASK_KEEP_LEFT`

Since: 0.6.0

```sql
MASK_KEEP_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that the first or left-most `numChars`
characters aren't masked in any way.

Example:

```sql
MASK_KEEP_LEFT("My Test $123", 4) => "My Txxx--nnn"
```

### `MASK_KEEP_RIGHT`

Since: 0.6.0

```sql
MASK_KEEP_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that the last or right-most
`numChars` characters aren't masked in any way.

Example:

```sql
MASK_KEEP_RIGHT("My Test $123", 4) => "Xx-Xxxx-$123"
```

### `MASK_LEFT`

Since: 0.6.0

```sql
MASK_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that only the first or left-most
`numChars` characters have any masking applied to them.

Example:

```sql
MASK_LEFT("My Test $123", 4) => "Xx-Xest $123"
```
### `MASK_RIGHT`

Since: 0.6.0

```sql
MASK_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that only the last or right-most
`numChars` characters have any masking applied to them.

Example:

```sql
MASK_RIGHT("My Test $123", 4) => "My Test -nnn"
```

### `REPLACE`

Since: 0.6.0

```sql
REPLACE(col1, 'foo', 'bar')
```

Replaces all instances of a substring in a string with a new string.

!!! Tip "See REPLACE in action"
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#ksqldb-code)

### `REGEXP_EXTRACT`

Since: 0.8.0

```sql
REGEXP_EXTRACT('.*', col1)
REGEXP_EXTRACT('(([AEIOU]).)', col1, 2)
```

Extracts the first substring matched by the regular expression pattern from the
input.

You can specify a capturing group number to return that specific group. If a
number isn't specified, the entire substring is returned by default.

Example:

```sql
REGEXP_EXTRACT("(.*) (.*)", 'hello there', 2) => "there"
```

### `REGEXP_EXTRACT_ALL`

Since: 0.10.0

```sql
REGEXP_EXTRACT_ALL('.*', col1)
REGEXP_EXTRACT_ALL('(([AEIOU]).)', col1, 2)
```

Extracts all subtrings matched by the regular expression pattern from the input.

You can specify a capturing group number to return that specific group. If a
number isn't specified, the entire substring is returned by default.

Example:

```sql
REGEXP_EXTRACT("(\\w+) (\\w+)", "hello there nice day", 2) => ["there", "day"]
```

### `REGEXP_REPLACE`

Since: 0.10.0

```sql
REGEXP_REPLACE(col1, 'a.b+', 'bar')
```

Replaces all matches of a regular expression in an input string with a new
string.

If either the input string, the regular expression, or the new string is `NULL`,
the result is `NULL`.

### `REGEXP_SPLIT_TO_ARRAY`

Since: 0.10.0

```sql
REGEXP_SPLIT_TO_ARRAY(col1, 'a.b+')
```

Splits a string into an array of substrings based on a regular expression.

If there is no match, the original string is returned as the only element
in the array.

If the regular expression is empty, all characters in the string are split.

If either the string or the regular expression is `NULL`, a `NULL` value is
returned.

If the regular expression is found at the beginning or end of the string, or
there are contiguous matches, an empty element is added to the array.

### `RPAD`

Since: 0.10.0

```sql
RPAD(input, length, padding)
```

Pads the input string or bytes, starting from the end, with the specified
padding of the same type, until the target length is reached.

If the input is longer than the specified target length, it is truncated.

If the padding string or byte array is empty or `NULL`, or the target length
is negative, `NULL` is returned.

Examples:

```sql
RPAD('Foo', 7, 'Bar')  =>  'FooBarB'
RPAD('Foo', 2, 'Bar')  =>  'Fo'
RPAD('', 2, 'Bar')  =>  'Ba'
```

### `SPLIT`

Since: 0.6.0

```sql
SPLIT(col1, delimiter)
```

Splits a string into an array of substrings, or bytes into an array of
subarrays, based on a delimiter.

If the delimiter isn't found, the original string or byte array is returned
as the only element in the array.

If the delimiter is empty, every character in the string or byte in the array
is split.

If the delimiter is found at the beginning or end of the string or bytes,
or there are contiguous delimiters, an empty space is added to the array.

Returns `NULL` if either parameter is `NULL`.

!!! Tip "See SPLIT in action"
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#ksqldb-code)

### `SPLIT_TO_MAP`

Since: 0.10.0

```sql
SPLIT_TO_MAP(input, entryDelimiter, kvDelimiter)
```

Splits a string into key-value pairs and creates a map from them.

The `entryDelimiter` splits the string into key-value pairs which are then
split by `kvDelimiter`.

If the same key is present multiple times in the input, the latest value for
the key is returned. 

Returns `NULL` if the input text is `NULL`.

Returns `NULL` if either of the delimiters is `NULL` or an empty string.

Example:

```sql
SPLIT_TO_MAP('apple':='green'/'cherry':='red', '/', ':=')  => { 'apple':'green', 'cherry':'red'}
```

### `SUBSTRING`

Since: 0.1.0

```sql
SUBSTRING(str, pos, [len])
SUBSTRING(bytes, pos, [len])
```

Returns the portion of `str` or `bytes` that starts at `pos` and
has length `len`, or continues to the end of the string or bytes.

The first character or byte is at position 1.

Example:

```sql
SUBSTRING("stream", 1, 4)  => "stre"
```

### `TO_BYTES`

Since: 0.21.0

```sql
TO_BYTES(string, encoding)
```

Converts a `STRING` column in the specified encoding type to a `BYTES` column.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

### `TRIM`

Since: 0.1.0

```sql
TRIM(col1)
```

Removes the spaces from the beginning and end of a string.

### `UCASE`

Since: 0.1.0

```sql
UCASE(col1)
```

Converts a string to uppercase.

!!! Tip "See UCASE in action"
    - [Handle corrupted data from Salesforce](https://developer.confluent.io/tutorials/salesforce/confluent.html#ksqldb-code)

### `UUID`

Since: 0.10.0

```sql
UUID()
```
Creates a Universally Unique Identifier (UUID) generated according to RFC 4122.

A call to UUID() returns a value conforming to UUID version 4, sometimes called
"random UUID", as described in RFC 4122.

The value is a 128-bit number represented as a string of five hexadecimal numbers,
_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_, for example, `237e9877-e79b-12d4-a765-321741963000`.

## Bytes

### `BIGINT_FROM_BYTES`

Since: 0.23.1

```sql
BIGINT_FROM_BYTES(col1, [byteOrder])
```

Converts a `BYTES` value to a `BIGINT` value according to the specified byte order.

`BYTES` must be 8 bytes long, or a `NULL` value is returned.

Byte order values must be `BIG_ENDIAN` or `LITTLE_ENDIAN`. If omitted,
`BIG_ENDIAN` is used.

A `NULL` value is returned if an invalid byte order value is provided.

Example, where `b` is a `BYTES` value represented as a base64 string
`AAAAASoF8gA=`:

```sql
BIGINT_FROM_BYTES(b, 'BIG_ENDIAN') => 5000000000
```

### `DOUBLE_FROM_BYTES`

Since: 0.23.1

```sql
DOUBLE_FROM_BYTES(col1, [byteOrder])
```

Converts a `BYTES` value to a `DOUBLE` value according to the specified byte
order.

`BYTES` must be 8 bytes long, or a `NULL` value is returned.

Byte order values must be `BIG_ENDIAN` or `LITTLE_ENDIAN`. If omitted,
`BIG_ENDIAN` is used.

A `NULL` value is returned if an invalid byte order value is provided.

Example, where `b` is a `BYTES` value represented as a base64 string `QICm/ZvJ9YI=`:

```sql
DOUBLE_FROM_BYTES(b, 'BIG_ENDIAN') => 532.8738323
```

### `INT_FROM_BYTES`

Since: 0.23.1

```sql
INT_FROM_BYTES(col1, [byteOrder])
```

Converts a `BYTES` value to an `INT` value according to the specified byte
order.

`BYTES` must be 4 bytes long, or a `NULL` value is returned.

Byte order values must be `BIG_ENDIAN` or `LITTLE_ENDIAN`. If omitted,
`BIG_ENDIAN` is used.

A `NULL` value is returned if an invalid byte order value is provided.

Example, where `b_big` is a `BYTES` value represented as a base64 string `AAAH5Q==`:

```sql
INT_FROM_BYTES(b, 'BIG_ENDIAN') -> 2021
```

### `TO_BYTES`

Since: 0.21.0

```sql
TO_BYTES(col1, encoding)
```

Converts a `STRING` value in the specified encoding to `BYTES`.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

## Nulls

### `COALESCE`

Since: 0.9.0

```sql
COALESCE(a, b, c, d)
```

Returns the first parameter that is not `NULL`. All parameters must be
of the same type.

If the parameter is a complex type, for example, `ARRAY` or `STRUCT`, the
contents of the complex type are not inspected. The behaviour is the same:
the first `NOT NULL` element is returned.

### `IFNULL`

Since: 0.9.0

```sql
IFNULL(expression, altValue)
```

If `expression` is `NULL`, returns `altValue`; otherwise, returns `expression`.

If `expression` evaluates to a complex type, for example, `ARRAY` or `STRUCT`,
the contents of the complex type are not inspected.

### `NULLIF`

Since: 0.19.0

```sql
NULLIF(expression1, expression2)
```

Returns `NULL` if `expression1` is equal to `expression2`; otherwise, returns `expression1`.

If `expression` evaluates to a complex type, for example, `ARRAY` or `STRUCT`,
the contents of the complex type are not inspected.

## Date and time

### `CONVERT_TZ`

```sql
CONVERT_TZ(col1, 'from_timezone', 'to_timezone')
```

Converts a `TIMESTAMP` value from `from_timezone` to `to_timezone`.

The `from_timezone` and `to_timezone` parameters are `java.util.TimeZone` ID
formats, for example: 

- "UTC"
- "America/Los_Angeles"
- "PDT"
- "Europe/London"

For more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

### `DATEADD`

Since: 0.20

```sql
DATEADD(unit, interval, col0)
```

Adds an interval to a date.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `DATESUB`

Since: 0.20

```sql
DATESUB(unit, interval, col0)
```

Subtracts an interval from a date.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `FORMAT_DATE`

```sql
FORMAT_DATE(date, 'yyyy-MM-dd')
```

Converts a `DATE` value into a string that represents the date in the
specified format.

You can escape single-quote characters in the timestamp format by using two
successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

### `FORMAT_TIME`

Since: 0.20

```sql
FORMAT_TIME(time, 'HH:mm:ss.SSS')
```

Converts a `TIME` value into the string representation of the time in the given
format.

You can escape single-quote characters in the time format by using two
successive single quotes, `''`, for example: `'''T''HH:mm:ssX'`.

For more information on time formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

### `FORMAT_TIMESTAMP`

```sql
FORMAT_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])
```

Converts a `TIMESTAMP` value into the string representation of the timestamp in
the specified format.

You can escape single-quote characters in the timestamp format by using two
successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`.

The optional `TIMEZONE` parameter is a `java.util.TimeZone` ID format,
for example: 

- "UTC"
- "America/Los_Angeles"
- "PDT"
- "Europe/London"

For more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

!!! Tip "See FORMAT_TIMESTAMP in action"
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#ksqldb-code)
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#ksqldb-code)

### `FROM_DAYS`

```sql
FROM_DAYS(days)
```

Converts an `INT` number of days since epoch to a `DATE` value.

### `FROM_UNIXTIME`

```sql
FROM_UNIXTIME(milliseconds)
```

Converts a `BIGINT` millisecond timestamp value into a `TIMESTAMP` value.

!!! Tip "See FROM_UNIXTIME in action"
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#ksqldb-code)

### `PARSE_DATE`

```sql
PARSE_DATE(col1, 'yyyy-MM-dd')
```

Converts a string representation of a date in the specified format into a `DATE`
value.

You can escape single-quote characters in the timestamp format by using two
successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

### `PARSE_TIME`

Since: 0.20

```sql
PARSE_TIME(col1, 'HH:mm:ss.SSS')
```

Converts a string value in the specified format into a `TIME` value.

You can escape single-quote characters in the time format by using successive
single quotes, `''`, for example: `'''T''HH:mm:ssX'`.

For more information on time formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).

### `PARSE_TIMESTAMP`

```sql
PARSE_TIMESTAMP(col1, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])
```

Converts a string value in the given format into the `TIMESTAMP` value.

You can escape single-quote characters in the timestamp format by using
successive single quotes, `''`, for example: `'yyyy-MM-dd''T''HH:mm:ssX'`.

The optional `TIMEZONE` parameter is a `java.util.TimeZone` ID format,
for example: 

- "UTC"
- "America/Los_Angeles"
- "PDT"
- "Europe/London"

### `TIMEADD`

Since: 0.20

```sql
TIMEADD(unit, interval, COL0)
```

Adds an interval to a `TIME`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `TIMESUB`

Since: 0.20

```sql
TIMESUB(unit, interval, COL0)
```

Subtracts an interval from a `TIME`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `TIMESTAMPADD`

Since: 0.17

```sql
TIMESTAMPADD(unit, interval, COL0)
```

Adds an interval to a `TIMESTAMP`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `TIMESTAMPSUB`

Since: 0.17

```sql
TIMESTAMPSUB(unit, interval, COL0)
```

Subtracts an interval from a `TIMESTAMP`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

### `UNIX_DATE`

Since: 0.6.0

```sql
UNIX_DATE([date])
```

If `UNIX_DATE` is called with the date parameter, the function returns the
`DATE` value as an `INTEGER` value representing the number of days since
`1970-01-01`.

If the `date` parameter is not provided, the function returns an integer
representing days since `1970-01-01`.

!!! important
    The returned integer may differ depending on the local time of different
    ksqlDB Server instances.

### `UNIX_TIMESTAMP`

Since: 0.6.0

```sql
UNIX_TIMESTAMP([timestamp])
```

If `UNIX_TIMESTAMP` is called with the timestamp parameter, the function
returns the `TIMESTAMP` value as a `BIGINT` value representing the number
of milliseconds since `1970-01-01T00:00:00 UTC`.

If the `timestamp` parameter is not provided, the function returns the current
UNIX timestamp in milliseconds, represented as a `BIGINT`.

!!! important
    The returned `BIGINT` may differ depending on the local time of different
    ksqlDB Server instances.

## URLs

!!! note
    All ksqlDB URL functions assume URI syntax defined in
    [RFC 39386](https://tools.ietf.org/html/rfc3986). For more information on the
    structure of a URI, including definitions of the various components, see
    Section 3 of the RFC.
    
    For encoding and decoding, the `application/x-www-form-urlencoded`
    convention is followed.

### `URL_DECODE_PARAM`

```sql
URL_DECODE_PARAM(col1)
```

Since: 0.6.0

Unescapes the `URL-param-encoded`_ value in `col1`.

This is the inverse of the `URL_ENCODE_PARAM` function.

Example:

```sql
URL_DECODE_PARAM("url%20encoded") => "url encoded"
```

### `URL_ENCODE_PARAM`

```sql
URL_ENCODE_PARAM(col1)
```

Since: 0.6.0

Escapes the value of `col1` such that it can safely be used in URL query
parameters.

!!! note
    `URL_ENCODE_PARAM` is not the same as encoding a value for use in the path
    portion of a URL.

Example:

```sql
URL_ENCODE_PARAM("url encoded") => "url%20encoded"
```  

### `URL_EXTRACT_FRAGMENT`

```sql
URL_EXTRACT_FRAGMENT(url)
```

Since: 0.6.0

Extracts the fragment portion of the specified value.

Returns `NULL` if `url` is not a valid URL or if the fragment doesn't exist.

All encoded values are decoded.                         

Examples:

```sql
URL_EXTRACT_FRAGMENT("http://test.com#frag") => "frag"
URL_EXTRACT_FRAGMENT("http://test.com#frag%20space") => "frag space"
```

### `URL_EXTRACT_HOST`

```sql
URL_EXTRACT_HOST(url)
```

Since: 0.6.0

Extracts the host-name portion of the specified value.

Returns `NULL` if `url` is not a valid URI according to RFC-2396.                       

Example:

```sql
URL_EXTRACT_HOST("http://test.com:8080/path") => "test.com"
```

### `URL_EXTRACT_PARAMETER`

Since: 0.6.0

```sql
URL_EXTRACT_PARAMETER(url, parameter_name)
```

Extracts the value of the requested parameter from the query-string of `url`.

Returns `NULL` if the parameter is not present, has no value specified for it
in the query string, or `url` is not a valid URI.

The function encodes the parameter and decodes the output.

To get all parameter values from a URL as a single string, use
`URL_EXTRACT_QUERY.`

Examples:

```sql
URL_EXTRACT_PARAMETER("http://test.com?a%20b=c%20d", "a b") => "c d"
URL_EXTRACT_PARAMETER("http://test.com?a=foo&b=bar", "b") => "bar"
```

### `URL_EXTRACT_PATH`

```sql
URL_EXTRACT_PATH(url)
```

Since: 0.6.0

Extracts the path from `url`.

Returns `NULL` if `url` is not a valid URI but returns an empty string if
the path is empty.

Example:

```sql
URL_EXTRACT_PATH("http://test.com/path/to#a") => "path/to"
```

### `URL_EXTRACT_PORT`

```sql
URL_EXTRACT_PORT(url)
```

Since: 0.6.0

Extracts the port number from `url`.

Returns `NULL` if `url` is not a valid URI or does not contain
an explicit port number.            

Example:

```sql
URL_EXTRACT_PORT("http://localhost:8080/path") => "8080"
```

### `URL_EXTRACT_PROTOCOL`

```sql
URL_EXTRACT_PROTOCOL(url)
```

Since: 0.6.0

Extracts the protocol from `url`.

Returns `NULL` if `url` is an invalid URI or has no protocol.

Example:

```sql
URL_EXTRACT_PROTOCOL("http://test.com?a=foo&b=bar") => "http"
```

### `URL_EXTRACT_QUERY`

Since: 0.6.0

```sql
URL_EXTRACT_QUERY(url)
```

Extracts the decoded query-string portion of `url`.

Returns `NULL` if no query-string is present or `url` is not a valid URI.       

Example:

```sql
URL_EXTRACT_QUERY("http://test.com?a=foo%20bar&b=baz") => "a=foo bar&b=baz"
```

## Deprecated

### `DATETOSTRING`

Since: 0.7.1

**Deprecated since 0.20.0 (use FORMAT_DATE)**

```sql
DATETOSTRING(START_DATE, 'yyyy-MM-dd')
```

Converts an integer representation of a date into a string representing the
date in the given format. Single quotes in the timestamp format can be escaped
with two successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.
The integer represents days since epoch matching the encoding used by
{{ site.kconnect }} dates.

### `STRINGTODATE`

Since: 0.7.1

**Deprecated since 0.20.0 (use PARSE_DATE)**

```sql
STRINGTODATE(col1, 'yyyy-MM-dd')
```

Converts a string representation of a date in the
given format into an integer representing days
since epoch. Single quotes in the timestamp
format can be escaped with two successive single
quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

### `STRINGTOTIMESTAMP`

Since: 0.7.1

**Deprecated since 0.16.0 (use PARSE_TIMESTAMP)**

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

Since: 0.7.1

**Deprecated since 0.16.0 (use FORMAT_TIMESTAMP)**

```sql
TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS' [, TIMEZONE])
```

Converts a BIGINT millisecond timestamp value into the string representation
of the timestamp in the given format. Single quotes in the timestamp format
can be escaped with two successive single quotes, `''`, for example:
`'yyyy-MM-dd''T''HH:mm:ssX'`.

TIMEZONE is an optional parameter, and it is a `java.util.TimeZone` ID format,
for example, "UTC", "America/Los_Angeles", "PDT", or "Europe/London". For more
information on timestamp formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).
