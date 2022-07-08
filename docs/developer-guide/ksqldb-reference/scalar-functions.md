---
layout: page
title: ksqlDB Scalar Functions
tagline:  ksqlDB scalar functions for queries
description: Scalar functions to use in SQL statements and queries
keywords: ksqlDB, SQL, function, scalar
---

## **Numeric functions**

### **`ABS`**

```sql title="Since: 0.1.0"
ABS(col1)
```

Returns the absolute value of `col1`.

---

### **`AS_VALUE`**

```sql title="Since: 0.9.0"
AS_VALUE(keyColumn)
```

Copies a row's key column into the row's value.

```sql title="Example", hl_lines="4"
CREATE TABLE AGG AS
   SELECT 
     ID,                  -- this is the grouping column, which is stored in the message key.
     AS_VALUE(ID) AS ID2  -- this creates a copy of ID, named ID2, which is stored in the message value.
     COUNT(*) AS COUNT
   FROM S
   GROUP BY ID;
```

!!! Tip "See AS_VALUE in action"
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#execute-ksqldb-code)

---

### **`CAST`**

```sql title="Since: 0.1.0"
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
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#execute-ksqldb-code)
    - [Understand user behavior with clickstream data](https://developer.confluent.io/tutorials/clickstream/confluent.html#execute-ksqldb-code)

---

### **`CEIL`**

```sql title="Since: 0.1.0"
CEIL(col1)
```

Returns the the smallest integer value that's greater than or equal to `col1`.

---

### **`ENTRIES`**

```sql title="Since: 0.6.0"
ENTRIES(map MAP, sorted BOOLEAN)
```

Creates an array of structs from the entries in a map. Each struct has a field
named `K` containing the key, which is a string, and a field named `V`, which
holds the value.

If `sorted` is true, the entries are sorted by key.

---

### **`EXP`**

```sql title="Since: 0.6.0"
EXP(col1)
```

Returns the exponential of `col1`, which is _e_ raised to the power of `col1`. 

---

### **`FLOOR`**

```sql title="Since: 0.1.0"
FLOOR(col1)
```

Returns the largest integer value that's less than or equal to `col1`.

---

### **`GENERATE_SERIES`**

```sql title="Since: 0.6.0"
GENERATE_SERIES(start, end)
GENERATE_SERIES(start, end, step)
```

Constructs an array of values between `start` and `end`, inclusive.

Parameters `start` and `end` can be an `INT` or `BIGINT`.

`step`, if supplied, specifies the step size. The step can be positive or negative.
If not supplied, `step` defaults to `1`. Parameter `step` must be an `INT`.

---

### **`GEO_DISTANCE`**

```sql title="Since: 0.6.0"
GEO_DISTANCE(lat1, lon1, lat2, lon2, unit)
```

The great-circle distance between two lat-long points, both specified
in decimal degrees. An optional final parameter specifies `KM`
(the default) or `miles`.

---

### **`GREATEST`**

```sql title="Since: 0.20.0"
GREATEST(col1, col2, …)
```

Returns the largest non-null value from a variable number of comparable columns.

If comparing columns of different numerical types, use [CAST](#cast) to first
cast them to be of the same type.

---

### **`LEAST`**

```sql title="Since: 0.20.0"
LEAST(col1, col2, …)
```

Returns the smallest non-null value from a variable number of comparable columns.

If comparing columns of different numerical types, use [CAST](#cast) to first
cast them to be of the same type.

---

### **`LN`**

```sql title="Since: 0.6.0"
LN(col1)
```

Returns the natural logarithm of `col1`, which is .

The value of `col1` must be greater than 0.

---

### **`RANDOM`**

```sql title="Since: 0.1.0"
RANDOM()
```

Returns a random `DOUBLE` value between 0.0 and 1.0.

---

### **`ROUND`**

```sql title="Since: 0.1.0"
ROUND(col1)
ROUND(col1, scale)
```

Rounds a value to the number of decimal places specified by `scale`.

If `scale` is negative, the value is rounded to the right of the decimal point.

Numbers equidistant to the nearest value are rounded up, in the positive
direction.

If the number of decimal places is not provided, it defaults to zero.

---

### **`SIGN`**

```sql title="Since: 0.6.0"
SIGN(col1)
```

Returns the sign of `col1` as an `INTEGER`:

* -1 if the argument is negative
* 0 if the argument is zero
* 1 if the argument is positive
* `null` argument is `null`

---

### **`SQRT`**

```sql title="Since: 0.6.0"
SQRT(col1)
```

Returns the square root of `col`.

---

## **Collections**

### **`ARRAY`**

```sql title="Since: 0.7.0"
ARRAY[exp1, exp2, …]
```

Constructs an array from a variable number of inputs.

All elements must be coercible to a common SQL type.
For more information, see
[Implicit type coercion](type-coercion.md#implicit-type-coercion).

---

### **`ARRAY_CONCAT`**

```sql title="Since: 0.21.0"
ARRAY_CONCAT(array1, array2)
```

Returns an array representing the concatenation of both input arrays.

Returns `NULL` if both input arrays are `NULL`. If only one argument is `NULL`,
the result is the other argument.

```sql title="Examples" 
ARRAY_CONCAT(ARRAY[1, 2, 3, 1, 2], [4, 1])  => [1, 2, 3, 1, 2, 4, 1]
ARRAY_CONCAT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', 'apple', NULL, 'cherry', 'cherry']
```

---

### **`ARRAY_CONTAINS`**

```sql title="Since: 0.6.0"
ARRAY_CONTAINS(ARRAY[1, 2, 3], 3)
```

Given an array, checks if a search value is contained in the array.

Accepts any `ARRAY` type. The type of the second param must match the element
type of the `ARRAY`.

!!! Tip "See ARRAY_CONTAINS in action"
    - [Build Customer Loyalty Programs](https://developer.confluent.io/tutorials/loyalty-rewards/confluent.html#execute-ksqldb-code)

---

### **`ARRAY_DISTINCT`**

```sql title="Since: 0.10.0"
ARRAY_DISTINCT([1, 2, 3])
```

Returns an array of all the distinct values, including `NULL` if present,
from the input array.

The output array elements are in order of their first occurrence in the input.

Returns `NULL` if the input array is `NULL`.

```sql title="Examples" 
ARRAY_DISTINCT(ARRAY[1, 1, 2, 3, 1, 2])  => [1, 2, 3]
ARRAY_DISTINCT(ARRAY['apple', 'apple', NULL, 'cherry'])  => ['apple', NULL, 'cherry']
```

---

### **`ARRAY_EXCEPT`**

```sql title="Since: 0.10.0"
ARRAY_EXCEPT(array1, array2)
```

Returns an array of all the distinct elements from an array, except for those
also present in a second array.

The order of entries in the first array is preserved but duplicates are removed. 

Returns `NULL` if either input is `NULL`.

```sql title="Examples" 
ARRAY_EXCEPT(ARRAY[1, 2, 3, 1, 2], [2, 3])  => [1]
ARRAY_EXCEPT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', NULL]
```

---

### **`ARRAY_INTERSECT`**

```sql title="Since: 0.10.0"
ARRAY_INTERSECT(array1, array2)
```

Returns an array of all the distinct elements from the intersection of both
input arrays.

The order of entries in the output is the same as in the first input array.

Returns `NULL` if either input array is `NULL`.

```sql title="Examples" 
ARRAY_INTERSECT(ARRAY[1, 2, 3, 1, 2], [2, 1])  => [1, 2]
ARRAY_INTERSECT(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['apple'])  => ['apple']
```

---

### **`ARRAY_JOIN`**

```sql title="Since: 0.10.0"
ARRAY_JOIN(col1, delimiter)
```

Creates a flat string representation of all the elements contained in an
array.

The elements in the resulting string are separated by the chosen `delimiter`, 
which is an optional parameter. The default is the comma character, `,`.

Array elements are limited to primitive ksqlDB types only.

!!! Tip "See ARRAY_JOIN in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#execute-ksqldb-code)

---

### **`ARRAY_LENGTH`**

```sql title="Since: 0.8.0"
ARRAY_LENGTH(ARRAY[1, 2, 3])
```

Returns the number of elements in an array.

If the supplied parameter is `NULL`, the method returns `NULL`.

---

### **`ARRAY_MAX`**

```sql title="Since: 0.10.0"
ARRAY_MAX(['foo', 'bar', 'baz'])
```

Returns the maximum value from an array of primitive elements.

Arrays of other arrays, arrays of maps, arrays of structs, or combinations
of these types aren't supported.

If the array field is `NULL`, or contains only `NULL` values, `NULL` is
returned.

Array entries are compared according to their natural sort order, which sorts
the various data types as shown in the following examples.


```sql title="Examples"
ARRAY_MAX[-1, 2, NULL, 0] => 2
ARRAY_MAX[false, NULL, true] => true
ARRAY_MAX['Foo', 'Bar', NULL, 'baz'] => 'baz' -- (lower-case characters are "greater" than upper-case characters)
```

---

### **`ARRAY_MIN`**

```sql title="Since: 0.10.0"
ARRAY_MIN(['foo', 'bar', 'baz'])
```

Returns the minimum value from an array of primitive elements.

Arrays of other arrays, arrays of maps, arrays of structs, or combinations
of these types aren't supported. 

If the array field is `NULL`, or contains only `NULL` values, `NULL` is
returned.

Array entries are compared according to their natural sort order, which sorts
the various data types as shown in the following examples.

```sql title="Examples"
ARRAY_MIN[-1, 2, NULL, 0] => -1
ARRAY_MIN[false, NULL, true] => false
ARRAY_MIN['Foo', 'Bar', NULL, 'baz'] => 'Bar'
```

---

### **`ARRAY_REMOVE`**

```sql title="Since: 0.11.0"
ARRAY_REMOVE(array, element)
```

Removes all elements from `array` that are equal to `element`.

If the `array` field is `NULL`, `NULL` is returned.

```sql title="Examples"
ARRAY_REMOVE([1, 2, 3, 2, 1], 2) => [1, 3, 1]
ARRAY_REMOVE([false, NULL, true, true], false) => [NULL, true, true]
ARRAY_REMOVE(['Foo', 'Bar', NULL, 'baz'], null) => ['Foo', 'Bar', 'baz']
```

---

### **`ARRAY_SORT`**

```sql title="Since: 0.10.0"
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

```sql title="Examples"
ARRAY_SORT[-1, 2, NULL, 0] -> [-1, 0, 2, NULL]
ARRAY_SORT[false, NULL, true] -> [false, true, NULL]
ARRAY_SORT['Foo', 'Bar', NULL, 'baz'] -> ['Bar', 'Foo', 'baz', NULL]
```

!!! Tip "See ARRAY_SORT in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#execute-ksqldb-code)

---

### **`ARRAY_UNION`**

```sql title="Since: 0.10.0"
ARRAY_UNION(array1, array2)
```

Returns an array of all the distinct elements from both input arrays, in the
order they're encountered.

Returns `NULL` if either input array is `NULL`.

```sql title="Examples" 
ARRAY_UNION(ARRAY[1, 2, 3, 1, 2], [4, 1])  => [1, 2, 3, 4]
ARRAY_UNION(ARRAY['apple', 'apple', NULL, 'cherry'], ARRAY['cherry'])  => ['apple', NULL, 'cherry']
```

---

### **`AS_MAP`**

```sql title="Since: 0.6.0"
AS_MAP(keys, vals)
```

Constructs a map from a list of keys and a list of values.

!!! Tip "See AS_MAP in action"
    - [Match users for online dating](https://developer.confluent.io/tutorials/online-dating/confluent.html#execute-ksqldb-code)

---

### **`ELT`**

```sql title="Since: 0.6.0"
ELT(n INTEGER, args VARCHAR[])
```

Returns element `n` in the `args` list of strings, or `NULL` if `n` is less than
1 or greater than the number of arguments.

The `ELT` function is 1-indexed.

`ELT` is the complement to the `FIELD` function.

---

### **`FIELD`**

```sql title="Since: 0.6.0"
FIELD(str VARCHAR, args VARCHAR[])
```

Returns the 1-indexed position of `str` in `args`, or 0 if not found.

If `str` is `NULL`, the return value is 0, because `NULL` isn't considered
to be equal to any value.

`FIELD` is the complement to the `ELT` function.

---

### **`JSON_ARRAY_CONTAINS`**

```sql title="Since: 0.6.0"
JSON_ARRAY_CONTAINS('[1, 2, 3]', 3)
```

Given a `STRING` containing a JSON array, checks if a search value is contained
in the array.

Returns `false` if the first parameter doesn't contain a JSON array.

---

### **`MAP`**

```sql title="Since: 0.7.0"
MAP(key VARCHAR := value, …)
```

Constructs a map from specific key-value tuples.

All values must be coercible to a common SQL type.

For more information, see
[Implicit type coercion](type-coercion.md#implicit-type-coercion).

---

### **`MAP_KEYS`**

```sql title="Since: 0.10.0"
MAP_KEYS(a_map)
```

Returns an array that contains all keys from the specified map.

Returns `NULL` if the input map is `NULL`.

```sql title="Example"
map_keys( map('apple' := 10, 'banana' := 20) )  => ['apple', 'banana'] 
```

---

### **`MAP_VALUES`**

```sql title="Since: 0.10.0"
MAP_VALUES(a_map)
```

Returns an array that contains all values from the specified map.

Returns `NULL` if the input map is `NULL`.

```sql title="Example"
map_values( map('apple' := 10, 'banana' := 20) )  => [10, 20] 
```

---

### **`MAP_UNION`**

```sql title="Since: 0.10.0"
MAP_UNION(map1, map2)
```

Returns a new map containing the union of all entries from both input maps.

If a key is present in both input maps, the corresponding value from _map2_
is returned.

Returns `NULL` if all input maps are `NULL`.

```sql title="Examples"
MAP_UNION( MAP('apple' := 10, 'banana' := 20), MAP('cherry' := 99) )  => ['apple': 10, 'banana': 20, 'cherry': 99] 
MAP_UNION( MAP('apple' := 10, 'banana' := 20), MAP('apple' := 50) )  => ['apple': 50, 'banana': 20] 
```

---

### **`SLICE`**

```sql title="Since: 0.6.0"
SLICE(col1, from, to)
```

Slices a list based on the supplied indices.

The indices start at 1 and include both endpoints.

---

## **Invocation Functions**

Apply lambda functions to collections.

### **`FILTER`**

```sql title="Since: 0.17.0"
FILTER(array, x => …)
FILTER(map, (k,v) => …)
```

Filters a collection with a lambda function.

If the collection is an array, the lambda function must have one input argument.

If the collection is a map, the lambda function must have two input arguments.

---

### **`REDUCE`**

```sql title="Since: 0.17.0"
REDUCE(array, state, (s, x) => …)
REDUCE(map, state, (s, k, v) => …)
```

Reduces a collection starting from an initial state.

If the collection is an array, the lambda function must have two input arguments.

If the collection is a map, the lambda function must have three input arguments.

If the state is `NULL`, the result is `NULL`.

---

### **`TRANSFORM`**

```sql title="Since: 0.17.0"
TRANSFORM(array, x => …)
TRANSFORM(map, (k,v) => …, (k,v) => …)
```

Transforms a collection by using a lambda function.

If the collection is an array, the lambda function must have one input argument.

If the collection is a map, two lambda functions must be provided, and both
lambdas must have two arguments: a map entry key and a map entry value.

---

## **Strings**

### **`CHR`**

```sql title="Since: 0.10.0"
CHR(decimal_code | utf_string)
```

Returns a single-character string representing the Unicode code-point described
by the input.

The input parameter can be either a decimal character code or a string
representation of a UTF code.

Returns `NULL` if the input is `NULL` or doesn't represent a valid code-point.

Commonly used to insert control characters such as `Tab` (9), `Line Feed` (10),
or `Carriage Return` (13) into strings.

```sql title="Examples"
CHR(75)        => 'K'
CHR('\u004b')  => 'K'
CHR(22909)     => '好'
CHR('\u597d')  => '好'
```

---

### **`CONCAT`**

```sql title="Since: 0.1.0"
CONCAT(col1, col2, 'hello', …, col-n)
CONCAT(bytes1, bytes2, …, bytes-n)
```

Concatenates two or more string or bytes expressions.

Any inputs which evaluate to `NULL` are replaced with an empty string or bytes
in the output.

!!! Tip "See CONCAT in action"
    - [Enrich orders with change data capture (CDC)](https://developer.confluent.io/tutorials/denormalization/confluent.html#execute-ksqldb-code)

---

### **`CONCAT_WS`**

```sql title="Since: 0.10.0"
CONCAT_WS(separator, expr1, expr2, …)
```

Concatenates two or more string or bytes expressions, inserting a separator
string or bytes between each.

If the separator is `NULL`, this function returns `NULL`.

Any expressions which evaluate to `NULL` are skipped.

```sql title="Example"
CONCAT_WS(', ', 'apple', 'banana', NULL, 'date')  ->  'apple, banana, date'
```

---

### **`ENCODE`**

```sql title="Since: 0.10.0"
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

```sql title="Example"
ENCODE(string, 'hex', 'utf8')
```

---

### **`EXTRACTJSONFIELD`**

```sql title="Since: 0.11.0"
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

---

### **`FROM_BYTES`**

```sql title="Since: 0.21.0"
FROM_BYTES(bytes, encoding)
```

Converts a `BYTES` column to a `STRING` in the specified encoding type.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

### `IS_JSON_STRING`

```sql title="Since: 0.24.0"
IS_JSON_STRING(json_string) => Boolean
```

Returns `true` if `json_string` can be parsed as a valid JSON value; otherwise,
`false` .

```sql title="Examples"
IS_JSON_STRING('[1, 2, 3]') => true
IS_JSON_STRING('{}') => true
IS_JSON_STRING('1') => true
IS_JSON_STRING('\"abc\"') => true
IS_JSON_STRING('null') => true
IS_JSON_STRING('') => false
IS_JSON_STRING('abc') => false
IS_JSON_STRING(NULL) => false
```

### `JSON_ARRAY_LENGTH`

```sql title="Since: 0.24.0"
JSON_ARRAY_LENGTH(json_string) => Integer
```

Parses `json_string` as a JSON value and returns the length of the top-level
array.

Returns `NULL` if the string can't be interpreted as a JSON array, for example,
when the string is `NULL` or it doesn't contain valid JSON, or the JSON value
is not an array.

```sql title="Examples"
JSON_ARRAY_LENGTH('[1, 2, 3]') => 3
JSON_ARRAY_LENGTH('[1, [1, [2]], 3]') =>  3
JSON_ARRAY_LENGTH('[]') => 0
JSON_ARRAY_LENGTH('{}') => NULL
JSON_ARRAY_LENGTH('123') => NULL
JSON_ARRAY_LENGTH(NULL) => NULL
JSON_ARRAY_LENGTH('abc') => returns NULL and logs an "Invalid JSON format" exception in server log
```

### `JSON_CONCAT`

```sql title="Since: 0.24.0"
JSON_CONCAT(json_string1, json_string2, ...) => String
```

Given N strings, parses them as JSON values and returns a string representing
their concatenation.

Concatenation rules are identical to PostgreSQL's
[|| operator](https://www.postgresql.org/docs/14/functions-json.html):

* If all strings deserialize into JSON objects, return an object with a union
  of the input keys. If there are duplicate objects, take values from the last
  object.
* If all strings deserialize into JSON arrays, return the result of array
  concatenation.
* If at least one of the deserialized values is not an object, convert
  non-array inputs to a single-element array and return the result of
  array concatenation.
* If at least one of the input strings is `NULL` or can't be deserialized as
  JSON, return `NULL`.

Similar to PostgreSQL's `||` operator, this function merges only top-level
object keys or arrays.

Examples:

```sql title="Examples"
JSON_CONCAT('{\"a\": 1}', '{\"b\": 2}') => '{"a":1,"b":2}'
JSON_CONCAT('{\"a\": {\"5\": 6}}', '{\"a\": {\"3\": 4}}') => '{"a":{"3":4}}'
JSON_CONCAT('{}', '{}') => '{}'
JSON_CONCAT('[1, 2]', '[3, 4]') => '[1,2,3,4]'
JSON_CONCAT('[1, [2]]', '[[[3]], [[[4]]]]') => '[ 1, [2], [[3]], [[[4]]] ]'
JSON_CONCAT('null', 'null') => '[null, null]'
JSON_CONCAT('[1, 2]', '{\"a\": 1}') => '[1,2,{"a":1}]'
JSON_CONCAT('[1, 2]', '3') => '[1, 2, 3]'
JSON_CONCAT('1', '2') => '[1, 2]'
JSON_CONCAT('[]', '[]') => '[]'
JSON_CONCAT('abc', '[1]') => NULL
JSON_CONCAT(NULL, '[1]') => NULL
```

### `JSON_KEYS`

```sql title="Since: 0.24.0"
JSON_KEYS(json_string) => Array<String>
```

Parses `json_string` as a JSON object and returns an array of strings
representing the top-level keys.

Returns `NULL` if the string can't be interpreted as a JSON object,
for example, when the string is `NULL` or it does not contain valid JSON,
or the JSON value is not an object.

```sql title="Examples"
JSON_KEYS('{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}') => ['a', 'b', 'd']
JSON_KEYS('{}') => []
JSON_KEYS('[]') => NULL
JSON_KEYS('123') => NULL
JSON_KEYS(NULL) => NULL
JSON_KEYS('') => NULL
```

### `JSON_RECORDS`

```sql title="Since: 0.24.0"
JSON_RECORDS(json_string) => Map<String, String>
```

Parses `json_string` as a JSON object and returns a map representing the
top-level keys and values.

Returns `NULL` if the string can't be interpreted as a JSON object,
for example, when the string is `NULL` or it does not contain valid JSON,
or the JSON value is not an object.

```sql title="Examples"
JSON_RECORDS('{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}') => {d=1, a="abc", b={"c":"a"}}
JSON_RECORDS('{}') => {}
JSON_RECORDS('[]') => NULL
JSON_RECORDS('123') => NULL
JSON_RECORDS(NULL) => NULL
JSON_RECORDS('abc') => NULL
```

### `TO_JSON_STRING`

```sql title="Since: 0.24.0"
TO_JSON_STRING(val) => String
```

Given any ksqlDB type, returns the equivalent JSON string.

```sql title="Primitives types"
TO_JSON_STRING(1) => '1'
TO_JSON_STRING(15.3) => '15.3'
TO_JSON_STRING('abc') => '"abc"'
TO_JSON_STRING(true) => 'true'
TO_JSON_STRING(PARSE_DATE('2021-10-11', 'yyyy-MM-dd')) => '"2021-10-11"'
TO_JSON_STRING(PARSE_TIME('13:25', 'HH:mm')) => '"13:25"'
TO_JSON_STRING(PARSE_TIMESTAMP('2021-06-31 12:18:39.446', 'yyyy-MM-dd HH:mm:ss.SSS')) => '"2021-06-30T12:18:39.446"'
TO_JSON_STRING(NULL) => 'null'
```

```sql title="Compound types"
TO_JSON_STRING(Array[1, 2, 3]) => '[1, 2, 3]'
TO_JSON_STRING(Struct(id := 1, name := 'A')) => '{"ID":1,"NAME":"A"}'
TO_JSON_STRING(Map('c' := 2, 'd' := 4)) => '{"c": 2, "d": 4}'
TO_JSON_STRING(Array[Struct(json_key := 1, json_value := Map('c' := 2, 'd' := 3))]) => '[{"JSON_KEY": 1, "JSON_VALUE": {"c": 2, "d": 3}}]'
```

---

### **`INITCAP`**

```sql title="Since: 0.6.0"
INITCAP(col1)
```

Capitalizes the first letter in each word and converts all other letters
to lowercase.

Words are delimited by whitespace.

---

### **`INSTR`**

```sql title="Since: 0.10.0"
INSTR(string, substring, [position], [occurrence])
```

Returns the position of `substring` in `string`.

The first character is at position 1.

If `position` is provided, search starts from the specified position.

A negative value for `position` causes the search to work from the end to the
start of `string`.

If `occurrence` is provided, the position of the *n*-th occurrence is returned.

If `substring` is not found, the return value is 0.

```sql title="Examples"
INSTR('CORPORATE FLOOR', 'OR') -> 2
INSTR('CORPORATE FLOOR', 'OR', 3) -> 5
INSTR('CORPORATE FLOOR', 'OR', 3, 2) -> 14
INSTR('CORPORATE FLOOR', 'OR', -3) -> 5
INSTR('CORPORATE FLOOR', 'OR', -3, 2) -> 2b
INSTR('CORPORATE FLOOR', 'MISSING') -> 0
```

---

### **`LCASE`**

```sql title="Since: 0.1.0"
LCASE(col1)
```

Converts a string to lowercase.

---

### **`LEN`**

```sql title="Since: 0.1.0"
LEN(string)
LEN(bytes)
```

Returns the length of a `STRING` or the number of bytes in a `BYTES` value.

---

### **`LPAD`**

```sql title="Since: 0.10.0"
LPAD(input, length, padding)
```

Pads the input string or bytes, beginning from the left, with the specified
padding of the same type, until the target length is reached.

If the input is longer than `length`, it is truncated.

If the padding string or byte array is empty or `NULL`, or the target length
is negative, `NULL` is returned.

```sql title="Examples"
LPAD('Foo', 7, 'Bar')  =>  'BarBFoo'
LPAD('Foo', 2, 'Bar')  =>  'Fo'
LPAD('', 2, 'Bar')  =>  'Ba'
LPAD('123', 5, '0')  => '00123'
```

---

### **`MASK`**

```sql title="Since: 0.6.0"
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

---

### **`MASK_KEEP_LEFT`**

```sql title="Since: 0.6.0"
MASK_KEEP_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that the first or left-most `numChars`
characters aren't masked in any way.

```sql title="Example"
MASK_KEEP_LEFT("My Test $123", 4) => "My Txxx--nnn"
```

---

### **`MASK_KEEP_RIGHT`**

```sql title="Since: 0.6.0"
MASK_KEEP_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that the last or right-most
`numChars` characters aren't masked in any way.

```sql title="Example"
MASK_KEEP_RIGHT("My Test $123", 4) => "Xx-Xxxx-$123"
```

---

### **`MASK_LEFT`**

```sql title="Since: 0.6.0"
MASK_LEFT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that only the first or left-most
`numChars` characters have any masking applied to them.

```sql title="Example"
MASK_LEFT("My Test $123", 4) => "Xx-Xest $123"
```

---

### **`MASK_RIGHT`**

```sql title="Since: 0.6.0"
MASK_RIGHT(col1, numChars, 'X', 'x', 'n', '-')
```

Similar to the `MASK` function, except that only the last or right-most
`numChars` characters have any masking applied to them.

```sql title="Example"
MASK_RIGHT("My Test $123", 4) => "My Test -nnn"
```

---

### **`REPLACE`**

```sql title="Since: 0.6.0"
REPLACE(col1, 'foo', 'bar')
```

Replaces all instances of a substring in a string with a new string.

!!! Tip "See REPLACE in action"
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#execute-ksqldb-code)

---

### **`REGEXP_EXTRACT`**

```sql title="Since: 0.8.0"
REGEXP_EXTRACT('.*', col1)
REGEXP_EXTRACT('(([AEIOU]).)', col1, 2)
```

Extracts the first substring matched by the regular expression pattern from the
input.

You can specify a capturing group number to return that specific group. If a
number isn't specified, the entire substring is returned by default.

```sql title="Example"
REGEXP_EXTRACT("(.*) (.*)", 'hello there', 2) => "there"
```

---

### **`REGEXP_EXTRACT_ALL`**

```sql title="Since: 0.10.0"
REGEXP_EXTRACT_ALL('.*', col1)
REGEXP_EXTRACT_ALL('(([AEIOU]).)', col1, 2)
```

Extracts all subtrings matched by the regular expression pattern from the input.

You can specify a capturing group number to return that specific group. If a
number isn't specified, the entire substring is returned by default.

```sql title="Example"
REGEXP_EXTRACT("(\\w+) (\\w+)", "hello there nice day", 2) => ["there", "day"]
```

---

### **`REGEXP_REPLACE`**

```sql title="Since: 0.10.0"
REGEXP_REPLACE(col1, 'a.b+', 'bar')
```

Replaces all matches of a regular expression in an input string with a new
string.

If either the input string, the regular expression, or the new string is `NULL`,
the result is `NULL`.

---

### **`REGEXP_SPLIT_TO_ARRAY`**

```sql title="Since: 0.10.0"
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

---

### **`RPAD`**

```sql title="Since: 0.10.0"
RPAD(input, length, padding)
```

Pads the input string or bytes, starting from the end, with the specified
padding of the same type, until the target length is reached.

If the input is longer than the specified target length, it is truncated.

If the padding string or byte array is empty or `NULL`, or the target length
is negative, `NULL` is returned.

```sql title="Examples"
RPAD('Foo', 7, 'Bar')  =>  'FooBarB'
RPAD('Foo', 2, 'Bar')  =>  'Fo'
RPAD('', 2, 'Bar')  =>  'Ba'
```

---

### **`SPLIT`**

```sql title="Since: 0.6.0"
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
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#execute-ksqldb-code)

---

### **`SPLIT_TO_MAP`**

```sql title="Since: 0.10.0"
SPLIT_TO_MAP(input, entryDelimiter, kvDelimiter)
```

Splits a string into key-value pairs and creates a map from them.

The `entryDelimiter` splits the string into key-value pairs which are then
split by `kvDelimiter`.

If the same key is present multiple times in the input, the latest value for
the key is returned. 

Returns `NULL` if the input text is `NULL`.

Returns `NULL` if either of the delimiters is `NULL` or an empty string.

```sql title="Example"
SPLIT_TO_MAP('apple':='green'/'cherry':='red', '/', ':=')  => { 'apple':'green', 'cherry':'red'}
```

---

### **`SUBSTRING`**

```sql title="Since: 0.1.0"
SUBSTRING(str, pos, [len])
SUBSTRING(bytes, pos, [len])
```

Returns the portion of `str` or `bytes` that starts at `pos` and
has length `len`, or continues to the end of the string or bytes.

The first character or byte is at position 1.

```sql title="Example"
SUBSTRING("stream", 1, 4)  => "stre"
```

---

### **`TO_BYTES`**

```sql title="Since: 0.21.0"
TO_BYTES(string, encoding)
```

Converts a `STRING` column in the specified encoding type to a `BYTES` column.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

---

### **`TRIM`**

```sql title="Since: 0.1.0"
TRIM(col1)
```

Removes the spaces from the beginning and end of a string.

---

### **`UCASE`**

```sql title="Since: 0.1.0"
UCASE(col1)
```

Converts a string to uppercase.

!!! Tip "See UCASE in action"
    - [Handle corrupted data from Salesforce](https://developer.confluent.io/tutorials/salesforce/confluent.html#execute-ksqldb-code)

---

### **`UUID`**

```sql title="Since: 0.10.0"
UUID()
```
Creates a Universally Unique Identifier (UUID) generated according to RFC 4122.

A call to UUID() returns a value conforming to UUID version 4, sometimes called
"random UUID", as described in RFC 4122.

The value is a 128-bit number represented as a string of five hexadecimal numbers,
_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_, for example, `237e9877-e79b-12d4-a765-321741963000`.

---

## **Bytes**

### **`BIGINT_FROM_BYTES`**

```sql title="Since: 0.23.1"
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

---

### **`DOUBLE_FROM_BYTES`**

```sql title="Since: 0.23.1"
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

---

### **`INT_FROM_BYTES`**

```sql title="Since: 0.23.1"
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

---

### **`TO_BYTES`**

```sql title="Since: 0.21.0"
TO_BYTES(col1, encoding)
```

Converts a `STRING` value in the specified encoding to `BYTES`.

The following list shows the supported encoding types.

- `hex`
- `utf8`
- `ascii`
- `base64`

---

## **Nulls**

### **`COALESCE`**

```sql title="Since: 0.9.0"
COALESCE(a, b, c, d)
```

Returns the first parameter that is not `NULL`. All parameters must be
of the same type.

If the parameter is a complex type, for example, `ARRAY` or `STRUCT`, the
contents of the complex type are not inspected. The behaviour is the same:
the first `NOT NULL` element is returned.

---

### **`IFNULL`**

```sql title="Since: 0.9.0"
IFNULL(expression, altValue)
```

If `expression` is `NULL`, returns `altValue`; otherwise, returns `expression`.

If `expression` evaluates to a complex type, for example, `ARRAY` or `STRUCT`,
the contents of the complex type are not inspected.

---

### **`NULLIF`**

```sql title="Since: 0.19.0"
NULLIF(expression1, expression2)
```

Returns `NULL` if `expression1` is equal to `expression2`; otherwise, returns `expression1`.

If `expression` evaluates to a complex type, for example, `ARRAY` or `STRUCT`,
the contents of the complex type are not inspected.

---

## **Date and time**

### **`CONVERT_TZ`**

```sql title="Since: 0.17.0"
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

---

### **`DATEADD`**

```sql title="Since: 0.20.0"
DATEADD(unit, interval, col0)
```

Adds an interval to a date.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`DATESUB`**

```sql title="Since: 0.20.0"
DATESUB(unit, interval, col0)
```

Subtracts an interval from a date.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`FORMAT_DATE`**

```sql title="Since: 0.20.0"
FORMAT_DATE(date, 'yyyy-MM-dd')
```

Converts a `DATE` value into a string that represents the date in the
specified format.

You can escape single-quote characters in the timestamp format by using two
successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

---

### **`FORMAT_TIME`**

```sql title="Since: 0.20.0"
FORMAT_TIME(time, 'HH:mm:ss.SSS')
```

Converts a `TIME` value into the string representation of the time in the given
format.

You can escape single-quote characters in the time format by using two
successive single quotes, `''`, for example: `'''T''HH:mm:ssX'`.

For more information on time formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).

---

### **`FORMAT_TIMESTAMP`**

```sql title="Since: 0.17.0"
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
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#execute-ksqldb-code)
    - [Detect and analyze SSH attacks](https://developer.confluent.io/tutorials/SSH-attack/confluent.html#execute-ksqldb-code)

---

### **`FROM_DAYS`**

```sql title="Since: 0.20.0"
FROM_DAYS(days)
```

Converts an `INT` number of days since epoch to a `DATE` value.

---

### **`FROM_UNIXTIME`**

```sql title="Since: 0.17.0"
FROM_UNIXTIME(milliseconds)
```

Converts a `BIGINT` millisecond timestamp value into a `TIMESTAMP` value.

!!! Tip "See FROM_UNIXTIME in action"
    - [Analyze datacenter power usage](https://developer.confluent.io/tutorials/datacenter/confluent.html#execute-ksqldb-code)

---

### **`PARSE_DATE`**

```sql title="Since: 0.20.0"
PARSE_DATE(col1, 'yyyy-MM-dd')
```

Converts a string representation of a date in the specified format into a `DATE`
value.

You can escape single-quote characters in the timestamp format by using two
successive single quotes, `''`, for example: `'yyyy-MM-dd''T'''`.

---

### **`PARSE_TIME`**

```sql title="Since: 0.20.0"
PARSE_TIME(col1, 'HH:mm:ss.SSS')
```

Converts a string value in the specified format into a `TIME` value.

You can escape single-quote characters in the time format by using successive
single quotes, `''`, for example: `'''T''HH:mm:ssX'`.

For more information on time formats, see [DateTimeFormatter](https://cnfl.io/java-dtf).

---

### **`PARSE_TIMESTAMP`**

```sql title="Since: 0.17.0"
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

---

### **`TIMEADD`**

```sql title="Since: 0.20.0"
TIMEADD(unit, interval, COL0)
```

Adds an interval to a `TIME`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`TIMESUB`**

```sql title="Since: 0.20.0"
TIMESUB(unit, interval, COL0)
```

Subtracts an interval from a `TIME`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`TIMESTAMPADD`**

```sql title="Since: 0.17.0"
TIMESTAMPADD(unit, interval, COL0)
```

Adds an interval to a `TIMESTAMP`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`TIMESTAMPSUB`**

```sql title="Since: 0.17.0"
TIMESTAMPSUB(unit, interval, COL0)
```

Subtracts an interval from a `TIMESTAMP`.

Intervals are defined by an integer value and a supported
[time unit](../../reference/sql/time.md#Time units).

---

### **`UNIX_DATE`**

```sql title="Since: 0.6.0"
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

---

### **`UNIX_TIMESTAMP`**

```sql title="Since: 0.6.0"
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

---

## **URLs**

All ksqlDB URL functions assume URI syntax defined in
[RFC 39386](https://tools.ietf.org/html/rfc3986). For more information on the
structure of a URI, including definitions of the various components, see
[Section 3 of the RFC](https://datatracker.ietf.org/doc/html/rfc3986#section-3).
    
For encoding and decoding, ksqlDB uses the `application/x-www-form-urlencoded`
convention.

### **`URL_DECODE_PARAM`**

```sql title="Since: 0.6.0"
URL_DECODE_PARAM(col1)
```

Unescapes the `URL-param-encoded`_ value in `col1`.

This is the inverse of the `URL_ENCODE_PARAM` function.

```sql title="Example"
URL_DECODE_PARAM("url%20encoded") => "url encoded"
```

---

### **`URL_ENCODE_PARAM`**

```sql title="Since: 0.6.0"
URL_ENCODE_PARAM(col1)
```

Escapes the value of `col1` such that it can safely be used in URL query
parameters.

!!! note
    `URL_ENCODE_PARAM` is not the same as encoding a value for use in the path
    portion of a URL.

```sql title="Example"
URL_ENCODE_PARAM("url encoded") => "url%20encoded"
```

---

### **`URL_EXTRACT_FRAGMENT`**

```sql title="Since: 0.6.0"
URL_EXTRACT_FRAGMENT(url)
```

Extracts the fragment portion of the specified value.

Returns `NULL` if `url` is not a valid URL or if the fragment doesn't exist.

All encoded values are decoded.                         

```sql title="Examples"
URL_EXTRACT_FRAGMENT("http://test.com#frag") => "frag"
URL_EXTRACT_FRAGMENT("http://test.com#frag%20space") => "frag space"
```

---

### **`URL_EXTRACT_HOST`**

```sql title="Since: 0.6.0"
URL_EXTRACT_HOST(url)
```

Extracts the host-name portion of the specified value.

Returns `NULL` if `url` is not a valid URI according to RFC-2396.                       

```sql title="Example"
URL_EXTRACT_HOST("http://test.com:8080/path") => "test.com"
```

---

### **`URL_EXTRACT_PARAMETER`**

```sql title="Since: 0.6.0"
URL_EXTRACT_PARAMETER(url, parameter_name)
```

Extracts the value of the requested parameter from the query-string of `url`.

Returns `NULL` if the parameter is not present, has no value specified for it
in the query string, or `url` is not a valid URI.

The function encodes the parameter and decodes the output.

To get all parameter values from a URL as a single string, use
`URL_EXTRACT_QUERY.`

```sql title="Examples"
URL_EXTRACT_PARAMETER("http://test.com?a%20b=c%20d", "a b") => "c d"
URL_EXTRACT_PARAMETER("http://test.com?a=foo&b=bar", "b") => "bar"
```

---

### **`URL_EXTRACT_PATH`**

```sql title="Since: 0.6.0"
URL_EXTRACT_PATH(url)
```

Extracts the path from `url`.

Returns `NULL` if `url` is not a valid URI but returns an empty string if
the path is empty.

```sql title="Example"
URL_EXTRACT_PATH("http://test.com/path/to#a") => "path/to"
```

---

### **`URL_EXTRACT_PORT`**

```sql title="Since: 0.6.0"
URL_EXTRACT_PORT(url)
```

Extracts the port number from `url`.

Returns `NULL` if `url` is not a valid URI or does not contain
an explicit port number.            

```sql title="Example"
URL_EXTRACT_PORT("http://localhost:8080/path") => "8080"
```

---

### **`URL_EXTRACT_PROTOCOL`**

```sql title="Since: 0.6.0"
URL_EXTRACT_PROTOCOL(url)
```

Extracts the protocol from `url`.

Returns `NULL` if `url` is an invalid URI or has no protocol.

```sql title="Example"
URL_EXTRACT_PROTOCOL("http://test.com?a=foo&b=bar") => "http"
```

---

### **`URL_EXTRACT_QUERY`**

```sql title="Since: 0.6.0"
URL_EXTRACT_QUERY(url)
```

Extracts the decoded query-string portion of `url`.

Returns `NULL` if no query-string is present or `url` is not a valid URI.

```sql title="Example"
URL_EXTRACT_QUERY("http://test.com?a=foo%20bar&b=baz") => "a=foo bar&b=baz"
```

---

## **Deprecated**

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

**Deprecated since 0.17.0 (use PARSE_TIMESTAMP)**

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

**Deprecated since 0.17.0 (use [FORMAT_TIMESTAMP](#format_timestamp))**

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

!!! note
    To use the [`FORMAT_TIMESTAMP`](#format_timestamp) function with a BIGINT millisecond timestamp
    parameter, convert the millisecond value to a `TIMESTAMP` by using the
    `FROM_UNIXTIME` function, for example:

    ```sql
    FORMAT_TIMESTAMP(FROM_UNIXTIME(unix_timestamp))
    ```