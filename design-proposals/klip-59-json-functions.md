# KLIP-59 - JSON functions

**Authors**: Aleksandr Sorokoumov (@gerrrr), Colin Hicks (@colinhicks)
**Release Target**: TBD
**Status**: _Approved_ |
**Discussion**: [GitHub PR](https://github.com/confluentinc/ksql/pull/8550)

## Motivation and background

Many ksqlDB users leverage JSON. ETL pipelines often deal in large JSON blobs from, e.g., MongoDB.
As the de-facto format of the web, it is often the format of analytics events produced to Kafka, as
well as a choice for materialized views consumed by web apps.

We want to position ksqlDB as a JSON-friendly database. Support for reading, writing, and
traversing data as JSON presents an important value-add. And users of traditional databases like
PostgreSQL already enjoy powerful support for JSON.

Today, ksqlDB can:
* Read JSON-formatted keys and values in input records.
* Serialize keys and values as JSON in output records.
* Extract a string value at a given JSON path.


## What is in scope

* Validate if a given string contains JSON value
* Calculate the length of a JSON array encoded in a string
* Extract keys and records from a given JSON-formatted string
* Concatenate JSON-formatted strings
* Convert a ksqlDB record to JSON-formatted string

## What is not in scope

* A JSON datatype
* Binary JSON support

## Public APIs

Users can use ksqlDB’s varchar type on columns that they know are JSON formatted. A JSON-formatted
string can also be nested within a map or struct.

### is_json_string

```
is_json_string(json_string) -> Boolean
```

Given a string, returns true if it can be parsed as a valid JSON value, false otherwise.

#### Examples

```
is_json_string("[1, 2, 3]") // returns true
is_json_string("{}") // returns true
is_json_string("1") // returns true
is_json_string("\"abc\"") // returns true
is_json_string("null") // returns true
is_json_string("") // returns false
is_json_string("abc") // returns false
is_json_string(NULL) // returns false
```


### json_array_length

```
json_array_length(json_string) -> Integer
```

Given a string, parses it as a JSON value and returns the length of the top-level array. Returns
`NULL` if the string can't be interpreted as a JSON array, i.e., it is `NULL` or it does not contain
valid JSON, or the JSON value is not an array.

#### Examples

```
json_array_length("[1, 2, 3]") // returns 3
json_array_length("[1, [1, [2]], 3]") // returns 3
json_array_length("[]") // returns 0
json_array_length("{}") // returns NULL
json_array_length("123") // returns NULL
json_array_length("abc") // returns NULL
json_array_length(NULL) // returns NULL
```

### json_keys

```
json_keys(json_string) -> Array<String>
```

Given a string, parses it as a JSON object and returns a ksqlDB array of strings representing the
top-level keys. Returns `NULL` if the string can't be interpreted as a JSON object, i.e., it is
`NULL` or it does not contain valid JSON, or the JSON value is not an object.

#### Examples

```
json_keys("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}") // returns ["a", "b", "d"]
json_keys("{}") // returns []
json_keys("[]") // returns NULL
json_keys("") // returns NULL
json_keys("123") // returns NULL
json_keys("abc") // returns NULL
json_keys(NULL) // returns NULL
```

### json_records

```
json_records(json_string) -> Map<String, String>
```

Given a string, parses it as a JSON object and returns a map representing the top-level keys and 
values. Returns `NULL` if the string can't be interpreted as a JSON object, i.e. it is `NULL` or 
it does not contain valid JSON, or the JSON value is not an object.

#### Examples

```
json_records("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}") // {"a": "\"abc\"", "b": "{ \"c\": \"a\" }", "d": "1"}
json_records("{}") // returns []
json_records("[]") // returns NULL
json_records("") // returns NULL
json_records("123") // returns NULL
json_records("abc") // returns NULL
json_records(NULL) // returns NULL
```

#### Rejected alternatives

**Remove redundant quotes around values**. For example:

```
json_records("{\"a\": \"abc\"}) // returns [["a", "abc"]]
```

This makes the output less awkward, but removes the type information in certain cases. In the following
case it is not possible to derive whether `1` is a string or a number:

```
json_records("{\"a\": \"1\"}) // returns [Struct{json_key="a", json_value="1"}]
```

**Return an array of arrays rather than an array of structs**. For example:
```
json_records("{\"a\": \"abc\"}) // returns [["a", "abc"]]
```

A struct is a better return type in this case as it enables meaningful field names and reduces
the number of potential sanity checks on the end-user side (e.g., array length).

**Return an array of structs**. For example:

Declaration:

```
json_records(json_string) -> Array<Struct<json_key:String, json_value:String>>
```

Usage:
```
json_records("{\"a\": \"abc\", \"b\": { \"c\": \"a\" }, \"d\": 1}") // returns [Struct{json_key="a", json_value="\"abc\""}, Struct{json_key="b", json_value="{ \"c\": \"a\" }}, Struct{json_key="d", json_value="1"}]
```

While this return type provides the same amount information as `Map<String, String>`, the latter
also allows retrieving specific keys in constant time. 

### json_concat

```
json_concat(json_string, json_string) -> String
```

Given 2 strings, parse them as JSON values and return a string representing their concatenation.
Concatenation rules are identical to PostgreSQL's [|| operator](https://www.postgresql.org/docs/14/functions-json.html):
* If both strings deserialize into JSON objects, then return an object with a union of the input key,
  taking values from the second object in the case of duplicates.
* If both strings deserialize into JSON arrays, then return the result of array concatenation.
* If at least one of the deserialized values is not an object, then convert non-array inputs to a
  single-element array and return the result of array concatenation.
* If at least one of the input strings is `NULL` or can't be deserialized as JSON, then return `NULL`.

Akin to PostgreSQL's `||` operator, this function merges only top-level object keys or arrays.

#### Examples

```
json_concat("{\"a\": 1}", "{\"b\": 2}") // returns "{\"a\": 1, \"b\": 2}"
json_concat("{\"a\": {\"5\": 6}}", "{\"a\": {\"3\": 4}}") // returns "{\"a\": {\"3\": 4}}"
json_concat("{}", "{}") // returns "{}"
json_concat("[1, 2]", "[3, 4]") // returns "[1,2,3,4]"
json_concat("[1, [2]]", "[[[3]], [[[4]]]]") // returns "[ 1, [2], [[3]], [[[4]]] ]"
json_concat("null", "null") // returns [null, null]
json_concat("[1, 2]", "{\"a\": 1}") // returns "[1, 2, {\"a\": 1}]"
json_concat("[1, 2]", "3") // returns "[1, 2, 3]"
json_concat("1", "2") // returns "[1, 2]"
json_concat("[]", "[]") // returns []
json_concat("abc", "[1]") // returns NULL
json_concat(NULL, "[1]") // returns NULL
```

#### Rejected alternatives

If one argument represents a valid JSON and the other one does not, then return the valid string
as a result of concatenation.

Given that concatenation of invalid JSON strings is an exceptional situation, we should return
`NULL` if any input is not a JSON. This way, we avoid ambiguity if a user needs to trace back how
they got certain results.

If this function returns the valid record in the case when the other one is invalid, from the user
perspective, there are 3 possibilities how they ended up with this result:
* The second object could have been empty.
* The second object could have had the same keys/values.
* The second object is invalid JSON.

By always returning `NULL` given an invalid JSON string we remove the third, the least obvious,
possibility which reduces cognitive load from the user.

### to_json_string

```
to_json_string(val) -> String
```

Given any ksqlDB type returns the equivalent JSON string.

#### Examples

**Primitives types**

```
to_json_string(1) // returns "1"
to_json_string(15.3) // returns "15.3"
to_json_string("abc") // returns "\"abc\""
to_json_string(true) // returns "true"
to_json_string(2021-10-11) // DATE type, returns "\"2021-10-11\""
to_json_string(13:25) // TIME type, returns "\"13:25\""
to_json_string(2021-06-31T12:18:39.446) // TIMESTAMP type, returns "\"2021-06-31T12:18:39.446\""
to_json_string(NULL) // returns "null"
```

**Compound types**
```
to_json_string(Array[1, 2, 3]) // returns "[1, 2, 3]"
to_json_string(Struct{id=1,name=A}) // returns "{\"id\": 1, \"name\": \"a\"}"
to_json_string(Map('c' := 2, 'd' := 4)) // returns "{\"c\": 2, \"d\": \"4\"}"
to_json_string(Array[Struct{json_key=1 json_value=Map('c' := 2, 'd' := true)}]) // returns "[{\"json_key\": 1, \"json_value\": {\"c\": 2, \"d\": true}}]"
```

Custom types are processed in the same way as the types they alias.

#### Rejected alternatives

Return `NULL` given `NULL` as input. Although, it might make sense from the `NULL` propagation point
of view, there are several arguments why we should map SQL `NULL` to JSON `null`:
* Akin to SQL's `NULL`, JSON's `null` is a marker of absent value, so semantics does not change.
* Returning SQL `NULL` would make it impossible to serialize any input to JSON `null`.

## Design

Akin to the existing `EXTRACTJSONFIELD` and `JSON_ARRAY_CONTAINS`, the general workflow for all
functions is to parse the given string(s) into jackson's `JsonNode`s and perform manipulations according
to the functions' specifications.

This approach might be suboptimal from the performance point of view since every function invocation
completely deserializes the entire structure and then serializes the result back. However, the best
option for performance-sensitive JSON manipulation is the introduction of native JSON types that
avoids the serde step altogether. In turn, string manipulation is easy to implement, and it enables
the majority of common use cases.

## Test Plan

Add unit tests, QTT tests for the new functions.

## Documentation Updates

Add functions description to [Scalar functions docs](https://github.com/confluentinc/ksql/blob/master/docs/developer-guide/ksqldb-reference/scalar-functions.md).
