# KLIP 61 - Bootstrapped TABLE subscriptions

**Author**: Natea Eshetu Beshada @nateab |
**Release Target**: 0.27.0 |
**Status**: _In Discussion_ |
**Discussion**: _link to the design discussion PR_

**tl;dr:** We want users to be able to very easily with a single query get the current state from a `TABLE` and 
simultaneously subscribe to any future changes to that `TABLE` by specifying `EMIT ALL`.

## Motivation and background

ksqlDB is a streaming database that allows users to work with data in motion (stream processing) and data at rest (databases).
Thus, we have push queries which let you subscribe to a `TABLE` as it changes in real-time, and also pull 
queries which retrieves the current state of the table, similar to a traditional database query. To further cement ksqlDB's
positioning at the intersection of stream processing and traditional databases, it is necessary to develop features that 
seamlessly combine the two. Adding bootstrapped `TABLE` subscriptions to ksqlDB achieves this purpose by allowing users to query their 
data at rest up until now, while also subscribing to any later changes from data that is in motion. 

Users can try to achieve this functionality by simply combining a `TABLE` pull query followed by a `TABLE` push query, but this 
could result in gaps in the returned data since there are no guarantees that the push query will not miss any data after the 
pull query is executed. Implementing this feature, we will keep track off the offsets under the hood for the user so that 
we can guarantee there is no missing data. It will also be more clean and intuitive for users to have to only use one query
to achieve this.

## What is in scope

- Queries should be supported on `TABLES` with `EMIT ALL` at the end.
- Bootstrapped subscriptions should retrieve all current rows in the `TABLE` and subscribe to any further changes.
- Bootstrapped subscriptions should not terminate until the user quits them.
- Bootstrapped subscriptions will have feature parity with push queries v2, such as filters and projections.

## What is not in scope

- Bootstrapped subscriptions will not be supported on `STREAMS`. Instead, we will throw an informative error that tells the
user to use `SET 'auto.offset.reset'='earliest';` with a push query in order to get all past data in the `STREAM` while 
simultaneously subscribing to any future updates. This is because unlike `TABLES` which have a materialized current state that a pull query retrieves,
there is no current state for a `STREAM`. 

## Value/Return

Adding this feature will ensure that users do not miss any data by trying to implement their own bootstrapped `TABLE` 
subscription by combining a pull query and a push query. It also reduces the potential complexity by only needing a single
query to get both the current state of a `TABLE` and any future changes to that `TABLE`.

## Public APIS

Users will be able to add an `EMIT ALL` clause at the end of their push query v2 on a table in order to indicate that they
want to perform a bootstrapped `TABLE` subscription. The proposed syntax would then look this:

```SQL
SET 'ksql.query.push.v2.enabled' = 'true';
SELECT select_expr [, ...]                 
  FROM table
  [ WHERE where_condition ]
  [ AND and_condition ]
  EMIT ALL;
```

## Design

For a bootstrapped `TABLE` subscription, we will peek at the latest offset in the ksqlDB output topic using the kafka admin
console. We will then save this offset and start the pull query, which can now take as long as needed since we have already
saved the latest offset. Once the pull query finishes and returns the current state, we can now begin the subscription 
and supply the saved offset as the starting subscription token to the push query in order to receive any future changes.

One limitation of this design is that there is a race condition between getting the latest offsets in the ksqlDB output 
topic and executing the pull query, since the pull query can possibly see new data. This can result in "time travel" 
once the subscription starts since we may return older results that the pull query has already returned until we catch up.
However, there is future work that is possible to address this limitation, such as getting the output offsets from the 
materialized data, or from a cache which maps upstream offsets to output offsets.

## Test plan

- Unit testing.
- Integration testing in `ksqldb-rest-app` to ensure that modules are working as expected when combined.
- Functional testing through the rest query validation tests (RQTT) to ensure that we are returning the correct results 
from our bootstrapped subscriptions. This will require some updates to the RQTT framework in order to produce input before
and after the query is run. 

## Documentation Updates

We will update the documentation around push queries v2 (which still need to be added), to include the new `EMIT ALL` syntax
along with some examples of bootstrapped `TABLE` subscriptions. We would also need to add `EMIT ALL` to the SQL quick reference. 

## Compatibility Implications

Since this feature is adding support for a new query, there are no expected compatibility implications.

## Security Implications

N/A
