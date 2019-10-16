# FAQ
This doc contains answers frequently asked questions. Answers are broken down into the following sections:

* [Basics / general](#basics)
* [Aggregations](#aggregations)
* [Joins](#joins)

## Basics



## Aggregations

### Why does KSQL output two events for each input event when aggregating a table?

Inspired by [this SO question](https://stackoverflow.com/questions/58358784/ksql-query-returning-unexpected-values-in-simple-aggregation)

#### Example

```sql
-- Given:
-- Create Trades table:
create table Trades(AccountId int, Amount double) with (KAFKA_TOPIC = 'TradeHistory', VALUE_FORMAT = 'JSON');

-- Run this in a different console to see the results of the aggregation:
select count(*) as Count, sum(Amount) as Total from Trades group by AccountId;

-- When:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (1, 1, 106.0);

-- Then the above select window outputs:
1 | 106.0 

-- When:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (1, 1, 107.0);

-- Then the above select window outputs:
0 | 0.0
1 | 107.0
```

The question is _why_ does the second insert result in _two_ output events, rather than one?

#### Answer

When KSQL sees an update to an existing row in a table it internally emits a CDC event, which contains the old and new value. 
Aggregations handle this by first 'undo'ing the old value, before applying the new value. 

So, in the example above, when the second insert happens, KSQL first undos the old value. 
So we see the `COUNT` going down by 1, and the `SUM` going down by the old value of `106.0`. 
Then KSQL applies the new row value, which sees the `COUNT` going up by 1 and the `SUM` going up by the new value `107.0`.

We have a [Github issue](https://github.com/confluentinc/ksql/issues/3560) to enhance KSQL to make use of Kafka Stream's Suppression functionality, 
which would allow users control how results are materialized.

## Joins