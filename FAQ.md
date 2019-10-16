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
-- Create Trades table - ROWKEY stores the TradeId:
create table Trades(AccountId int, Amount double) with (KAFKA_TOPIC = 'TradeHistory', VALUE_FORMAT = 'JSON');

-- Run this in a different console to see the results of the aggregation:
select ROWKEY, count(*) as Count, sum(Amount) as Total from Trades group by AccountId;

-- When:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (1, 1, 106.0);

-- Then the above select window outputs:
1 | 1 | 106.0 

-- When:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (1, 1, 107.0);

-- Then the above select window outputs:
1 | 0 | 0.0
1 | 1 | 107.0
```

The question is _why_ does the second insert result in _two_ output events, rather than one?

#### Answer

When KSQL sees an update to an existing row in a table it internally emits a CDC event, which contains the old and new value. 
Aggregations handle this by first undoing the old value, before applying the new value. 

So, in the example above, when the second insert happens it's actually _updating_ an existing row in the `Trades` table. 
So KSQL first undos the old value. This results in the `COUNT` going down by `1` to `0`, and the `SUM` going down by the old value of `106.0` to `0`. 
Then KSQL applies the new row value, which sees the `COUNT` going up by `1` to `1` and the `SUM` going up by the new value `107.0` to `107.0`.

If a third insert was done for the same `TradeId` and `AccountId` then the same pattern would be seen, i.e. first the old value would be removed,
resulting in the count and sum going to zero, before the new row is added.

Why does KSQL do this? Well, to help understand what might at first look like strange behaviour, let's consider what happens when we add a few more rows:

```sql
-- When insert with different tradeId, but same AccountId:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (2, 1, 10.0);

-- Then select window will output
-- COUNT is now 2, as 2 source table rows are contributing to the aggregate
-- SUM is not 117.0, as this is the sum of the two source row's Amount 
1 | 2 | 117.0 

-- When update the new trade to reference a different AccountId:
INSERT INTO Trades (RowKey, AccountId, Amount) VALUES (2, 3, 10.0);

-- Then the above select window outputs:
-- First KSQL undoes the old value for tradeId 2:
--   This drops the count of trades against AccountId 1 to a single trade
--   And drops the sum of Amount for AccountId 1 to the amount of that single trade
1 | 1 | 107.0
-- Then it applies the new value for tradeId 2:
--   This outputs a new row with AccountId = 2
--   With a single trade, so COUNT of 1 and SUM of 10.0
2 | 1 | 10.0
```

Hopefully, the above example makes the behaviour seem a little less strange. 
Undoing the old value is important to ensure the aggregate values are correct.
The behaviour only seems _strange_ when the old and new values both affect the same aggregate row. 
We have a [Github issue](https://github.com/confluentinc/ksql/issues/3560) to enhance KSQL to make use of Kafka Stream's Suppression functionality, 
which would allow users control how results are materialized, and avoid the intermediate output when the old and new values affect the same aggregate row.

## Joins