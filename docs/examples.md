# Examples

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start-guide) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) | Examples | [FAQ](/docs/faq.md#faq)  | [Roadmap](/docs/roadmap.md#roadmap) | [Demo](/docs/demo.md#demo) |
|---|----|-----|----|----|----|----|----|

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

Here are some example queries to illustrate the look and feel of the KSQL syntax.

### Filter an inbound stream of page views to only show errors

```sql
SELECT STREAM request, ip, status 
 WHERE status >= 400
```

### Create a new stream that contains the pageviews from female users only

```sql
CREATE STREAM pageviews_by_female_users AS
  SELECT users.userid AS userid, pageid, regionid, gender FROM pageviews
  LEFT JOIN users ON pageview.userid = users.userid
  WHERE gender = 'FEMALE';
```

### Continuously compute the number of pageviews for each page with 5-second tumbling windows

```sql
CREATE TABLE pageview_counts AS
  SELECT pageid, count(*) FROM pageviews
  WINDOW TUMBLING (size 5 second)
  GROUP BY pageid;
```	

