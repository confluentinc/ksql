Changelog
=========

Version 5.2.0
-------------

KSQL 5.2 includes new features, including:

KSQL 5.2 includes bug fixes, including:

* Improved support for multi-line requests to the rest-api, where statements are interdependent.
  Prior to v5.2 KSQL parsed the full request before attempting to execute any statements.
  Requests that contained later statements that were dependant on prior statements having executed
  would like of failed. From this release this should no longer be an issue.

* Improved support for non-interactive a.k.a headless mode. scripts what would previously fail
  Prior to v5.2 KSQL parsed the full script before attempting to execute any statements.
  The full parse would often fail where later statements relied on earlier statements having been
  correctly executed. From this release this should no longer be an issue.

Version 5.1.0
-------------

KSQL 5.1 includes new features, including:

* ``WindowStart()`` and ``WindowEnd()`` UDFs
* ``StringToDate()`` and ``DateToString()`` UDFs

Detailed Changlog
+++++++++++++++++

* `PR-2265 <https://github.com/confluentinc/ksql/pull/2265>`_ - MINOR: Fix bug encountered when restoring RUN SCRIPT
* `PR-2240 <https://github.com/confluentinc/ksql/pull/2240>`_ - Bring version checker improvements to 5.1.x
* `PR-2242 <https://github.com/confluentinc/ksql/pull/2242>`_ - KSQL-1795: First draft of STRUCT topic
* `PR-2235 <https://github.com/confluentinc/ksql/pull/2235>`_ - KSQL-1794: First draft of query with arrays and maps topic
* `PR-2239 <https://github.com/confluentinc/ksql/pull/2239>`_ - KSQL-1975: Fix munged Docker commands for kafkacat examples
* `PR-2232 <https://github.com/confluentinc/ksql/pull/2232>`_ - KSQL-1912: Fix munged scalar functions table
* `PR-2229 <https://github.com/confluentinc/ksql/pull/2229>`_ - KSQL-1912: Remove extraneous newline
* `PR-2227 <https://github.com/confluentinc/ksql/pull/2227>`_ - KSQL-1912: Add IFNULL to Scalar Functions table
* `PR-2219 <https://github.com/confluentinc/ksql/pull/2219>`_ - KSQL-1912: Add IFNULL function to functions table
* `PR-2223 <https://github.com/confluentinc/ksql/pull/2223>`_ - KSQL-1958: Fix munged CSAS properties table YET AGAIN
* `PR-2222 <https://github.com/confluentinc/ksql/pull/2222>`_ - KSQL-1957: Add links to new topics; also restore missing CSAS and CTAS text
* `PR-2221 <https://github.com/confluentinc/ksql/pull/2221>`_ - DOCS-960: Add link to partitioning topic in key requirements section
* `PR-2220 <https://github.com/confluentinc/ksql/pull/2220>`_ - DOCS-960: Add note about the KEY property
* `PR-2134 <https://github.com/confluentinc/ksql/pull/2134>`_ - KSQL-1787: First draft of Time and Windows topic
* `PR-2201 <https://github.com/confluentinc/ksql/pull/2201>`_ - KSQL-1930: Fix a typo in the new Transform a Stream topic
* `PR-2180 <https://github.com/confluentinc/ksql/pull/2180>`_ - KSQL-1797: First draft of Transform a Stream topic
* `PR-2181 <https://github.com/confluentinc/ksql/pull/2181>`_ - KSQL-1796: First draft of aggregation topic
* `PR-2136 <https://github.com/confluentinc/ksql/pull/2136>`_ - Add reference about compatibility breaking configs in upgrade docs
* `PR-2193 <https://github.com/confluentinc/ksql/pull/2193>`_ - Fix flaky json format test
* `PR-2195 <https://github.com/confluentinc/ksql/pull/2195>`_ - 5.0.x fix flaky
* `PR-2174 <https://github.com/confluentinc/ksql/pull/2174>`_ - DOCS-1006: Fix munged :: block
* `PR-2170 <https://github.com/confluentinc/ksql/pull/2170>`_ - DOCS-911: Fix typos and grammatical errors
* `PR-2169 <https://github.com/confluentinc/ksql/pull/2169>`_ - DOCS-911: Fix typos and grammatical errors
* `PR-2142 <https://github.com/confluentinc/ksql/pull/2142>`_ - KSQL-1786: First draft of KSQL and KStreams topic
* `PR-2165 <https://github.com/confluentinc/ksql/pull/2165>`_ - KSQL-1854: Merge partition sections
* `PR-2143 <https://github.com/confluentinc/ksql/pull/2143>`_ - Fix some bugs in recovery logic
* `PR-2156 <https://github.com/confluentinc/ksql/pull/2156>`_ - KSQL-1864: Remove ksql> prompt from example commands
* `PR-2155 <https://github.com/confluentinc/ksql/pull/2155>`_ - KSQL-1864: Remove ksql> prompt from example commands
* `PR-2152 <https://github.com/confluentinc/ksql/pull/2152>`_ - KSQL-1864: Remove $ chars prompts for example commands
* `PR-2150 <https://github.com/confluentinc/ksql/pull/2150>`_ - Currently we don't support AS for aliasing stream/table.
* `PR-2149 <https://github.com/confluentinc/ksql/pull/2149>`_ - Using ksql topic name instead of Kafka topic name in topic map in metastore.
* `PR-2137 <https://github.com/confluentinc/ksql/pull/2137>`_ - Clarify the description of SUBSTRING and its legacy mode setting.
* `PR-2120 <https://github.com/confluentinc/ksql/pull/2120>`_ - KSQL-1789: First draft of Create a KSQL Table topic
* `PR-2132 <https://github.com/confluentinc/ksql/pull/2132>`_ - KSQL-1853: Fix heading levels in join and partition topics
* `PR-2130 <https://github.com/confluentinc/ksql/pull/2130>`_ - DOCS-950: Reworked partitions topic per feedback
* `PR-2122 <https://github.com/confluentinc/ksql/pull/2122>`_ - Bringing back the commit that was lost because of bad merge.
* `PR-2109 <https://github.com/confluentinc/ksql/pull/2109>`_ - KSQL-1799: New topic: Troubleshoot KSQL
* `PR-2092 <https://github.com/confluentinc/ksql/pull/2092>`_ - Window's UDF doc changes.
* `PR-2090 <https://github.com/confluentinc/ksql/pull/2090>`_ - Add WindowStart and WindowEnd UDFs (#1993)
* `PR-2075 <https://github.com/confluentinc/ksql/pull/2075>`_ - Disable optimizations for 5.1.x
* `PR-2051 <https://github.com/confluentinc/ksql/pull/2051>`_ - Preserve originals when merging configs
* `PR-2080 <https://github.com/confluentinc/ksql/pull/2080>`_ - Fixed the test.
* `PR-2079 <https://github.com/confluentinc/ksql/pull/2079>`_ - Fix deprecation issues.
* `PR-2031 <https://github.com/confluentinc/ksql/pull/2031>`_ - Fix deprecated issues in the build
* `PR-2066 <https://github.com/confluentinc/ksql/pull/2066>`_ - Minor: Fix bug involving filters with NOT keyword.
* `PR-2056 <https://github.com/confluentinc/ksql/pull/2056>`_ - Added stringtodate and datetostring UDFs for 5.1.x
* `PR-2048 <https://github.com/confluentinc/ksql/pull/2048>`_ - Minor: Fix bug involving LIKE patterns without wildcards.
* `PR-2045 <https://github.com/confluentinc/ksql/pull/2045>`_ - List UDAFs for 5.1.x
* `PR-2043 <https://github.com/confluentinc/ksql/pull/2043>`_ - Bump airline version to 2.6.0
* `PR-2023 <https://github.com/confluentinc/ksql/pull/2023>`_ - MINOR: Cause 'ksql help' and 'ksql -help' to behave the same as 'ksql -h' and 'ksql --help'
* `PR-1979 <https://github.com/confluentinc/ksql/pull/1979>`_ - Metrics refactor + fix a couple issues
* `PR-2018 <https://github.com/confluentinc/ksql/pull/2018>`_ - Display stats timestamps in unambiguous format.
* `PR-2017 <https://github.com/confluentinc/ksql/pull/2017>`_ - KSQL-1725: Fix tables and build warnings
* `PR-1997 <https://github.com/confluentinc/ksql/pull/1997>`_ - MINOR: Remove duplicate junit dependency in ksql-examples
* `PR-2014 <https://github.com/confluentinc/ksql/pull/2014>`_ - KSQL-1722: Fix broken inline literal
* `PR-2007 <https://github.com/confluentinc/ksql/pull/2007>`_ - KSQL-1722: Fix build error in changelog.rst
* `PR-1991 <https://github.com/confluentinc/ksql/pull/1991>`_ - Minor: Switch tests to use mock Kafka clients.
* `PR-1992 <https://github.com/confluentinc/ksql/pull/1992>`_ - Minor: Improve test output for QueryTranslationTest
* `PR-1999 <https://github.com/confluentinc/ksql/pull/1999>`_ - KSQL-1717: Fix build warning in faq.rst
* `PR-1981 <https://github.com/confluentinc/ksql/pull/1981>`_ - ST-1153: Switch to use cp-base-new and bash-config to hide passwords by default
* `PR-1977 <https://github.com/confluentinc/ksql/pull/1977>`_ - Use version 5.0.0 for KSQL server image
* `PR-1955 <https://github.com/confluentinc/ksql/pull/1955>`_ - Hide ssl configs and refactor KsqlResourceTest
