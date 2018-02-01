# Change Log

## [v0.4](https://github.com/confluentinc/ksql/tree/v0.4) (2018-02-01)
[Full Changelog](https://github.com/confluentinc/ksql/compare/v0.3...v0.4)

**Implemented enhancements:**

- Integration with Schema Registry [\#483](https://github.com/confluentinc/ksql/issues/483)

**Closed issues:**

- KSQL Avro format seems to be incompatible with the avro format expected by Kafka connect. [\#651](https://github.com/confluentinc/ksql/issues/651)
- CSAS execution silently fails [\#643](https://github.com/confluentinc/ksql/issues/643)
- Enhance 'print topic;' to display kafka topics [\#625](https://github.com/confluentinc/ksql/issues/625)
- ksql-server-start will hang when it "Unable to check broker compatibility" [\#611](https://github.com/confluentinc/ksql/issues/611)
- UDF function to search if the value exists in the json array [\#607](https://github.com/confluentinc/ksql/issues/607)
- CSAS: WITH is incompatible with WHERE?  [\#604](https://github.com/confluentinc/ksql/issues/604)
- Timestamp field appears not to work in DESCRIBE EXTENDED [\#603](https://github.com/confluentinc/ksql/issues/603)
- EXTRACTJSONFIELD JSON array support [\#523](https://github.com/confluentinc/ksql/issues/523)
- Extend the DESCRIBE statement to more useful information [\#470](https://github.com/confluentinc/ksql/issues/470)

**Merged pull requests:**

- Update quickstart docs with new output for describe [\#684](https://github.com/confluentinc/ksql/pull/684) ([apurvam](https://github.com/apurvam))
- Expect ksql-run-class parameters after the class name [\#683](https://github.com/confluentinc/ksql/pull/683) ([rodesai](https://github.com/rodesai))
- Bump version to 0.4 [\#682](https://github.com/confluentinc/ksql/pull/682) ([apurvam](https://github.com/apurvam))
- Make sure each Beacon ping includes current time [\#681](https://github.com/confluentinc/ksql/pull/681) ([apovzner](https://github.com/apovzner))
- Update the start scripts so that the ksql server always generates a gc log [\#674](https://github.com/confluentinc/ksql/pull/674) ([apurvam](https://github.com/apurvam))
- More informative help output for print-metrics [\#671](https://github.com/confluentinc/ksql/pull/671) ([rodesai](https://github.com/rodesai))
- Cos integration for KSQL. [\#663](https://github.com/confluentinc/ksql/pull/663) ([hjafarpour](https://github.com/hjafarpour))
- Remove suppression of method length check [\#662](https://github.com/confluentinc/ksql/pull/662) ([dguy](https://github.com/dguy))
- Allow passing a properties file to the quickstart examples [\#657](https://github.com/confluentinc/ksql/pull/657) ([mimaison](https://github.com/mimaison))
- enable checkstyle check to avoid star import [\#654](https://github.com/confluentinc/ksql/pull/654) ([dguy](https://github.com/dguy))
- update datagen so iteration works properly and add schemas to use for testing [\#653](https://github.com/confluentinc/ksql/pull/653) ([dguy](https://github.com/dguy))
- Use PhoneHomeConfig for KSQL Beacon [\#648](https://github.com/confluentinc/ksql/pull/648) ([apovzner](https://github.com/apovzner))
- updates for line length style check [\#645](https://github.com/confluentinc/ksql/pull/645) ([norwood](https://github.com/norwood))
- use EntityUtils instead of manually reading entity [\#644](https://github.com/confluentinc/ksql/pull/644) ([norwood](https://github.com/norwood))
- Add ksql-tools [\#634](https://github.com/confluentinc/ksql/pull/634) ([rodesai](https://github.com/rodesai))
- Tests for cli module plus further tests for refactoring. [\#626](https://github.com/confluentinc/ksql/pull/626) ([dguy](https://github.com/dguy))
- Enhance show topics; and print "topic"; [\#624](https://github.com/confluentinc/ksql/pull/624) ([bluemonk3y](https://github.com/bluemonk3y))
- \[BUG\] jsonNode array index access for EXTRACTJSONFIELD [\#610](https://github.com/confluentinc/ksql/pull/610) ([robert2d](https://github.com/robert2d))
- \[UDF\] JSON Array Contains function [\#608](https://github.com/confluentinc/ksql/pull/608) ([satybald](https://github.com/satybald))
- \[Minor\] Fix Variable initialization [\#602](https://github.com/confluentinc/ksql/pull/602) ([satybald](https://github.com/satybald))

## [v0.3](https://github.com/confluentinc/ksql/tree/v0.3) (2018-01-16)
[Full Changelog](https://github.com/confluentinc/ksql/compare/v0.3-temp...v0.3)

**Fixed bugs:**

- Broker compatibility check needs at least one topic to exist [\#612](https://github.com/confluentinc/ksql/issues/612)

**Closed issues:**

- Compilation issue: assembly-plugin group id '113584762' is too big [\#590](https://github.com/confluentinc/ksql/issues/590)
- Quickstart Non-Docker Setup for KSQL encounters error during compilation and cannot find symbol "class ZkUtilsProvider" [\#582](https://github.com/confluentinc/ksql/issues/582)
- Add system test - Rolling bounces [\#467](https://github.com/confluentinc/ksql/issues/467)
- Add system test - Core Kafka broker failures \(one node or complete cluster, hard and soft\) [\#466](https://github.com/confluentinc/ksql/issues/466)
- Add system test - lose a stream or rest node \(due to OOM or crash\)  [\#465](https://github.com/confluentinc/ksql/issues/465)
- Make KSQL emit warning message when running against non-compliant AK, CK version [\#464](https://github.com/confluentinc/ksql/issues/464)

**Merged pull requests:**

- restore assembly configs [\#621](https://github.com/confluentinc/ksql/pull/621) ([aayars](https://github.com/aayars))
- remove docker boilerplate now provided by common [\#618](https://github.com/confluentinc/ksql/pull/618) ([aayars](https://github.com/aayars))
- create topic if none exist during broker compat test \#612 [\#617](https://github.com/confluentinc/ksql/pull/617) ([dguy](https://github.com/dguy))
- Fix queries with limit [\#615](https://github.com/confluentinc/ksql/pull/615) ([rodesai](https://github.com/rodesai))
- Refactoring to use schema type for comparison. [\#614](https://github.com/confluentinc/ksql/pull/614) ([hjafarpour](https://github.com/hjafarpour))
- Use Apache commons CSV package for both serializer and deserializer i… [\#613](https://github.com/confluentinc/ksql/pull/613) ([hjafarpour](https://github.com/hjafarpour))
- COS packaging for KSQL. [\#609](https://github.com/confluentinc/ksql/pull/609) ([hjafarpour](https://github.com/hjafarpour))
- KSQL-473: point quickstart non-docker docs to release tarballs [\#601](https://github.com/confluentinc/ksql/pull/601) ([rodesai](https://github.com/rodesai))
- Fix code style issues [\#599](https://github.com/confluentinc/ksql/pull/599) ([satybald](https://github.com/satybald))
- MINOR: Further code cleanups. [\#597](https://github.com/confluentinc/ksql/pull/597) ([dguy](https://github.com/dguy))
- Support avro types for nullable fields  generated by Connect. [\#592](https://github.com/confluentinc/ksql/pull/592) ([hjafarpour](https://github.com/hjafarpour))
- MINOR: don't allow modification of AggregateAnalysis collections externally [\#589](https://github.com/confluentinc/ksql/pull/589) ([dguy](https://github.com/dguy))
- KSQL-508: add metrics interceptors instead of replacing [\#585](https://github.com/confluentinc/ksql/pull/585) ([rodesai](https://github.com/rodesai))
- Tests for commons package. [\#584](https://github.com/confluentinc/ksql/pull/584) ([hjafarpour](https://github.com/hjafarpour))
- Ksql 569 serde tests [\#583](https://github.com/confluentinc/ksql/pull/583) ([hjafarpour](https://github.com/hjafarpour))
- Skip the '@' character in the beginning of the field name in JSON  [\#581](https://github.com/confluentinc/ksql/pull/581) ([hjafarpour](https://github.com/hjafarpour))
- KSQL-235:  dont block on socket read [\#579](https://github.com/confluentinc/ksql/pull/579) ([rodesai](https://github.com/rodesai))
- Top k udaf [\#575](https://github.com/confluentinc/ksql/pull/575) ([hjafarpour](https://github.com/hjafarpour))

## [v0.3-temp](https://github.com/confluentinc/ksql/tree/v0.3-temp) (2018-01-05)
[Full Changelog](https://github.com/confluentinc/ksql/compare/v0.2...v0.3-temp)

**Implemented enhancements:**

- View Tables Properties for a Stream [\#516](https://github.com/confluentinc/ksql/issues/516)

**Fixed bugs:**

- Don't execute create table/create stream as select statements during startup when the entity has been dropped [\#531](https://github.com/confluentinc/ksql/issues/531)
- Don't replay all previous events found on the command topic [\#454](https://github.com/confluentinc/ksql/issues/454)
- CLI should not start up if server is down [\#360](https://github.com/confluentinc/ksql/issues/360)

**Closed issues:**

- Command topic replication factor is not set correctly. [\#564](https://github.com/confluentinc/ksql/issues/564)
- Update Avro docs for tables because of issue 557 [\#561](https://github.com/confluentinc/ksql/issues/561)
- CREATE TABLE examples do not define KEY property yet [\#559](https://github.com/confluentinc/ksql/issues/559)
- Build Failing in all versions [\#558](https://github.com/confluentinc/ksql/issues/558)
- Schema inference for tables [\#557](https://github.com/confluentinc/ksql/issues/557)
- Add avro sample data generation in examples package. [\#541](https://github.com/confluentinc/ksql/issues/541)
- Could not find artifact io.confluent:common:pom:4.0.0-SNAPSHOT [\#537](https://github.com/confluentinc/ksql/issues/537)
- GROUP BY not recognizing SELECT Elements? [\#512](https://github.com/confluentinc/ksql/issues/512)
- Can't find any documentation about ksql in Confluent 4.0.x [\#502](https://github.com/confluentinc/ksql/issues/502)
- join not supported? [\#493](https://github.com/confluentinc/ksql/issues/493)
- LEFT Join statement always return null [\#491](https://github.com/confluentinc/ksql/issues/491)
- Don't start terminated queries on ksql-server startup [\#482](https://github.com/confluentinc/ksql/issues/482)
- Error building kqsl: Non-resolvable parent POM for io.confluent.ksql:ksql-parent:4.0.0-SNAPSHOT [\#481](https://github.com/confluentinc/ksql/issues/481)
- Add basic metrics integration to KSQL [\#463](https://github.com/confluentinc/ksql/issues/463)

**Merged pull requests:**

- Fix compilation issue group id '113584762' is too big in assemly-plugin [\#591](https://github.com/confluentinc/ksql/pull/591) ([satybald](https://github.com/satybald))
- MINOR: remove unused KsqlConfig param from QueryMetadata etc [\#588](https://github.com/confluentinc/ksql/pull/588) ([dguy](https://github.com/dguy))
- Fix findbugs errors [\#587](https://github.com/confluentinc/ksql/pull/587) ([ewencp](https://github.com/ewencp))
- Fix findbugs errors [\#586](https://github.com/confluentinc/ksql/pull/586) ([ewencp](https://github.com/ewencp))
- Fixed the issue with BaseMetricsReporter. [\#578](https://github.com/confluentinc/ksql/pull/578) ([hjafarpour](https://github.com/hjafarpour))
- Updated the docs. [\#574](https://github.com/confluentinc/ksql/pull/574) ([hjafarpour](https://github.com/hjafarpour))
- Add a column for the 0.3 release to the version compatibility docs [\#571](https://github.com/confluentinc/ksql/pull/571) ([apurvam](https://github.com/apurvam))
- KSQL-473: add a package module [\#570](https://github.com/confluentinc/ksql/pull/570) ([rodesai](https://github.com/rodesai))
- The number of replicas for the command topic should be set by 'ksql.s… [\#565](https://github.com/confluentinc/ksql/pull/565) ([hjafarpour](https://github.com/hjafarpour))
- Docs improvement. [\#563](https://github.com/confluentinc/ksql/pull/563) ([hjafarpour](https://github.com/hjafarpour))
- Schema inference for tables [\#562](https://github.com/confluentinc/ksql/pull/562) ([hjafarpour](https://github.com/hjafarpour))
- Remove redundant "for more information" in DESCRIBE EXTENDED output [\#560](https://github.com/confluentinc/ksql/pull/560) ([miguno](https://github.com/miguno))
- KSQL-493: Fix producer thread leak [\#556](https://github.com/confluentinc/ksql/pull/556) ([rodesai](https://github.com/rodesai))
- Minor updates to the non docker quickstart [\#552](https://github.com/confluentinc/ksql/pull/552) ([apurvam](https://github.com/apurvam))
- Fix quickstart to have the correct schema registry url [\#551](https://github.com/confluentinc/ksql/pull/551) ([apurvam](https://github.com/apurvam))
- Clarify that Docker clickstream demo requires 5+ GB of RAM [\#549](https://github.com/confluentinc/ksql/pull/549) ([miguno](https://github.com/miguno))
- Minor docs improvements [\#548](https://github.com/confluentinc/ksql/pull/548) ([miguno](https://github.com/miguno))
- Improve DESCRIBE and EXPLAIN docs \(see PR \#539\) [\#547](https://github.com/confluentinc/ksql/pull/547) ([miguno](https://github.com/miguno))
- Docs updates for 0.3 release [\#546](https://github.com/confluentinc/ksql/pull/546) ([apurvam](https://github.com/apurvam))
- Add ability to specify the schema registry url to the CLI in local mode. [\#545](https://github.com/confluentinc/ksql/pull/545) ([apurvam](https://github.com/apurvam))
- Added avro producer for data gen for the new schema registry integrat… [\#544](https://github.com/confluentinc/ksql/pull/544) ([hjafarpour](https://github.com/hjafarpour))
- Fixed minor style issues in the docs. [\#543](https://github.com/confluentinc/ksql/pull/543) ([hjafarpour](https://github.com/hjafarpour))
- Bump ksql version to 0.3 [\#542](https://github.com/confluentinc/ksql/pull/542) ([apurvam](https://github.com/apurvam))
- Improve docs on DESCRIBE and EXPLAIN [\#539](https://github.com/confluentinc/ksql/pull/539) ([miguno](https://github.com/miguno))
-  Add tip on configuring artifactory to ensure builds are working [\#538](https://github.com/confluentinc/ksql/pull/538) ([miguno](https://github.com/miguno))
- Add ratings example [\#535](https://github.com/confluentinc/ksql/pull/535) ([blueedgenick](https://github.com/blueedgenick))
- Add Idle query and error rate metrics [\#534](https://github.com/confluentinc/ksql/pull/534) ([apurvam](https://github.com/apurvam))
- don't execute dropped streams/tables, but update metastore [\#532](https://github.com/confluentinc/ksql/pull/532) ([dguy](https://github.com/dguy))
- KSQL-484 - Add version compatibility table to the docs [\#526](https://github.com/confluentinc/ksql/pull/526) ([joel-hamill](https://github.com/joel-hamill))
- Support DESCRIBE EXTENDED, v2 [\#522](https://github.com/confluentinc/ksql/pull/522) ([bluemonk3y](https://github.com/bluemonk3y))
- Print more informative error messages [\#514](https://github.com/confluentinc/ksql/pull/514) ([rodesai](https://github.com/rodesai))
- Add metrics which report the number of running queries [\#513](https://github.com/confluentinc/ksql/pull/513) ([apurvam](https://github.com/apurvam))
- Describe extend functionality [\#511](https://github.com/confluentinc/ksql/pull/511) ([bluemonk3y](https://github.com/bluemonk3y))
- Don't run terminated queries at startup \#482 [\#508](https://github.com/confluentinc/ksql/pull/508) ([dguy](https://github.com/dguy))
- Remove outdated roadmap document [\#507](https://github.com/confluentinc/ksql/pull/507) ([miguno](https://github.com/miguno))
- Revert commits which broke the docs [\#501](https://github.com/confluentinc/ksql/pull/501) ([apurvam](https://github.com/apurvam))
- KSQL-476: Use explicit tags for docker image dependencies [\#500](https://github.com/confluentinc/ksql/pull/500) ([aayars](https://github.com/aayars))
- add broker compatibility check [\#499](https://github.com/confluentinc/ksql/pull/499) ([dguy](https://github.com/dguy))
- fix regression in app id creation [\#498](https://github.com/confluentinc/ksql/pull/498) ([dguy](https://github.com/dguy))
- Make doc links point to GH repo [\#496](https://github.com/confluentinc/ksql/pull/496) ([joel-hamill](https://github.com/joel-hamill))
- Fixed the regression for stream-table join. Table should always read … [\#495](https://github.com/confluentinc/ksql/pull/495) ([hjafarpour](https://github.com/hjafarpour))
- Add list of actual results to CLITest output when it fails [\#494](https://github.com/confluentinc/ksql/pull/494) ([apurvam](https://github.com/apurvam))
- change replication factor check when creating topics [\#492](https://github.com/confluentinc/ksql/pull/492) ([dguy](https://github.com/dguy))
- Add snapshots repo for Confluent dependencies [\#490](https://github.com/confluentinc/ksql/pull/490) ([aayars](https://github.com/aayars))
- Ksql 415 schema registry integration [\#488](https://github.com/confluentinc/ksql/pull/488) ([hjafarpour](https://github.com/hjafarpour))
- Fix the sporadically failing CLITests [\#486](https://github.com/confluentinc/ksql/pull/486) ([apurvam](https://github.com/apurvam))
- Hide user name and password command line flags  [\#485](https://github.com/confluentinc/ksql/pull/485) ([apurvam](https://github.com/apurvam))
- Change jenkins slack channel [\#484](https://github.com/confluentinc/ksql/pull/484) ([hjafarpour](https://github.com/hjafarpour))
- Don't replay all previous events found on the command topic \#454 [\#480](https://github.com/confluentinc/ksql/pull/480) ([dguy](https://github.com/dguy))
- Added basic auth capabilities to KSQL Remote cli [\#477](https://github.com/confluentinc/ksql/pull/477) ([apurvam](https://github.com/apurvam))
- Adding metric-collection for DESCRIBE EXTEND [\#475](https://github.com/confluentinc/ksql/pull/475) ([bluemonk3y](https://github.com/bluemonk3y))
- Make key property mandatory in CREATE TABLE statement. [\#473](https://github.com/confluentinc/ksql/pull/473) ([hjafarpour](https://github.com/hjafarpour))
- DOCS-127 - Add KSQL doc to CP [\#472](https://github.com/confluentinc/ksql/pull/472) ([joel-hamill](https://github.com/joel-hamill))
- Add doc for fail.on.deserialization.error [\#462](https://github.com/confluentinc/ksql/pull/462) ([dguy](https://github.com/dguy))
- KSQL-351 - Doc error for ksql-server-start [\#440](https://github.com/confluentinc/ksql/pull/440) ([joel-hamill](https://github.com/joel-hamill))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
