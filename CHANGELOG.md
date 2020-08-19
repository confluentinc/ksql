# Change Log

## [0.11.0](https://github.com/confluentinc/ksql/releases/tag/v0.11.0-ksqldb) (2020-08-03)

### Features

* **client:** support streaming inserts in Java client ([#5641](https://github.com/confluentinc/ksql/pull/5641)) ([1e109bf](https://github.com/confluentinc/ksql/commit/1e109bf))
* **client:** support admin operations in Java client ([#5671](https://github.com/confluentinc/ksql/pull/5671)) ([7d0079a](https://github.com/confluentinc/ksql/commit/7d0079a))
* **client:** support list queries in Java client ([#5682](https://github.com/confluentinc/ksql/pull/5682)) ([4d860f8](https://github.com/confluentinc/ksql/commit/4d860f8))
* **client:** support DDL/DML statements in Java client ([#5775](https://github.com/confluentinc/ksql/pull/5775)) ([53ca76f](https://github.com/confluentinc/ksql/commit/53ca76f))
* support WINDOWEND in WHERE of pull queries ([#5680](https://github.com/confluentinc/ksql/pull/5680)) ([40f2f13](https://github.com/confluentinc/ksql/commit/40f2f13))
* Adds SSL mutual auth support to intra-cluster requests ([#5482](https://github.com/confluentinc/ksql/pull/5482)) ([82b137f](https://github.com/confluentinc/ksql/commit/82b137f))
* new array_remove UDF ([#5843](https://github.com/confluentinc/ksql/pull/5843)) ([4ebeff2](https://github.com/confluentinc/ksql/commit/4ebeff2))
* Replay command topic to local file to backup KSQL Metastore ([#5831](https://github.com/confluentinc/ksql/pull/5831)) ([8523051](https://github.com/confluentinc/ksql/commit/8523051))
* organize UDFs by category ([#5813](https://github.com/confluentinc/ksql/pull/5813)) ([7f5b843](https://github.com/confluentinc/ksql/commit/7f5b843))
* expose query ID in CommandStatusEntity (MINOR) ([#5814](https://github.com/confluentinc/ksql/pull/5814)) ([bb29185](https://github.com/confluentinc/ksql/commit/bb29185))

### Bug Fixes

* circumvent KAFKA-10179 by forcing changelog topics for tables ([#5781](https://github.com/confluentinc/ksql/pull/5781)) ([ef8fa4f](https://github.com/confluentinc/ksql/commit/ef8fa4f))
* always use the changelog subject in table state stores ([#5823](https://github.com/confluentinc/ksql/pull/5823)) ([e69acb4](https://github.com/confluentinc/ksql/commit/e69acb4))
* remove schema compat check if schema exists ([#5872](https://github.com/confluentinc/ksql/pull/5872)) ([6338270](https://github.com/confluentinc/ksql/commit/6338270))
* ensure null values cast to varchar/string remain null ([#5769](https://github.com/confluentinc/ksql/pull/5769)) ([530eb7f](https://github.com/confluentinc/ksql/commit/530eb7f))
* ksqlDB should not truncate decimals ([#5763](https://github.com/confluentinc/ksql/pull/5763)) ([ba833f7](https://github.com/confluentinc/ksql/commit/ba833f7))
* Make sure UDTF describe shows actual function description ([#5744](https://github.com/confluentinc/ksql/pull/5744)) ([afe85d9](https://github.com/confluentinc/ksql/commit/afe85d9))
* Reuse KsqlClient instance for inter node requests ([#5742](https://github.com/confluentinc/ksql/pull/5742)) ([cd7f540](https://github.com/confluentinc/ksql/commit/cd7f540))
* SEC-1034: log4j migration to confluent repackaged version ([#5783](https://github.com/confluentinc/ksql/pull/5783)) ([4563d02](https://github.com/confluentinc/ksql/commit/4563d02))
* show overridden props in CLI ([#5750](https://github.com/confluentinc/ksql/pull/5750)) ([f6fd2ee](https://github.com/confluentinc/ksql/commit/f6fd2ee))
* simplify pull query error message ([#5672](https://github.com/confluentinc/ksql/pull/5672)) ([9bc4755](https://github.com/confluentinc/ksql/commit/9bc4755))
* Upgrade to Vert.x 3.9.1 which depends on version of Netty which allows backported ALPN in JDK 1.8.0_252 to be used, and provide warning if openSSL is not installed ([#5818](https://github.com/confluentinc/ksql/pull/5818)) ([36e44a6](https://github.com/confluentinc/ksql/commit/36e44a6))
* windowed tables now have cleanup policy compact+delete ([#5743](https://github.com/confluentinc/ksql/pull/5743)) ([2038770](https://github.com/confluentinc/ksql/commit/2038770))
* configure topic retention based on retention clause for windowed tables ([#5835](https://github.com/confluentinc/ksql/pull/5835)) ([b509c99](https://github.com/confluentinc/ksql/commit/b509c99))
* set Schema Registry port in tutorials docker compose ([f46d358](https://github.com/confluentinc/ksql/commit/f46d358))
* close query on invalid use of HTTP/2 with /query endpoint ([#5883](https://github.com/confluentinc/ksql/pull/5883)) ([bcab116](https://github.com/confluentinc/ksql/commit/bcab116))
* adds a handler to gracefully shutdown ([#5895](https://github.com/confluentinc/ksql/pull/5895)) ([5fbf171](https://github.com/confluentinc/ksql/commit/5fbf171))

### BREAKING CHANGES

* ksqlDB now creates windowed tables with cleanup policy "compact,delete", rather than "compact". Also, topics that back streams are always created with cleanup policy "delete", rather than the broker default (by default, "delete").

## [0.10.1](https://github.com/confluentinc/ksql/releases/tag/v0.10.1-ksqldb) (2020-07-09)

### Bug Fixes

* allow empty structs in schema inference ([#5656](https://github.com/confluentinc/ksql/pull/5656)) ([3c38c8c](https://github.com/confluentinc/ksql/commit/3c38c8ce3771ad3418645f81ee0caf1370e784b1))
* do not overwrite schemas from a CREATE STREAM/TABLE ([#5756](https://github.com/confluentinc/ksql/pull/5756)) ([5aa0b72](https://github.com/confluentinc/ksql/commit/5aa0b7241b46e54a821eef06b3a1e64b31ccdbaa))
* make sure old query stream doesn't block on close ([#5730](https://github.com/confluentinc/ksql/pull/5730)) ([663a67b](https://github.com/confluentinc/ksql/commit/663a67b0078d8d97fecdf7cbaa5c7c5d80b434d2))
* query w/ scoped all columns, join and where clause ([#5684](https://github.com/confluentinc/ksql/pull/5684)) ([304eb0c](https://github.com/confluentinc/ksql/commit/304eb0c48530068c02839f216aca0f71fc84f7ca))
* support CASE statements returning NULL ([#5703](https://github.com/confluentinc/ksql/pull/5703)) ([5062942](https://github.com/confluentinc/ksql/commit/506294245d83dc741e8da3046a584e47c7b05a99))
* UDTF don't return null on keyless stream ([#5761](https://github.com/confluentinc/ksql/pull/5761)) ([190f9e2](https://github.com/confluentinc/ksql/commit/190f9e265a6360c6308cc4e39a378e12e9ad1cbb))

## [0.10.0](https://github.com/confluentinc/ksql/releases/tag/v0.10.0-ksqldb) (2020-06-25)

### Features

* Any key name ([#5093](https://github.com/confluentinc/ksql/pull/5093)) ([1f0ca3e](https://github.com/confluentinc/ksql/commit/1f0ca3efb0f5ecadc7a604f219cbc16b2e120d8d))
* Explicit keys ([#5533](https://github.com/confluentinc/ksql/pull/5533)) ([d0db0cf](https://github.com/confluentinc/ksql/commit/d0db0cfac050cef94019c6daa59cd765ca0f7379))
* add extra log messages for pull queries ([#4909](https://github.com/confluentinc/ksql/pull/4909)) ([d622ecc](https://github.com/confluentinc/ksql/commit/d622eccb845698c245340556b6b4965fec075305))
* Adds the ability have internal endpoints listen on ksql.internal.listener ([#5212](https://github.com/confluentinc/ksql/pull/5212)) ([46acb73](https://github.com/confluentinc/ksql/commit/46acb73f8e0df7118876e1d991fd8ba0946905f7))
* create MetricsContext for ksql metrics reporters ([#5528](https://github.com/confluentinc/ksql/pull/5528)) ([50561a5](https://github.com/confluentinc/ksql/commit/50561a55cdf7a4264481ca51de8d3db17d3aeea6))
* drop WITH(KEY) syntax ([#5363](https://github.com/confluentinc/ksql/pull/5363)) ([bb43d23](https://github.com/confluentinc/ksql/commit/bb43d23ad36ec3519a06d05c25ca99f726244ad0))
* expose JMX metric that classifies an error state ([#5374](https://github.com/confluentinc/ksql/pull/5374)) ([52271bf](https://github.com/confluentinc/ksql/commit/52271bf6ccbfdb26e83e5c30c2e11db0c72e8efc))
* Expose Vert.x metrics ([#5340](https://github.com/confluentinc/ksql/pull/5340)) ([e82f762](https://github.com/confluentinc/ksql/commit/e82f7626236cc62d98e62e1d51ff33e4d6ff1519))
* introduce RegexClassifier for classifying errors via cfg ([#5412](https://github.com/confluentinc/ksql/pull/5412)) ([b25dd98](https://github.com/confluentinc/ksql/commit/b25dd987b219dcb8a3105beb7251503828ade023))
* Pull Queries: QPS check utilizes internal API flag to determine if forwarded ([#5392](https://github.com/confluentinc/ksql/pull/5392)) ([08b428f](https://github.com/confluentinc/ksql/commit/08b428f368a6f6238f9ffd9caab9f27afad9d1b9))
* reload TLS certificate without restarting server ([#5516](https://github.com/confluentinc/ksql/pull/5516)) ([a5920b0](https://github.com/confluentinc/ksql/commit/a5920b0f31fe2f32adcfe8cd03e1f2bd14c1368e))
* support TIMESTAMP being a key column ([#5542](https://github.com/confluentinc/ksql/pull/5542)) ([286ce08](https://github.com/confluentinc/ksql/commit/286ce0850e43e06ef0c7263546d31946c3c05302))
* turn on snappy compression for produced data ([#5495](https://github.com/confluentinc/ksql/pull/5495)) ([27d8ad5](https://github.com/confluentinc/ksql/commit/27d8ad5d3d9d2f731b0281a525b5e80edb24a637))
* **client:** Java client with push + pull query support ([#5200](https://github.com/confluentinc/ksql/pull/5200)) ([280ef0c](https://github.com/confluentinc/ksql/commit/280ef0ca8aa02693db1427ece08cd7863abed98d))
* **client:** support (non-streaming) insert into in Java client ([#5448](https://github.com/confluentinc/ksql/pull/5448)) ([9e8234a](https://github.com/confluentinc/ksql/commit/9e8234ad93745f73dc8e0d7463e44f33bb0739a9))
* **client:** support push query termination in Java client ([#5371](https://github.com/confluentinc/ksql/pull/5371)) ([62dacca](https://github.com/confluentinc/ksql/commit/62dacca7efc8fc2a4202c69caa58596051c67fee))
* New UDF/UDAF
    * Adds UDF regexp_extract_all ([#5507](https://github.com/confluentinc/ksql/pull/5507)) ([e19233c](https://github.com/confluentinc/ksql/commit/e19233c6bba4e5f4be9a93c224265c949626d317))
    * Adds UDF regexp_replace ([#5504](https://github.com/confluentinc/ksql/pull/5504)) ([30309bf](https://github.com/confluentinc/ksql/commit/30309bf07fbc706fb5200996ce33318d379cd97f))
    * Adds udf regexp_split_to_array ([#5501](https://github.com/confluentinc/ksql/pull/5501)) ([3766129](https://github.com/confluentinc/ksql/commit/3766129fe4511cedc2f7102427944876fb927a8a))
    * new UDFs for array max/min/sort ([#5505](https://github.com/confluentinc/ksql/pull/5505)) ([415d930](https://github.com/confluentinc/ksql/commit/415d93049670d30d48d68b294f7eb724fb8f90c8))
    * new UDFs for set-like operations on Arrays ([#5548](https://github.com/confluentinc/ksql/pull/5548)) ([50428c7](https://github.com/confluentinc/ksql/commit/50428c74089dddd43fb96e8299a28c507c615e4a))
    * new UDFs for working with Maps ([#5536](https://github.com/confluentinc/ksql/pull/5536)) ([bc9ad2e](https://github.com/confluentinc/ksql/commit/bc9ad2ea93b08ace708d6e74684582fa3e9c2d4d))
    * new UUID UDF ([#5535](https://github.com/confluentinc/ksql/pull/5535)) ([cfa65da](https://github.com/confluentinc/ksql/commit/cfa65da59ee057840a6c394e3038eb741a161e06))
    * new split_to_map udf ([#5563](https://github.com/confluentinc/ksql/pull/5563)) ([a68b9ad](https://github.com/confluentinc/ksql/commit/a68b9add94ddb9f97e1cff51fb57dddd1c9458eb))
    * new string UDFs LPad, RPad ([#5546](https://github.com/confluentinc/ksql/pull/5546)) ([00f5083](https://github.com/confluentinc/ksql/commit/00f5083cf3320ddb773d25cc160fffb414173ddc))
    * implement earliest_by_offset() UDAF ([#5273](https://github.com/confluentinc/ksql/pull/5273)) ([2a356ac](https://github.com/confluentinc/ksql/commit/2a356acbc1d55c868f0cde95b72835476615f8d6))
    * INSTR function [#881](https://github.com/confluentinc/ksql/issues/881) ([#5385](https://github.com/confluentinc/ksql/pull/5385)) ([ca86bbf](https://github.com/confluentinc/ksql/commit/ca86bbfad51e479d6847f84a0c7928f31d8a2b26))
    * add CHR UDF ([#5559](https://github.com/confluentinc/ksql/pull/5559)) ([5a746e8](https://github.com/confluentinc/ksql/commit/5a746e85502235753cf11bfa507ab69e1e19064c))
    * implements ARRAY_JOIN as requested in ([#5028](https://github.com/confluentinc/ksql/pull/5028)) ([#5474](https://github.com/confluentinc/ksql/issues/5474)) ([#5638](https://github.com/confluentinc/ksql/issues/5638)) ([6c67866](https://github.com/confluentinc/ksql/commit/6c678665cee4a67e7737460710eac2875bc8c2e2))
    * Add encode udf ([#5523](https://github.com/confluentinc/ksql/pull/5523)) ([b02f1ce](https://github.com/confluentinc/ksql/commit/b02f1ce8ae564688eda385268a51b1217dc9e277))


### Bug Fixes

* /inserts-stream endpoint now accepts complex types ([#5469](https://github.com/confluentinc/ksql/pull/5469)) ([0840160](https://github.com/confluentinc/ksql/commit/0840160e81b52b7cd0d55c6af7581c6be0a9eb85))
* allow dynamic construction of an ARRAY of STRUCTS with duplicate values [#5436](https://github.com/confluentinc/ksql/issues/5436) ([#5506](https://github.com/confluentinc/ksql/pull/5506)) ([0b1162c](https://github.com/confluentinc/ksql/commit/0b1162cc94bef391531bdc33b726aede4b3aab04))
* allow setting auto.offset.reset=latest on /query-stream endpoint ([#5455](https://github.com/confluentinc/ksql/pull/5455)) ([d91c016](https://github.com/confluentinc/ksql/commit/d91c01625acab2e6c3ce56339ec76d9f5b7a81aa))
* allow structs in schema provider return types ([#5287](https://github.com/confluentinc/ksql/pull/5287)) ([2e604f0](https://github.com/confluentinc/ksql/commit/2e604f04daf56e720927c9f66219853e0d4fabef))
* Allow value delimiter to be specified for datagen ([#5332](https://github.com/confluentinc/ksql/pull/5332)) ([865e834](https://github.com/confluentinc/ksql/commit/865e834f9c075705cbe43515925943149e638984))
* avoid unnecessary warnings from PRINT ([#5459](https://github.com/confluentinc/ksql/pull/5459)) ([080fba9](https://github.com/confluentinc/ksql/commit/080fba94770bfccaa78db2e26e6ed6424b93ee2f))
* Block writer thread if response output buffer is full ([#5386](https://github.com/confluentinc/ksql/pull/5386)) ([0edda40](https://github.com/confluentinc/ksql/commit/0edda40c04f25d26a60cb135a3827198f4d5c684))
* deals with issue [#5521](https://github.com/confluentinc/ksql/issues/5521) by adding more descriptive error message ([#5529](https://github.com/confluentinc/ksql/pull/5529)) ([86f8b67](https://github.com/confluentinc/ksql/commit/86f8b67ba6855dbdb8f77c15657aeab567b15bb7))
* disallow requests to /inserts-stream if insert values disabled ([#5592](https://github.com/confluentinc/ksql/pull/5592)) ([e277f25](https://github.com/confluentinc/ksql/commit/e277f2541f35f329339f1d617c7ca60d632a1cba))
* exclude window bounds from persistent query value & schema match ([#5425](https://github.com/confluentinc/ksql/pull/5425)) ([3596ceb](https://github.com/confluentinc/ksql/commit/3596ceb09965faf09e2a9e0e2460126b8c9ebf0b))
* fail AVRO/Protobuf/JSON Schema statements if SR is missing ([#5597](https://github.com/confluentinc/ksql/pull/5597)) ([85a0320](https://github.com/confluentinc/ksql/commit/85a0320e774d236bda4898f82eb5d5c2f7b42cec))
* fail on WINDOW clause without matching GROUP BY ([#5431](https://github.com/confluentinc/ksql/pull/5431)) ([68354d4](https://github.com/confluentinc/ksql/commit/68354d488aec07dc0bc9a7354ee8b8794bfef4e6))
* improve print topic ([#5552](https://github.com/confluentinc/ksql/pull/5552)) ([e193576](https://github.com/confluentinc/ksql/commit/e193576c0f6c0295da6f4357391639e49a45863b))
* Improve pull query error logging ([#5477](https://github.com/confluentinc/ksql/pull/5477)) ([f23412e](https://github.com/confluentinc/ksql/commit/f23412e9103531d2b8ce96cfcee3eff92784239b))
* KSQL does not accept more queries when running QueryLimit - 1 queries ([#5461](https://github.com/confluentinc/ksql/pull/5461)) ([d64f1bc](https://github.com/confluentinc/ksql/commit/d64f1bc7ba4359b3d7a8cf3ba28d315eef47389e))
* make stream and column names case-insensitive in /inserts-stream ([#5591](https://github.com/confluentinc/ksql/pull/5591)) ([e9e3042](https://github.com/confluentinc/ksql/commit/e9e3042f1bdbd6da384146410f866aab6b12b987))
* NPE in latest_by_offset if first element being processed has null… ([#4975](https://github.com/confluentinc/ksql/pull/4975)) ([a9668d2](https://github.com/confluentinc/ksql/commit/a9668d245db797e193d1e4bcc8734eab29ba99b3))
* Prevent memory leaks caused by pull query logging ([#5532](https://github.com/confluentinc/ksql/pull/5532)) ([723b6cb](https://github.com/confluentinc/ksql/commit/723b6cb764950a4d9d639f5d40c4b4e6c615fba6))
* remove leading zeros when casting decimal to string ([#5270](https://github.com/confluentinc/ksql/pull/5270)) ([e1cc8ad](https://github.com/confluentinc/ksql/commit/e1cc8ad8db9f9f34c3a2f26b60c3d0a7631b23a2))
* Remove stacktrace from error message ([#5478](https://github.com/confluentinc/ksql/pull/5478)) ([b63d7e8](https://github.com/confluentinc/ksql/commit/b63d7e8d3fb325a74120e40e3fc96d51e9c35856))
* Retry on connection closed ([#5515](https://github.com/confluentinc/ksql/pull/5515)) ([8eb1f88](https://github.com/confluentinc/ksql/commit/8eb1f88c165aa0bb494288c46cd96ac911d7cfdf))
* set retention.ms to -1 instead of Long.MAX_VALUE ([#5560](https://github.com/confluentinc/ksql/pull/5560)) ([22da8a0](https://github.com/confluentinc/ksql/commit/22da8a091f0020bbd541ad5f812054c7744f0785))
* update Concat UDF to new framework and make variadic ([#5513](https://github.com/confluentinc/ksql/pull/5513)) ([cab6a86](https://github.com/confluentinc/ksql/commit/cab6a8672f97d86da75a3e5b1e70985146129640))
* zero decimal bug ([#5531](https://github.com/confluentinc/ksql/pull/5531)) ([1b9094b](https://github.com/confluentinc/ksql/commit/1b9094bd5ead8a8441ef8f10a5f86722dd7d3988))
* /query-stream endpoint should serialize Struct (MINOR) ([#5205](https://github.com/confluentinc/ksql/pull/5205)) ([12b092b](https://github.com/confluentinc/ksql/commit/12b092bda160f614ef0b8096f77b7013efc3b190))
* Move Cors handler in front of /chc handlers ([#5239](https://github.com/confluentinc/ksql/pull/5239)) ([004ced2](https://github.com/confluentinc/ksql/commit/004ced255479f6a5975848270746a45746a4437a))
* use schema in annotation as schema provider if present ([1a90eeb](https://github.com/confluentinc/ksql/commit/1a90eeb71ae8292410fc99d49c24fdafadcf9de7))
* use sr's jackson-jsonschema version ([#5213](https://github.com/confluentinc/ksql/pull/5213)) ([0b3899a](https://github.com/confluentinc/ksql/commit/0b3899a4a31f668a897d82d98c59365aae8022ec))
* /inserts-stream endpoint now supports nested types ([#5621](https://github.com/confluentinc/ksql/pull/5621)) ([866ae34](https://github.com/confluentinc/ksql/commit/866ae3499a9d59068461d17ed15e1353759a0334))
* don't fail if broker does not support AuthorizedOperations ([#5617](https://github.com/confluentinc/ksql/pull/5617)) ([0feb081](https://github.com/confluentinc/ksql/commit/0feb081c89d00f326e151035ac5616d19f610307))
* ensure only deserializable cmds are written to command topic ([#5645](https://github.com/confluentinc/ksql/pull/5645)) ([4ad2bde](https://github.com/confluentinc/ksql/commit/4ad2bde44f31ea3c15fdd898ce12d50a595013d1))
* support GROUP BY with no source columns used ([#5644](https://github.com/confluentinc/ksql/pull/5644)) ([a8e6630](https://github.com/confluentinc/ksql/commit/a8e66304081879335c62948d5a8dd5f5531766be))


### BREAKING CHANGES

An associated [blog post](An associated blog post also covers many of these breaking changes: https://www.confluent.io/blog/ksqldb-0-10-updates-key-columns/) also covers many of these breaking changes.

#### Any key name

Statements containing PARTITION BY, GROUP BY, or JOIN clauses now produce different output schemas.

For PARTITION BY and GROUP BY statements, the name of the key column in the result is determined by the PARTITION BY or GROUP BY clause:
1. Where the partitioning or grouping is a single column reference, then the key column has the same name as this column. For example:

```sql
-- OUTPUT will have a key column called X;
CREATE STREAM OUTPUT AS
  SELECT * 
  FROM INPUT 
  GROUP BY X;
```
2. Where the partitioning or grouping is a single struct field, then the key column has the same name as the field. For example:

```sql
-- OUTPUT will have a key column called FIELD1;
CREATE STREAM OUTPUT AS
  SELECT * 
  FROM INPUT 
  GROUP BY X->field1;
```
3. Otherwise, the key column name is system-generated and has the form `KSQL_COL_n`, where `n` is a positive integer.

In all cases, except where grouping by more than one column, you can set the new key column's name by defining an alias in the projection. For example:

```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    USERID AS ID, 
    COUNT(*) 
  FROM USERS 
  GROUP BY ID;
```
For groupings of multiple expressions, you can't provide a name for the system-generated key column.
However, a work around is to combine the grouping columns yourself, which does enable you to provide an alias:

```sql
-- products_by_sub_cat will have a key column named COMPOSITEKEY:
CREATE TABLE products_by_sub_cat AS
  SELECT 
    categoryId + ‘§’ + subCategoryId AS compositeKey
    SUM(quantity) as totalQty  
  FROM purchases
  GROUP BY CAST(categoryId AS STRING) + ‘§’ + CAST(subCategoryId AS STRING);

```

For JOIN statements, the name of the key column in the result is determined by the join criteria.
1. For INNER and LEFT OUTER joins where the join criteria contain at least one column reference, the key column is named based on the left-most source whose join criteria is a column reference. For example:

```sql
-- OUTPUT will have a key column named I2_ID.
CREATE TABLE OUTPUT AS
  SELECT * 
  FROM I1 
    JOIN I2 ON abs(I1.ID) = I2.ID JOIN I3 ON I2.ID = I3.ID;
```
The key column can be given a new name, if required, by defining an alias in the projection. For example:
```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    I2.ID AS ID, 
    I1.V0, 
    I2.V0, 
    I3.V0 
  FROM I1 
    JOIN I2 ON abs(I1.ID) = I2.ID 
    JOIN I3 ON I2.ID = I3.ID;
```
2. For FULL OUTER joins and other joins where the join criteria are not on column references, the key column in the output is not equivalent to any column from any source. The key column has a system-generated name in the form `KSQL_COL_n`, where `n` is a positive integer. For example:

```sql
-- OUTPUT will have a key column named KSQL_COL_0, or similar.
CREATE TABLE OUTPUT AS
  SELECT * 
  FROM I1 
    FULL OUTER JOIN I2 ON I1.ID = I2.ID;
```
The key column can be given a new name, if required, by defining an alias in the projection. A new UDF has been introduced to help define the alias called `JOINKEY`. It takes the join criteria as its parameters. For example:
```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    JOINKEY(I1.ID, I2.ID) AS ID, 
    I1.V0, 
    I2.V0 
  FROM I1 
    FULL OUTER JOIN I2 ON I1.ID = I2.ID;
```
`JOINKEY` will be deprecated in a future release of ksqlDB once multiple key columns are supported.

#### Explicit keys

`CREATE TABLE` statements will now fail if the `PRIMARY KEY` column is not provided.

For example, a statement such as:

```sql
CREATE TABLE FOO (
    name STRING
  ) WITH (
    kafka_topic='foo', 
    value_format='json'
  );
```

Will need to be updated to include the definition of the PRIMARY KEY, for example:

```sql
CREATE TABLE FOO (
    ID STRING PRIMARY KEY, 
    name STRING
  ) WITH (
    kafka_topic='foo', 
    value_format='json'
  );
```

If using schema inference, i.e. loading the value columns of the topic from the Schema Registry, the primary key can be provided as a partial schema, for example:

```sql
-- FOO will have value columns loaded from the Schema Registry
CREATE TABLE FOO (
    ID INT PRIMARY KEY
  ) WITH (
    kafka_topic='foo', 
    value_format='avro'
  );
```

`CREATE STREAM` statements that do not define a `KEY` column no longer have an implicit `ROWKEY` key column.

For example:

```sql
CREATE STREAM BAR (
    NAME STRING
  ) WITH (...);
```

Previously, the above statement would have resulted in a stream with two columns: `ROWKEY STRING KEY` and `NAME STRING`.

With this change, the above statement results in a stream with only the `NAME STRING` column.

Streams with no KEY column are serialized to Kafka topics with a `null` key.

#### Key columns required in projection

A statement that creates a materialized view must include the key columns in the projection. For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId,  // <-- key column in projection
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

The key column `productId` is required in the projection. In previous versions of ksqlDB, the presence

of `productId` in the projection would have placed a _copy_ of the data into the value of the underlying 
Kafka topic's record.  But starting in version 0.10.0, the projection must include the key columns, and ksqlDB stores these columns

in the _key_ of the underlying Kafka record.  Optionally, you may provide an alias for 

the key column(s). 

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId as id,  // <-- aliased key column
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

If you need a copy of the key column in the Kafka record's value, use the 

[AS_VALUE](docs/developer-guide/ksqldb-reference/scalar-functions#as_value) function to indicate this
to ksqlDB. For example, the following statement produces an output inline with the previous version of ksqlDB

for the above example materialized view:

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId as ROWKEY,              // <-- key column named ROWKEY
      AS_VALUE(productId) as productId, // <-- productId copied into value
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

### WITH(KEY) syntax removed

In previous versions, all key columns were called `ROWKEY`. To enable using a more

user-friendly name for the key column in queries, it was possible

to supply an alias for the key column in the WITH clause, for example:

```sql
CREATE TABLE INPUT (
    ROWKEY INT PRIMARY KEY, 
    ID INT, 
    V0 STRING
  ) WITH (
    key='ID', 
    ...
  );
```

With the previous query, the `ID` column can be used as an alias for `ROWKEY`.
This approach required the Kafka message value to contain an exact copy of the key.

[KLIP-24](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-24-key-column-semantics-in-queries.md)
removed the restriction that key columns must be named `ROWKEY`, negating the need for the `WITH(KEY)`
syntax, which has been removed. Also, this change removed the requirement for
the Kafka message value to contain an exact copy of the key.

Update your queries by removing the `KEY` from the `WITH` clause and naming

your `KEY` and `PRIMARY KEY` columns appropriately. For example, the previous
CREATE TABLE statement can now be rewritten as:

```sql
CREATE TABLE INPUT (
    ID INT PRIMARY KEY, 
    V0 STRING
  ) WITH (...);
```

Unless the value format is `DELIMITED`, which means the value columns are
*order dependent*, so dropping the `ID` value column would result in a

deserialization error or the wrong values being loaded. If you're using
`DELIMITED`, consider rewriting as:

```sql
CREATE TABLE INPUT (
    ID INT PRIMARY KEY, 
    ignoreMe INT, 
    V0 STRING
  ) WITH (...);
```

## [0.9.0](https://github.com/confluentinc/ksql/releases/tag/v0.9.0-ksqldb) (2020-05-11)

### Features

* add multi-join expression support ([#5081](https://github.com/confluentinc/ksql/pull/5081)) ([002cd5a](https://github.com/confluentinc/ksql/commit/002cd5ad6473cbb000291e32ebe5a459d2870b61))
* support more advanced suite of LIKE expressions ([#5013](https://github.com/confluentinc/ksql/pull/5013)) ([67cd9d9](https://github.com/confluentinc/ksql/commit/67cd9d91690039cc01f43438db1fbcd3fe4924ff))
* add COALESCE function ([#4829](https://github.com/confluentinc/ksql/pull/4829)) ([251c237](https://github.com/confluentinc/ksql/commit/251c237c455f4cf3843628f41e4bbf57795cfd12))
* add GROUP BY support for any key names ([#4899](https://github.com/confluentinc/ksql/pull/4899)) ([e7cbdfc](https://github.com/confluentinc/ksql/commit/e7cbdfcc8c9853e2ae6dfcaf670e04f07ccd5444)), closes [#4898](https://github.com/confluentinc/ksql/issues/4898)
* partition-by primitive key support (#4098) ([7addf88](https://github.com/confluentinc/ksql/commit/7addf8856a6d62a6890a5f2520eead26538233a6)), closes [#4098](https://github.com/confluentinc/ksql/issues/4098)
* add KsqlQueryStatus to decouple from KafkaStreams.State ([#5029](https://github.com/confluentinc/ksql/pull/5029)) ([e8cbcde](https://github.com/confluentinc/ksql/commit/e8cbcde548e1f3078b1396173a3f55f86ee20626))
* Adds rate limiting to pull queries ([#4951](https://github.com/confluentinc/ksql/pull/4951)) ([6284111](https://github.com/confluentinc/ksql/commit/6284111652252b63cfc233bdfc3e08dbff983bbd))
* Do not allow access to new streaming endpoints using HTTP1.x ([#5193](https://github.com/confluentinc/ksql/pull/5193)) ([8b90035](https://github.com/confluentinc/ksql/commit/8b90035c6facf47d81dfd1616784b191141dce31))
* fail startup if command contains incompatible version ([#5104](https://github.com/confluentinc/ksql/pull/5104)) ([a1751b1](https://github.com/confluentinc/ksql/commit/a1751b1689532ef9abd436d5f27fe9aa8ff555ac))
* klip-14 - rowtime as pseduocolumn ([#5150](https://github.com/confluentinc/ksql/pull/5150)) ([d541420](https://github.com/confluentinc/ksql/commit/d541420acf61f0e0e7b25d91120886007dbdae01))
* 'SHOW QUERIES [EXTENDED]' statement returns results for all nodes in the cluster ([#4875](https://github.com/confluentinc/ksql/pull/4875)) ([7385a31](https://github.com/confluentinc/ksql/commit/7385a31dd33293b76e594649213da2abb81b8ddf))
* transient queries added to show queries output ([#5105](https://github.com/confluentinc/ksql/pull/5105)) ([e8a2a63](https://github.com/confluentinc/ksql/commit/e8a2a63210219cbac7d979ec0915f1d2016398e2))

### Bug Fixes

* 'drop (stream|table) if exists' should not fail if source does not exist ([#4872](https://github.com/confluentinc/ksql/pull/4872)) ([b0669a0](https://github.com/confluentinc/ksql/commit/b0669a044b53407fba3ceb11bddee73e69c0006f))
* add deserializer for SqlType ([#4830](https://github.com/confluentinc/ksql/pull/4830)) ([eed9912](https://github.com/confluentinc/ksql/commit/eed99123a7f673eec9c1f5224c164a5737e6dc41))
* allow trailing slash in listeners config (MINOR) ([#5012](https://github.com/confluentinc/ksql/pull/5012)) ([13b0455](https://github.com/confluentinc/ksql/commit/13b04557d01aed36dcf11e8d6c8300dae4143615))
* Allows unclosed quote characters to exist in comments ([#4993](https://github.com/confluentinc/ksql/pull/4993)) ([fd65021](https://github.com/confluentinc/ksql/commit/fd6502115170c97f6a2c61cabf513145ea9c4cdc))
* avoid duplicate column name errors from auto-generated aliases ([#4827](https://github.com/confluentinc/ksql/pull/4827)) ([258d0b0](https://github.com/confluentinc/ksql/commit/258d0b06d81954af2c4e2c17fd7f26c74f315368))
* avoid long possible format lists on PRINT TOPIC for nulls ([#4867](https://github.com/confluentinc/ksql/pull/4867)) ([a66e489](https://github.com/confluentinc/ksql/commit/a66e489d176e1ed6ea715af3d9cfd75a351e0447))
* better error code on shutdown ([#4754](https://github.com/confluentinc/ksql/pull/4754)) ([17d758d](https://github.com/confluentinc/ksql/commit/17d758d0137102aa30493eee458e1a7c2ea54f82))
* Catch server startup exceptions ([#4974](https://github.com/confluentinc/ksql/pull/4974)) ([898f3a1](https://github.com/confluentinc/ksql/commit/898f3a104f88fba5cf79e02f12ef43760065eaae))
* CommandRunner metric has correct metric displayed when thread dies ([#4653](https://github.com/confluentinc/ksql/pull/4653)) ([1db542b](https://github.com/confluentinc/ksql/commit/1db542bfea5006fc9b3af59bca28d4af48fc5d57))
* do not allow GROUP BY and PARTITION BY on boolean expressions ([#4940](https://github.com/confluentinc/ksql/pull/4940)) ([d84d2d1](https://github.com/confluentinc/ksql/commit/d84d2d110d5ac502de4ba16602aa32babe24b4ca))
* do not allow grouping sets ([#4942](https://github.com/confluentinc/ksql/pull/4942)) ([51bb9f6](https://github.com/confluentinc/ksql/commit/51bb9f6f4bcef2d75f6d6d96c4c883559a21999b))
* do not allow implicit casting in UDAF function lookup ([#5145](https://github.com/confluentinc/ksql/pull/5145)) ([6709010](https://github.com/confluentinc/ksql/commit/6709010b06fed487334338e5e102bf53d51b4c46))
* do not filter out rows where PARTITION BY resolves to null ([#4823](https://github.com/confluentinc/ksql/pull/4823)) ([e75a792](https://github.com/confluentinc/ksql/commit/e75a79286927a6e43b7d580b32215fb18b00d707))
* Filter out hosts with no lag info by default ([#4859](https://github.com/confluentinc/ksql/pull/4859)) ([e10bbce](https://github.com/confluentinc/ksql/commit/e10bbcede42f209765e96b2b2bb392f4d83b2560))
* fix repartition semantics ([#4816](https://github.com/confluentinc/ksql/pull/4816)) ([609e9e2](https://github.com/confluentinc/ksql/commit/609e9e28ca5cbbc9302e66f0fc54a983afd256de))
* generated aliases for struct field access no longer require quoting ([#4977](https://github.com/confluentinc/ksql/pull/4977)) ([2002458](https://github.com/confluentinc/ksql/commit/2002458f6b668b7b17b05b192a7cc3fab0a23b20))
* handle leap days correctly during timestamp extraction ([#4878](https://github.com/confluentinc/ksql/pull/4878)) ([c54db81](https://github.com/confluentinc/ksql/commit/c54db81708c56a73c082df642f8ab94d8e276142)), closes [#4864](https://github.com/confluentinc/ksql/issues/4864)
* If startup hangs (esp on preconditions), shutdown server correctly ([#4889](https://github.com/confluentinc/ksql/pull/4889)) ([20c4b59](https://github.com/confluentinc/ksql/commit/20c4b5903c13fb8435aa94e486b637074b5bb769))
* Improve error message for where/having type errors ([#5023](https://github.com/confluentinc/ksql/pull/5023)) ([23eb80d](https://github.com/confluentinc/ksql/commit/23eb80d06ea0275b28edac9b0fb5453ea4a966f8))
* improve handling of NULLs ([#5019](https://github.com/confluentinc/ksql/pull/5019)) ([c53dd68](https://github.com/confluentinc/ksql/commit/c53dd68bd912d79ced54403650ba13597ffe35a0)), closes [#4912](https://github.com/confluentinc/ksql/issues/4912)
* include lower-case identifiers among those that need quotes ([#3723](https://github.com/confluentinc/ksql/pull/3723)) ([#5139](https://github.com/confluentinc/ksql/issues/5139)) ([3bcbcf4](https://github.com/confluentinc/ksql/commit/3bcbcf45e1fc03245a57005d318133c568fb12c6))
* make endpoints available while waiting for precondition ([#5069](https://github.com/confluentinc/ksql/pull/5069)) ([1136162](https://github.com/confluentinc/ksql/commit/113616260cc0db89124330ed5e935e4b9e5af281))
* Make sure internal client is configured for TLS ([#5059](https://github.com/confluentinc/ksql/pull/5059)) ([37c7713](https://github.com/confluentinc/ksql/commit/37c7713920c7654c9b5c2f81ed72225233e1176c))
* NPE in latest_by_offset if first two elements processed are both null ([#4975](https://github.com/confluentinc/ksql/pull/4975)) ([4c72f93](https://github.com/confluentinc/ksql/commit/4c72f93813af19f6fd074b3a691018434731d545))
* only create processing log stream if it doesn't exist ([#4805](https://github.com/confluentinc/ksql/pull/4805)) ([8dead0f](https://github.com/confluentinc/ksql/commit/8dead0f4ea82de1efa34614a715b267f0246f224))
* output valid multiline queries when running SHOW QUERIES ([#4956](https://github.com/confluentinc/ksql/pull/4956)) ([ec74a33](https://github.com/confluentinc/ksql/commit/ec74a337c93e834278938f9fb692aab143d06276))
* query id for TERMINATE should be case insensitive ([#5005](https://github.com/confluentinc/ksql/pull/5005)) ([588c1e9](https://github.com/confluentinc/ksql/commit/588c1e905fc92c40635ca3271089dfecdfd4f3f6))
* reject requests to new API server if server is not ready ([#5048](https://github.com/confluentinc/ksql/pull/5048)) ([d988722](https://github.com/confluentinc/ksql/commit/d9887220bb5a688b6e8682227189341e88a61967))
* Remove unnecessary error logging for heartbeat ([#4809](https://github.com/confluentinc/ksql/pull/4809)) ([fa84576](https://github.com/confluentinc/ksql/commit/fa845763d8c52c54a65a7618a044300e8f0043f6))
* replace 'null' in explain plan with correct op type ([#5075](https://github.com/confluentinc/ksql/pull/5075)) ([f9bc0e6](https://github.com/confluentinc/ksql/commit/f9bc0e68e2b50201085352b27c56cc120eb72225))
* Returns empty lag info for a dead host rather than last received lags ([#4837](https://github.com/confluentinc/ksql/pull/4837)) ([3d98527](https://github.com/confluentinc/ksql/commit/3d985274bb4658b168758eb6c20cba38aeb1ab51))
* Stop PARTITION BY and UDTFs that fail from terminating the query ([#4822](https://github.com/confluentinc/ksql/pull/4822)) ([522fe84](https://github.com/confluentinc/ksql/commit/522fe84f407b56dbc82309db4a6502d0c9b8fc38))
* support quotes in explain statements (MINOR) ([#5142](https://github.com/confluentinc/ksql/pull/5142)) ([bee2fe0](https://github.com/confluentinc/ksql/commit/bee2fe0f523ee7e763e223bd2d1f4fc6f178ffec))
* throw error when column does not exist during INSERT INTO ([#4926](https://github.com/confluentinc/ksql/pull/4926)) ([89e261d](https://github.com/confluentinc/ksql/commit/89e261da5aff6b62901bc7d73c12baecee7bf95c))
* remove immutable properties from headless mode ([#4936](https://github.com/confluentinc/ksql/pull/4936)) ([5550880](https://github.com/confluentinc/ksql/commit/5550880de097255adbe536907d0656ba8deea0cc))
* rename ksqldb in normal logging path (MINOR) ([#4915](https://github.com/confluentinc/ksql/pull/4915)) ([16172ba](https://github.com/confluentinc/ksql/commit/16172ba8c820bb50c838ea820873b3593e062c6c))
* support trailing slashes in listener URLs ([#5076](https://github.com/confluentinc/ksql/pull/5076)) ([e9e0431](https://github.com/confluentinc/ksql/commit/e9e0431f6eafd7ef18dd2cb9e17730ec3ba1e226))
* logic when closing ksqlEngine fixed ([#4917](https://github.com/confluentinc/ksql/pull/4917)) ([a217eb9](https://github.com/confluentinc/ksql/commit/a217eb9a8a80b38c970021faad76a4b9fcade609))
* use describeTopics() for Kafka healtcheck probe ([#4814](https://github.com/confluentinc/ksql/pull/4814)) ([578d0d5](https://github.com/confluentinc/ksql/commit/578d0d543bce685ca6e253a8c6f9bcfbf191093d))
* Do not allow access to new streaming endpoints using HTTP1.x ([#5193](https://github.com/confluentinc/ksql/pull/5193)) ([8b90035](https://github.com/confluentinc/ksql/commit/8b90035c6facf47d81dfd1616784b191141dce31))
* speed up restarts by not building topologies for terminated queries ([#5002](https://github.com/confluentinc/ksql/pull/5002)) ([2472382](https://github.com/confluentinc/ksql/commit/24723824635e7ca175bc5be1fbf6a415010b00bb))

### BREAKING CHANGES

* Select star, i.e. `select *`, no longer expands to include `ROWTIME` column(s). Instead, `ROWTIME` is only included in the results of queries if explicitly included in the projection, e.g. `select rowtime, *`.
This only affects new statements. Any view previously created via a `CREATE STREAM AS SELECT` or `CREATE TABLE AS SELECT` statement is unaffected.
* This release changes the system generated column name for any columns in projections that are struct field dereferences. Previously, the full path was used when generating the name, now only the final field name is used. For example, `SELECT someStruct->someField, ...` previously generated a column name of `SOMESTRUCT__SOMEFIELD` and now generates a name of `SOMEFIELD`. Generated column names may have a numeral appended to the end to ensure uniqueness, for example `SOMEFIELD_2`.

    Note: it is recommended that you do not rely on system generated column names for production systems, because naming logic may change between releases. Providing an explicit alias ensures consistent naming across releases, for example, `SELECT someStruct->someField AS someField`.
    Backward compatibility: existing running queries will not be affected by this change, and they will continue to run with the same column names. Any statements executed after the upgrade will use the new names where no explicit alias is provided. Add explicit aliases to your statements if you require the old names, for example: `SELECT someStruct->someField AS SOMESTRUCT__SOMEFIELD, ...`

* Existing queries that reference a single GROUP BY column in the projection would fail if they were resubmitted, due to a duplicate column. The same existing queries will continue to run if already running, i.e. this is only a change for newly submitted queries. Existing queries will use the old query semantics.
* Push queries, which rely on auto-generated column names, may see changes in column names. Pull queries and any existing persistent queries are unaffected, e.g. those created with `CREATE STREAM AS SELECT`, `CREATE TABLE AS SELECT` or `INSERT INTO`.
* The ksqlDB server no longer ships with Jetty. This means that when you start the server, you must supply Jetty-specific dependencies, like certain login modules used for basic authentication, by using the KSQL_CLASSPATH environment variable for them to be found.

    See [how to configure ksqlDB for basic HTTP authentication](https://github.com/confluentinc/ksql/blob/0.9.0-ksqldb/docs/operate-and-deploy/installation/server-config/security.md#configure-ksqldb-for-basic-http-authentication)
	
## Upgrading

If you're upgrading from a ksqlDB version before 0.7, follow these [upgrade instructions](https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/#upgrade-notes).

## [0.8.1](https://github.com/confluentinc/ksql/releases/tag/v0.8.1-ksqldb) (2020-03-30)

### Bug Fixes

* Don't wait for streams thread to be in running state ([#4908](https://github.com/confluentinc/ksql/pull/4908)) ([2f83119](https://github.com/confluentinc/ksql/commit/2f83119))
* Infer TLS based on scheme of server string ([#4893](https://github.com/confluentinc/ksql/pull/4893)) ([a519ed3](https://github.com/confluentinc/ksql/commit/a519ed3))

## [0.8.0](https://github.com/confluentinc/ksql/releases/tag/v0.8.0-ksqldb) (2020-03-18)

### Features

* support Protobuf in ksqlDB ([#4469](https://github.com/confluentinc/ksql/pull/4469)) ([a77cebe](https://github.com/confluentinc/ksql/commit/a77cebe))
* introduce JSON_SR format ([#4596](https://github.com/confluentinc/ksql/pull/4596)) ([daa04d2](https://github.com/confluentinc/ksql/commit/daa04d2))
* support for tunable retention, grace period for windowed tables ([#4733](https://github.com/confluentinc/ksql/pull/4733)) ([30d49b3](https://github.com/confluentinc/ksql/commit/30d49b3)), closes [#4157](https://github.com/confluentinc/ksql/issues/4157)
* add REGEXP_EXTRACT UDF ([#4728](https://github.com/confluentinc/ksql/pull/4728)) ([a25f0fb](https://github.com/confluentinc/ksql/commit/a25f0fb))
* add ARRAY_LENGTH UDF ([#4725](https://github.com/confluentinc/ksql/pull/4725)) ([31a9d9d](https://github.com/confluentinc/ksql/commit/31a9d9d))
* Implement latest_by_offset() UDAF ([#4782](https://github.com/confluentinc/ksql/pull/4782)) ([0c13bb0](https://github.com/confluentinc/ksql/commit/0c13bb0))
* add confluent-hub to ksqlDB docker image ([#4729](https://github.com/confluentinc/ksql/pull/4729)) ([b74867a](https://github.com/confluentinc/ksql/commit/b74867a))
* add the topic name to deserialization errors ([#4573](https://github.com/confluentinc/ksql/pull/4573)) ([0f7edf6](https://github.com/confluentinc/ksql/commit/0f7edf6))
* Add metrics for pull queries endpoint ([#4608](https://github.com/confluentinc/ksql/pull/4608)) ([23e3868](https://github.com/confluentinc/ksql/commit/23e3868))
* log groupby errors to processing logger ([#4575](https://github.com/confluentinc/ksql/pull/4575)) ([b503d25](https://github.com/confluentinc/ksql/commit/b503d25))
* Provide upper limit on number of push queries ([#4581](https://github.com/confluentinc/ksql/pull/4581)) ([2cd66c7](https://github.com/confluentinc/ksql/commit/2cd66c7))
* display errors in CLI in red text ([#4509](https://github.com/confluentinc/ksql/pull/4509)) ([56f9c9b](https://github.com/confluentinc/ksql/commit/56f9c9b))
* enhance `PRINT TOPIC`'s format detection ([#4551](https://github.com/confluentinc/ksql/pull/4551)) ([8b19bc6](https://github.com/confluentinc/ksql/commit/8b19bc6))



### Bug Fixes

* change default exception handling for timestamp extractors ([#4632](https://github.com/confluentinc/ksql/pull/4632)) ([1576af0](https://github.com/confluentinc/ksql/commit/1576af0))
* create schemas at topic creation ([#4717](https://github.com/confluentinc/ksql/pull/4717)) ([514025d](https://github.com/confluentinc/ksql/commit/514025d))
* don't display decimals in scientific notation in CLI ([#4723](https://github.com/confluentinc/ksql/pull/4723)) ([3626f42](https://github.com/confluentinc/ksql/commit/3626f42))
* stop logging about command topic creation on startup if exists (MINOR) ([#4709](https://github.com/confluentinc/ksql/pull/4709)) ([f4cec0a](https://github.com/confluentinc/ksql/commit/f4cec0a))
* added special handling for forwarded pull query request  ([#4597](https://github.com/confluentinc/ksql/pull/4597)) ([ba4fe74](https://github.com/confluentinc/ksql/commit/ba4fe74))
* backport fixes from query close ([#4662](https://github.com/confluentinc/ksql/pull/4662)) ([8168002](https://github.com/confluentinc/ksql/commit/8168002))
* change configOverrides back to streamsProperties ([#4675](https://github.com/confluentinc/ksql/pull/4675)) ([ce74cf8](https://github.com/confluentinc/ksql/commit/ce74cf8))
* csas/ctas with timestamp column is used for output rowtime ([#4489](https://github.com/confluentinc/ksql/pull/4489)) ([ddddf92](https://github.com/confluentinc/ksql/commit/ddddf92))
* patch KafkaStreamsInternalTopicsAccessor as KS internals changed ([#4621](https://github.com/confluentinc/ksql/pull/4621)) ([eb07370](https://github.com/confluentinc/ksql/commit/eb07370))
* use HTTPS instead of HTTP to resolve dependencies in Maven archetype ([#4511](https://github.com/confluentinc/ksql/pull/4511)) ([f21823f](https://github.com/confluentinc/ksql/commit/f21823f))
* add deserializer for SqlType ([#4830](https://github.com/confluentinc/ksql/pull/4830)) ([eed9912](https://github.com/confluentinc/ksql/commit/eed9912))



## [0.7.1](https://github.com/confluentinc/ksql/releases/tag/v0.7.1-ksqldb) (2020-02-28)

### Features

* support custom column widths in cli ([#4616](https://github.com/confluentinc/ksql/pull/4616)) ([cb66e05](https://github.com/confluentinc/ksql/commit/cb66e05))
    
    A new ksqlDB CLI configuration allows you to specify the width of each column in tabular output.

    ```
    ksql> SET CLI COLUMN-WIDTH 10
    ```
    Given a customized value, subsequent renderings of output use the setting:
    
    ```
    ksql> SELECT * FROM riderLocations
    >  WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;
    +----------+----------+----------+----------+----------+
    |ROWTIME   |ROWKEY    |PROFILEID |LATITUDE  |LONGITUDE |
    +----------+----------+----------+----------+----------+
    ```
    The default behavior, which determines column width based on terminal width and the number of columns, can be re-enabled using:
    ```
    ksql> SET CLI COLUMN-WIDTH 0
    ```
* support partial schemas ([#4625](https://github.com/confluentinc/ksql/pull/4625)) ([7cc19a0](https://github.com/confluentinc/ksql/commit/7cc19a0))

### Bug Fixes

* add functional-test dependencies to Docker module ([#4586](https://github.com/confluentinc/ksql/pull/4586)) ([04fcf8d](https://github.com/confluentinc/ksql/commit/04fcf8d))
* don't cleanup topics on engine close ([#4658](https://github.com/confluentinc/ksql/pull/4658)) ([ad66a81](https://github.com/confluentinc/ksql/commit/ad66a81))
* idempotent terminate that can handle hung streams ([#4643](https://github.com/confluentinc/ksql/pull/4643)) ([d96db14](https://github.com/confluentinc/ksql/commit/d96db14))


## [0.7.0](https://github.com/confluentinc/ksql/releases/tag/v0.7.0-ksqldb) (2020-02-11)

### Upgrading

Note that ksqlDB 0.7.0 has a number of breaking changes when compared with ksqlDB 0.6.0 (see the 'Breaking changes' section below for details). Please make sure to read and follow these [upgrade instructions](./docs-md/operate-and-deploy/installation/upgrading.md) if you are upgrading from a previous ksqlDB version.

### Features

* feat: primitive key support ([#4478](https://github.com/confluentinc/ksql/pull/4478)) ([ddf09d](https://github.com/confluentinc/ksql/commit/ddf09d))

    ksqlDB now supports the following primitive key types: `INT`, `BIGINT`, `DOUBLE` as well as the existing `STRING` type.

    The key type can be defined in the CREATE TABLE or CREATE STREAM statement by including a column definition for `ROWKEY` in the form `ROWKEY <primitive-key-type> KEY,`, for example:
    ```sql
    CREATE TABLE USERS (ROWKEY BIGINT KEY, NAME STRING, RATING DOUBLE) WITH (kafka_topic='users', VALUE_FORMAT='json');
    ```
    ksqlDB currently requires the name of the key column to be `ROWKEY`. Support for arbitrary key names is tracked by #3536.

    ksqlDB currently requires keys to use the `KAFKA` format. Support for additional formats is tracked by https://github.com/confluentinc/ksql/projects/3.

    Schema inference currently only works with `STRING` keys, Support for additional key types is tracked by #4462. (Schema inference is where ksqlDB infers the schema of a CREATE TABLE and CREATE STREAM statements from the schema registered in the Schema Registry, as opposed to the user supplying the set of columns in the statement).

    Apache Kafka Connect can be configured to output keys in the `KAFKA` format by using a Converter, e.g. `"key.converter": "org.apache.kafka.connect.converters.IntegerConverter"`. Details of which converter to use for which key type can be found here: https://docs.confluent.io/current/ksql/docs/developer-guide/serialization.html#kafka in the `Connect Converter` column.

    @rmoff has written an introductory blog about primitive keys: https://rmoff.net/2020/02/07/primitive-keys-in-ksqldb/
* add a new default SchemaRegistryClient and remove default for SR url ([#4325](https://github.com/confluentinc/ksql/pull/4325)) ([e045f7c](https://github.com/confluentinc/ksql/commit/e045f7c))
* Adds lag reporting and API for use in lag aware routing as described in KLIP 12 ([#4392](https://github.com/confluentinc/ksql/pull/4392)) ([cb9ae29](https://github.com/confluentinc/ksql/commit/cb9ae29))
* better error message when transaction to command topic fails to initialize by timeout ([#4486](https://github.com/confluentinc/ksql/pull/4486)) ([a5fed3b](https://github.com/confluentinc/ksql/commit/a5fed3b))
* expression support in JOINs ([#4278](https://github.com/confluentinc/ksql/pull/4278)) ([2d0bfe8](https://github.com/confluentinc/ksql/commit/2d0bfe8))
* hide internal/system topics from SHOW TOPICS ([#4322](https://github.com/confluentinc/ksql/pull/4322)) ([075fed3](https://github.com/confluentinc/ksql/commit/075fed3))
* Implement pull query routing to standbys if active is down ([#4398](https://github.com/confluentinc/ksql/pull/4398)) ([ace23b1](https://github.com/confluentinc/ksql/commit/ace23b1))
* Implementation of heartbeat mechanism as part of KLIP-12 ([#4173](https://github.com/confluentinc/ksql/pull/4173)) ([37c1eaa](https://github.com/confluentinc/ksql/commit/37c1eaa))
* native map/array constructors ([#4232](https://github.com/confluentinc/ksql/pull/4232)) ([3ecfaad](https://github.com/confluentinc/ksql/commit/3ecfaad))
* support implicit casting in UDFs ([#4406](https://github.com/confluentinc/ksql/pull/4406)) ([6fc4f72](https://github.com/confluentinc/ksql/commit/6fc4f72))
* add COUNT_DISTINCT and allow generics in UDAFs ([#4150](https://github.com/confluentinc/ksql/pull/4150)) ([2d5e680](https://github.com/confluentinc/ksql/commit/2d5e680))
* Add Cube UDTF ([#3935](https://github.com/confluentinc/ksql/pull/3935)) ([6be8e7c](https://github.com/confluentinc/ksql/commit/6be8e7c))
* remove WindowStart() and WindowEnd() UDAFs ([#4459](https://github.com/confluentinc/ksql/pull/4459)) ([eda2e34](https://github.com/confluentinc/ksql/commit/eda2e34))
* ask for password if -p is not provided ([#4153](https://github.com/confluentinc/ksql/pull/4153)) ([7a83bbf](https://github.com/confluentinc/ksql/commit/7a83bbf))
* make (certain types of) server error messages configurable ([#4121](https://github.com/confluentinc/ksql/pull/4121)) ([cedf47e](https://github.com/confluentinc/ksql/commit/cedf47e))
* add source statement to SourceDescription ([#4134](https://github.com/confluentinc/ksql/pull/4134)) ([1146aa5](https://github.com/confluentinc/ksql/commit/1146aa5))
* add support for inline struct creation ([#4120](https://github.com/confluentinc/ksql/pull/4120)) ([6e558da](https://github.com/confluentinc/ksql/commit/6e558da))
* allow environment variables to configure embedded connect ([#4260](https://github.com/confluentinc/ksql/pull/4260)) ([e032ea9](https://github.com/confluentinc/ksql/commit/e032ea9))
* enable Kafla ACL authorization checks for Pull Queries ([#4187](https://github.com/confluentinc/ksql/pull/4187)) ([5ee1e9e](https://github.com/confluentinc/ksql/commit/5ee1e9e))
* implemention of KLIP-13 ([#4099](https://github.com/confluentinc/ksql/pull/4099)) ([b23dae9](https://github.com/confluentinc/ksql/commit/b23dae9))
* show properties now includes embedded connect properties and scope ([#4099](https://github.com/confluentinc/ksql/pull/4099)) ([ebac104](https://github.com/confluentinc/ksql/commit/ebac104))
* add connector status to LIST CONNECTORS ([#4077](https://github.com/confluentinc/ksql/pull/4077)) ([5ff94b6](https://github.com/confluentinc/ksql/commit/5ff94b6))
* add JMX metric for commandRunner status ([#4019](https://github.com/confluentinc/ksql/pull/4019)) ([55d75f2](https://github.com/confluentinc/ksql/commit/55d75f2))
* add support to terminate all running queries ([#3944](https://github.com/confluentinc/ksql/pull/3944)) ([abbce84](https://github.com/confluentinc/ksql/commit/abbce84))
* expose execution plans from the ksql engine API ([#3482](https://github.com/confluentinc/ksql/pull/3482)) ([067139c](https://github.com/confluentinc/ksql/commit/067139c))
* expression support for PARTITION BY ([#4032](https://github.com/confluentinc/ksql/pull/4032)) ([0f31f8e](https://github.com/confluentinc/ksql/commit/0f31f8e))
* remove unnecessary changelog for topics ([#3987](https://github.com/confluentinc/ksql/pull/3987)) ([6e0d00e](https://github.com/confluentinc/ksql/commit/6e0d00e))



### Performance Improvements

* Avoids logging INFO for rest-util requests, since it hurts pull query performance ([#4302](https://github.com/confluentinc/ksql/pull/4302)) ([50b4c1c](https://github.com/confluentinc/ksql/commit/50b4c1c))
* Improves pull query performance by making the default schema service a singleton ([#4216](https://github.com/confluentinc/ksql/pull/4216)) ([f991752](https://github.com/confluentinc/ksql/commit/f991752))




### Bug Fixes

* add ksql-test-runner deps to ksql package lib ([#4272](https://github.com/confluentinc/ksql/pull/4272)) ([6e28cc4](https://github.com/confluentinc/ksql/commit/6e28cc4))
* ConcurrentModificationException in ClusterStatusResource ([#4510](https://github.com/confluentinc/ksql/pull/4510)) ([c79cba9](https://github.com/confluentinc/ksql/commit/c79cba9))
* deadlock when closing transient push query ([#4297](https://github.com/confluentinc/ksql/pull/4297)) ([ac8fb63](https://github.com/confluentinc/ksql/commit/ac8fb63))
* delimiters reset across non-delimited types (reverts [#4366](https://github.com/confluentinc/ksql/issues/4366)) ([#4371](https://github.com/confluentinc/ksql/pull/4371)) ([5788729](https://github.com/confluentinc/ksql/commit/5788729))
* do not throw error if VALUE_DELIMITER is set on non-DELIMITED topic ([#4366](https://github.com/confluentinc/ksql/pull/4366)) ([2b59b8b](https://github.com/confluentinc/ksql/commit/2b59b8b))
* exception on shutdown of ksqlDB server ([#4483](https://github.com/confluentinc/ksql/pull/4483)) ([126e2cf](https://github.com/confluentinc/ksql/commit/126e2cf))
* fix compilation error due to `Format` refactoring ([#4465](https://github.com/confluentinc/ksql/pull/4465)) ([07a4dcd](https://github.com/confluentinc/ksql/commit/07a4dcd))
* fix NPE in CLI if not username supplied ([#4312](https://github.com/confluentinc/ksql/pull/4312)) ([0b6da0b](https://github.com/confluentinc/ksql/commit/0b6da0b))
* Fixes the single host lag reporting case ([#4494](https://github.com/confluentinc/ksql/pull/4494)) ([6b8bc2a](https://github.com/confluentinc/ksql/commit/6b8bc2a))
* floating point comparison was inexact ([#4372](https://github.com/confluentinc/ksql/pull/4372)) ([2a4ca47](https://github.com/confluentinc/ksql/commit/2a4ca47))
* Include functional tests jar in docker images ([#4274](https://github.com/confluentinc/ksql/pull/4274)) ([2559b2f](https://github.com/confluentinc/ksql/commit/2559b2f))
* include valid alternative UDF signatures in error message (MINOR) ([#4403](https://github.com/confluentinc/ksql/pull/4403)) ([f397ad8](https://github.com/confluentinc/ksql/commit/f397ad8))
* Make null key serialization/deserialization symmetrical ([#4351](https://github.com/confluentinc/ksql/pull/4351)) ([2a61acb](https://github.com/confluentinc/ksql/commit/2a61acb))
* partial push & persistent query support for window bounds columns ([#4401](https://github.com/confluentinc/ksql/pull/4401)) ([48aa6ec](https://github.com/confluentinc/ksql/commit/48aa6ec))
* print root cause in error message ([#4505](https://github.com/confluentinc/ksql/pull/4505)) ([6299410](https://github.com/confluentinc/ksql/commit/6299410))
* pull queries should work across nodes ([#4169](https://github.com/confluentinc/ksql/pull/4169)) ([#4271](https://github.com/confluentinc/ksql/issues/4271)) ([2369213](https://github.com/confluentinc/ksql/commit/2369213))
* remove deprecated Acl API ([#4373](https://github.com/confluentinc/ksql/pull/4373)) ([a2b69f7](https://github.com/confluentinc/ksql/commit/a2b69f7))
* remove duplicate comment about Schema Regitry URL from sample server properties ([#4346](https://github.com/confluentinc/ksql/pull/4346)) ([0d542c5](https://github.com/confluentinc/ksql/commit/0d542c5))
* rename stale to standby in KsqlConfig ([#4467](https://github.com/confluentinc/ksql/pull/4467)) ([f8bb986](https://github.com/confluentinc/ksql/commit/f8bb986))
* report window type and query status better from API ([#4313](https://github.com/confluentinc/ksql/pull/4313)) ([ca9368a](https://github.com/confluentinc/ksql/commit/ca9368a))
* reserve `WINDOWSTART` and `WINDOWEND` as system column names ([#4388](https://github.com/confluentinc/ksql/pull/4388)) ([ea0a0ac](https://github.com/confluentinc/ksql/commit/ea0a0ac))
* Sets timezone of RestQueryTranslationTest test to make it work in non UTC zones ([#4407](https://github.com/confluentinc/ksql/pull/4407)) ([50b25d5](https://github.com/confluentinc/ksql/commit/50b25d5))
* show queries now returns the correct Kafka Topic if the query string contains with clause ([#4430](https://github.com/confluentinc/ksql/pull/4430)) ([1b713cd](https://github.com/confluentinc/ksql/commit/1b713cd))
* support conversion of STRING to BIGINT for window bounds ([#4500](https://github.com/confluentinc/ksql/pull/4500)) ([9c3cbf8](https://github.com/confluentinc/ksql/commit/9c3cbf8))
* support WindowStart() and WindowEnd() in pull queries ([#4435](https://github.com/confluentinc/ksql/pull/4435)) ([8da2b63](https://github.com/confluentinc/ksql/commit/8da2b63))
* add logging during restore ([#4270](https://github.com/confluentinc/ksql/pull/4270)) ([4e32da6](https://github.com/confluentinc/ksql/commit/4e32da6))
* log4j properties files ([#4293](https://github.com/confluentinc/ksql/pull/4293)) ([5911faf](https://github.com/confluentinc/ksql/commit/5911faf))
* report clearer error message when AVG used with DELIMITED ([#4295](https://github.com/confluentinc/ksql/pull/4295)) ([307bf4d](https://github.com/confluentinc/ksql/commit/307bf4d))
* better error message on self-join ([#4248](https://github.com/confluentinc/ksql/pull/4248)) ([1281ab2](https://github.com/confluentinc/ksql/commit/1281ab2))
* change query id generation to work with planned commands ([#4149](https://github.com/confluentinc/ksql/pull/4149)) ([91c421a](https://github.com/confluentinc/ksql/commit/91c421a))
* CLI commands may be terminated with semicolon+whitespace (MINOR) ([#4234](https://github.com/confluentinc/ksql/pull/4234)) ([096b78f](https://github.com/confluentinc/ksql/commit/096b78f))
* decimals in structs should display as numeric ([#4165](https://github.com/confluentinc/ksql/pull/4165)) ([75b539e](https://github.com/confluentinc/ksql/commit/75b539e))
* don't load current qtt test case from legacy loader ([#4245](https://github.com/confluentinc/ksql/pull/4245)) ([9479fd6](https://github.com/confluentinc/ksql/commit/9479fd6))
* immutability in some more classes (MINOR) ([#4179](https://github.com/confluentinc/ksql/pull/4179)) ([cbd3bab](https://github.com/confluentinc/ksql/commit/cbd3bab))
* include path of field that causes JSON deserialization error ([#4249](https://github.com/confluentinc/ksql/pull/4249)) ([5cc718b](https://github.com/confluentinc/ksql/commit/5cc718b))
* reintroduce FetchFieldFromStruct as a public UDF ([#4185](https://github.com/confluentinc/ksql/pull/4185)) ([a50a665](https://github.com/confluentinc/ksql/commit/a50a665))
* show topics doesn't display topics with different casing ([#4159](https://github.com/confluentinc/ksql/pull/4159)) ([0ac8747](https://github.com/confluentinc/ksql/commit/0ac8747))
* untracked file after cloning on Windows ([#4122](https://github.com/confluentinc/ksql/pull/4122)) ([04de30e](https://github.com/confluentinc/ksql/commit/04de30e))
* array access is now 1-indexed instead of 0-indexed ([#4057](https://github.com/confluentinc/ksql/pull/4057)) ([f09f797](https://github.com/confluentinc/ksql/commit/f09f797))
* Explicitly disallow table functions with table sources, fixes [#4033](https://github.com/confluentinc/ksql/issues/4033) ([#4085](https://github.com/confluentinc/ksql/pull/4085)) ([60e20ef](https://github.com/confluentinc/ksql/commit/60e20ef))
* fix issues with multi-statement requests failing to validate ([#3952](https://github.com/confluentinc/ksql/pull/3952)) ([3e7169b](https://github.com/confluentinc/ksql/commit/3e7169b)), closes [#3363](https://github.com/confluentinc/ksql/issues/3363)
* NPE when starting StandaloneExecutor ([#4119](https://github.com/confluentinc/ksql/pull/4119)) ([c6c00b1](https://github.com/confluentinc/ksql/commit/c6c00b1))
* properly set key when partition by ROWKEY and join on non-ROWKEY ([#4090](https://github.com/confluentinc/ksql/pull/4090)) ([6c80941](https://github.com/confluentinc/ksql/commit/6c80941))
* remove mapValues that excluded ROWTIME and ROWKEY columns ([#4066](https://github.com/confluentinc/ksql/pull/4066)) ([a6982bd](https://github.com/confluentinc/ksql/commit/a6982bd)), closes [#4052](https://github.com/confluentinc/ksql/issues/4052)
* robin's requested message changes ([#4021](https://github.com/confluentinc/ksql/pull/4021)) ([422a2e3](https://github.com/confluentinc/ksql/commit/422a2e3))
* schema column order returned by websocket pull query ([#4012](https://github.com/confluentinc/ksql/pull/4012)) ([85fef09](https://github.com/confluentinc/ksql/commit/85fef09))
* some terminals dont work with JLine 3.11 ([#3931](https://github.com/confluentinc/ksql/pull/3931)) ([ad183ec](https://github.com/confluentinc/ksql/commit/ad183ec))
* the Abs, Ceil and Floor methods now return proper types ([#3948](https://github.com/confluentinc/ksql/pull/3948)) ([3d6e119](https://github.com/confluentinc/ksql/commit/3d6e119))
* UncaughtExceptionHandler not being set for Persistent Queries ([#4087](https://github.com/confluentinc/ksql/pull/4087)) ([e193a2a](https://github.com/confluentinc/ksql/commit/e193a2a))
* unify behavior for PARTITION BY and GROUP BY ([#3982](https://github.com/confluentinc/ksql/pull/3982)) ([67d3f8c](https://github.com/confluentinc/ksql/commit/67d3f8c))
* wrong source type in pull query error message ([#3885](https://github.com/confluentinc/ksql/pull/3885)) ([65523c7](https://github.com/confluentinc/ksql/commit/65523c7)), closes [#3523](https://github.com/confluentinc/ksql/issues/3523)



### BREAKING CHANGES

* existing queries that perform a PARTITION BY or GROUP BY on a single column of one of the above supported primitive key types will now set the key to the appropriate type, not a `STRING` as previously.
* The `WindowStart()` and `WindowEnd()` UDAFs have been removed from KSQL. Use the `WindowStart` and `WindowEnd` system columns to access the window bounds within the SELECT expression instead.
* the order of columns for internal topics has changed. The `DELIMITED` format can not handle this in a backwards compatible way. Hence this is a breaking change for any existing queries the use the `DELIMITED` format and have internal topics.
This change has been made now for two reasons:
   1. its a breaking change, making it much harder to do later.
   2. The recent  https://github.com/confluentinc/ksql/pull/4404 change introduced this same issue for pull queries. This current change corrects pull queries too.
* Any query of a windowed source that uses `ROWKEY` in the SELECT projection will see the contents of `ROWKEY` change from a formatted `STRING` containing the underlying key and the window bounds, to just the underlying key.  Queries can access the window bounds using `WINDOWSTART` and `WINDOWEND`.
* Joins on windowed sources now include `WINDOWSTART` and `WINDOWEND` columns from both sides on a `SELECT *`.
* `WINDOWSTART` and `WINDOWEND` are now reserved system column names. Any query that previously used those names will need to be changed: for example, alias the columns to a different name.
These column names are being reserved for use as system columns when dealing with streams and tables that have a windowed key.
* standalone literals that used to be doubles may now be
interpreted as BigDecimal. In most scenarios, this won't affect any
queries as the DECIMAL can auto-cast to DOUBLE; in the case were the
literal stands alone, the output schema will be a DECIMAL instead of a
DOUBLE. To specify a DOUBLE literal, use scientific notation (e.g.
1.234E-5).
* The response from the RESTful API has changed for some commands with this commit: the `SourceDescription` type no longer has a `format` field. Instead it has `keyFormat` and `valueFormat` fields.
	Response now includes a `state` property for each query that indicates the state of the query.
	e.g.
	```json
	{
	"queryString" : "create table OUTPUT as select * from INPUT;",
	"sinks" : [ "OUTPUT" ],
	"id" : "CSAS_OUTPUT_0",
	"state" : "Running"
	}
	```
	The CLI output was:
	```
	ksql> show queries;
	Query ID                   | Kafka Topic         | Query String
	CSAS_OUTPUT_0              | OUTPUT              | CREATE STREAM OUTPUT WITH (KAFKA_TOPIC='OUTPUT', PARTITIONS=1, REPLICAS=1) AS SELECT *
	FROM INPUT INPUT
	EMIT CHANGES;
	CTAS_CLICK_USER_SESSIONS_5 | CLICK_USER_SESSIONS | CREATE TABLE CLICK_USER_SESSIONS WITH (KAFKA_TOPIC='CLICK_USER_SESSIONS', PARTITIONS=1, REPLICAS=1) AS SELECT
	CLICKSTREAM.USERID USERID,
	COUNT(*) COUNT
	FROM CLICKSTREAM CLICKSTREAM
	WINDOW SESSION ( 300 SECONDS )
	GROUP BY CLICKSTREAM.USERID
	EMIT CHANGES;
	For detailed information on a Query run: EXPLAIN <Query ID>;
	```
	and is now:
	```
	Query ID                   | Status      | Kafka Topic         | Query String
	CSAS_OUTPUT_0              | RUNNING     | OUTPUT              | CREATE STREAM OUTPUT WITH (KAFKA_TOPIC='OUTPUT', PARTITIONS=1, REPLICAS=1) AS SELECT *FROM INPUT INPUTEMIT CHANGES;
	For detailed information on a Query run: EXPLAIN <Query ID>;
	```
	Note the addition of the `Status` column and the fact that `Query String` is now longer being written across multiple lines.
	old CLI output:
	```
	ksql> describe CLICK_USER_SESSIONS;
	Name                 : CLICK_USER_SESSIONS
	Field   | Type
	ROWTIME | BIGINT           (system)
	ROWKEY  | INTEGER          (system)
	USERID  | INTEGER
	COUNT   | BIGINT
	For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
	```
	New CLI output:
	```
	ksql> describe CLICK_USER_SESSIONS;
	Name                 : CLICK_USER_SESSIONS
	Field   | Type
	ROWTIME | BIGINT           (system)
	ROWKEY  | INTEGER          (system) (Window type: SESSION)
	USERID  | INTEGER
	COUNT   | BIGINT
	For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
	```
	Note the addition of the `Window Type` information.
	The extended version of the command has also changed.
	Old output:
	```
	ksql> describe extended CLICK_USER_SESSIONS;
	Name                 : CLICK_USER_SESSIONS
	Type                 : TABLE
	Key field            : USERID
	Key format           : STRING
	Timestamp field      : Not set - using <ROWTIME>
	Value Format                : JSON
	Kafka topic          : CLICK_USER_SESSIONS (partitions: 1, replication: 1)
	Statement            : CREATE TABLE CLICK_USER_SESSIONS WITH (KAFKA_TOPIC='CLICK_USER_SESSIONS', PARTITIONS=1, REPLICAS=1) AS SELECT
	CLICKSTREAM.USERID USERID,
	COUNT(*) COUNT
	FROM CLICKSTREAM CLICKSTREAM
	WINDOW SESSION ( 300 SECONDS )
	GROUP BY CLICKSTREAM.USERID
	EMIT CHANGES;
	Field   | Type
	ROWTIME | BIGINT           (system)
	ROWKEY  | INTEGER          (system)
	USERID  | INTEGER
	COUNT   | BIGINT
	```
* Any `KEY` column identified in the `WITH` clause must be of the same Sql type as `ROWKEY`.
	Users can provide the name of a value column that matches the key column, e.g.
	```sql
	CREATE STREAM S (ID INT, NAME STRING) WITH (KEY='ID', ...);
	```
	Before primitive keys was introduced all keys were treated as `STRING`. With primitive keys `ROWKEY` can be types other than `STRING`, e.g. `BIGINT`.
	It therefore follows that any `KEY` column identified in the `WITH` clause must have the same SQL type as the _actual_ key,  i.e. `ROWKEY`.
	With this change the above example statement will fail with the error:
	```
	The KEY field (ID) identified in the WITH clause is of a different type to the actual key column.
	Either change the type of the KEY field to match ROWKEY, or explicitly set ROWKEY to the type of the KEY field by adding 'ROWKEY INTEGER KEY' in the schema.
	KEY field type: INTEGER
	ROWKEY type: STRING
	```
	As the error message says, the error can be resolved by changing the statement to:
	```sql
	CREATE STREAM S (ROWKEY INT KEY, ID INT, NAME STRING) WITH (KEY='ID', ...);
	```
* Some existing joins may now fail and the type of `ROWKEY` in the result schema of joins may have changed.
When `ROWKEY` was always a `STRING` it was possible to join an `INTEGER` column with a `BIGINT` column.  This is no longer the case. A `JOIN` requires the join columns to be of the same type. (See https://github.com/confluentinc/ksql/issues/4130 which tracks adding support for being able to `CAST` join criteria).
Where joining on two `INT` columns would previously have resulted in a schema containing `ROWKEY STRING KEY`, it would not result in `ROWKEY INT KEY`.
* A `GROUP BY` on single expressions now changes the SQL type of `ROWKEY` in the output schema of the query to match the SQL type of the expression.
	For example, consider:
	```sql
	CREATE STREAM INPUT (ROWKEY STRING KEY, ID INT) WITH (...);
	CREATE TABLE OUTPUT AS SELECT COUNT(*) AS COUNT FROM INPUT GROUP BY ID;
	```
	Previously, the above would have resulted in an output schema of `ROWKEY STRING KEY, COUNT BIGINT`, where `ROWKEY` would have stored the string representation of the integer from the `ID` column.
	With this commit the output schema will be `ROWKEY INT KEY COUNT BIGINT`.
* Any`GROUP BY` expression that resolves to `NULL`, including because a UDF throws an exception, now results in the row being excluded from the result.  Previously, as the key was a `STRING` a value of `"null"` could be used. With other primitive types this is not possible. As key columns must be non-null any exception is logged and the row is excluded.
* commands that were persisted with RUN SCRIPT will no
longer be executable
* the ARRAYCONTAINS function now needs to be referenced
as either JSON_ARRAY_CONTAINS or ARRAY_CONTAINS depending on the
intended param types
* A `PARTITION BY` now changes the SQL type of `ROWKEY` in the output schema of a query.
	For example, consider:
	```sql
	CREATE STREAM INPUT (ROWKEY STRING KEY, ID INT) WITH (...);
	CREATE STREAM OUTPUT AS SELECT ROWKEY AS NAME FROM INPUT PARTITION BY ID;
	```
	Previously, the above would have resulted in an output schema of `ROWKEY STRING KEY, NAME STRING`, where `ROWKEY` would have stored the string representation of the integer from the `ID` column.  With this commit the output schema will be `ROWKEY INT KEY, NAME STRING`.
* any queries that were using array index mechanism
should change to use 1-base indexing instead of 0-base.
* The maxInterval parameter for ksql-datagen is now deprecated. Use msgRate instead.
* this change makes it so that PARTITION BY statements
use the _source_ schema, not the value/projection schema, when selecting
the value to partition by. This is consistent with GROUP BY, and
standard SQL for GROUP by. Any statement that previously used PARTITION
BY may need to be reworked. 1/2
* when querying with EMIT CHANGES and PARTITION BY, the
PARTITION BY clause should now come before EMIT CHANGES. 2/2
* KSQL will now, by default, not create duplicate changelog for table sources.
fixes: https://github.com/confluentinc/ksql/issues/3621
Now that Kafka Steams has a `KTable.transformValues` we no longer need to create a table by first creating a stream, then doing a select/groupby/aggregate on it. Instead, we can just use `StreamBuilder.table`.
This change makes the switch, removing the `StreamToTable` types and calls and replaces them with either `TableSource` or `WindowedTableSource`, copying the existing pattern for `StreamSource` and `WindowedStreamSource`.
It also reinstates a change in `KsqlConfig` that ensures topology optimisations are on by default. This was the case for 5.4.x, but was inadvertently turned off.
With the optimisation config turned on, and the new builder step used, KSQL no longer creates a changelog topic to back the tables state store. This is not needed as the source topic is itself the changelog.  The change includes new tests in `table.json` to confirm the change log topic is not created by default and is created if the user turns off optimisations.
This change also removes the line in the `TestExecutor` that explicitly sets topology optimisations to `all`. The test should _not_ of being doing tis. This may been why the bug turning off optimisations was not detected.
* this change removes the old method of generating query
IDs based on their sequence of _successful_ execution. Instead all
queries will use their offset in the command topic. Similarly, all DROP
queries issued before 5.0 will no longer cascade query terminiation.
* `ALL` is now a reserved word and can not be used for identifiers without being quoted.
* abs, ceil and floor will now return types aligned with
other databases systems (i.e. the same type as the input). Previously
these udfs would always return Double.
* Statements in the command topic will be retried until they succeed. For example, if the source topic has been deleted for a create stream/table statement, the server may fail to start since command runner will be stuck processing the statement. This ensures that the same set of streams/tables are created when restarting the server. You can check to see if the command runner is stuck by:
    1. Looking in the server logs to see if a statement is being retried.
    2. The JMX metric `_confluent-ksql-<service-id>ksql-rest-app-command-runner` will be in an `ERROR` state





## [v0.6.0](https://github.com/confluentinc/ksql/releases/tag/v0.6.0-ksqldb) (2019-11-19)

### Features

* add config to disable pull queries when validation is required ([#3879](https://github.com/confluentinc/ksql/pull/3879)) ([ccc636d](https://github.com/confluentinc/ksql/commit/ccc636d)), closes [#3863](https://github.com/confluentinc/ksql/issues/3863)
* add configurable metrics reporters ([#3490](https://github.com/confluentinc/ksql/pull/3490)) ([378b8af](https://github.com/confluentinc/ksql/commit/378b8af))
* add flag to disable pull queries (MINOR) ([#3778](https://github.com/confluentinc/ksql/pull/3778)) ([04e206f](https://github.com/confluentinc/ksql/commit/04e206f))
* add health check endpoint ([#3501](https://github.com/confluentinc/ksql/pull/3501)) ([2308686](https://github.com/confluentinc/ksql/commit/2308686))
* add KsqlUncaughtExceptionHandler and new KsqlRestConfig for enabling it ([#3425](https://github.com/confluentinc/ksql/pull/3425)) ([d83c787](https://github.com/confluentinc/ksql/commit/d83c787))
* add request logging ([#3518](https://github.com/confluentinc/ksql/pull/3518)) ([c401ec0](https://github.com/confluentinc/ksql/commit/c401ec0))
* Add UDF invoker benchmark ([#3592](https://github.com/confluentinc/ksql/pull/3592)) ([83dfc24](https://github.com/confluentinc/ksql/commit/83dfc24))
* Added UDFs ENTRIES and GENERATE_SERIES ([#3724](https://github.com/confluentinc/ksql/pull/3724)) ([0a4558b](https://github.com/confluentinc/ksql/commit/0a4558b))
* build ks app from an execution plan visitor ([#3418](https://github.com/confluentinc/ksql/pull/3418)) ([b57d194](https://github.com/confluentinc/ksql/commit/b57d194))
* build materializations from the physical plan ([#3494](https://github.com/confluentinc/ksql/pull/3494)) ([f45d649](https://github.com/confluentinc/ksql/commit/f45d649))
* change /metadata REST path to /v1/metadata ([#3467](https://github.com/confluentinc/ksql/pull/3467)) ([ed94895](https://github.com/confluentinc/ksql/commit/ed94895))
* Config file for the no-response bot which closes issues which haven't been responded to ([#3765](https://github.com/confluentinc/ksql/pull/3765)) ([1dfdb68](https://github.com/confluentinc/ksql/commit/1dfdb68))
* drop legacy key field functionality ([#3764](https://github.com/confluentinc/ksql/pull/3764)) ([5369dc2](https://github.com/confluentinc/ksql/commit/5369dc2))
* expose query status through EXPLAIN ([#3570](https://github.com/confluentinc/ksql/pull/3570)) ([8ef82eb](https://github.com/confluentinc/ksql/commit/8ef82eb))
* expression support for insert values ([#3612](https://github.com/confluentinc/ksql/pull/3612)) ([37f9763](https://github.com/confluentinc/ksql/commit/37f9763))
* Implement complex expressions for table functions ([#3683](https://github.com/confluentinc/ksql/pull/3683)) ([200022b](https://github.com/confluentinc/ksql/commit/200022b))
* Implement describe and list functions for UDTFs ([#3716](https://github.com/confluentinc/ksql/pull/3716)) ([b0bbea4](https://github.com/confluentinc/ksql/commit/b0bbea4))
* Implement EXPLODE(ARRAY) for single table function in SELECT  ([#3589](https://github.com/confluentinc/ksql/pull/3589)) ([8b52aa8](https://github.com/confluentinc/ksql/commit/8b52aa8))
* Implement schemaProvider for UDTFs ([#3690](https://github.com/confluentinc/ksql/pull/3690)) ([4e66825](https://github.com/confluentinc/ksql/commit/4e66825))
* Implement user defined table functions ([#3687](https://github.com/confluentinc/ksql/pull/3687)) ([e62bd46](https://github.com/confluentinc/ksql/commit/e62bd46))
* Makes timeout for owner lookup in StaticQueryExecutor and rebalancing in KsStateStore configurable ([#3856](https://github.com/confluentinc/ksql/pull/3856)) ([39245c6](https://github.com/confluentinc/ksql/commit/39245c6))
* pass auth header to connect client for RBAC integration ([#3492](https://github.com/confluentinc/ksql/pull/3492)) ([cef0ea3](https://github.com/confluentinc/ksql/commit/cef0ea3))
* serialize expressions ([#3721](https://github.com/confluentinc/ksql/pull/3721)) ([e1cd477](https://github.com/confluentinc/ksql/commit/e1cd477))
* Support multiple table functions in queries ([#3685](https://github.com/confluentinc/ksql/pull/3685)) ([44be5a2](https://github.com/confluentinc/ksql/commit/44be5a2))
* support numeric json serde for decimals ([#3588](https://github.com/confluentinc/ksql/pull/3588)) ([8621594](https://github.com/confluentinc/ksql/commit/8621594))
* support quoted identifiers in column names ([#3477](https://github.com/confluentinc/ksql/pull/3477)) ([be2bdcc](https://github.com/confluentinc/ksql/commit/be2bdcc))
* Transactional Produces to Command Topic ([#3660](https://github.com/confluentinc/ksql/pull/3660)) ([cba2877](https://github.com/confluentinc/ksql/commit/cba2877))
* **static:** allow logical schema to have fields in any order ([#3422](https://github.com/confluentinc/ksql/pull/3422)) ([d935af3](https://github.com/confluentinc/ksql/commit/d935af3))
* **static:** allow windowed queries without bounds on rowtime ([#3438](https://github.com/confluentinc/ksql/pull/3438)) ([6593ee3](https://github.com/confluentinc/ksql/commit/6593ee3))
* **static:** fail on ROWTIME in projection ([#3430](https://github.com/confluentinc/ksql/pull/3430)) ([2f27b68](https://github.com/confluentinc/ksql/commit/2f27b68))
* **static:** support for partial datetimes on `WindowStart` bounds ([#3435](https://github.com/confluentinc/ksql/pull/3435)) ([99f6e24](https://github.com/confluentinc/ksql/commit/99f6e24))
* **static:** support ROWKEY in the projection of static queries ([#3439](https://github.com/confluentinc/ksql/pull/3439)) ([9218766](https://github.com/confluentinc/ksql/commit/9218766))
* **static:** switch partial datetime parser to use UTC by default ([#3473](https://github.com/confluentinc/ksql/pull/3473)) ([81557e3](https://github.com/confluentinc/ksql/commit/81557e3))
* **static:** unordered table elements and meta columns serialization ([#3428](https://github.com/confluentinc/ksql/pull/3428)) ([3b23fd6](https://github.com/confluentinc/ksql/commit/3b23fd6))
* add KsqlRocksDBConfigSetter to bound memory and set num threads ([#3167](https://github.com/confluentinc/ksql/pull/3167)) ([cdcaa2d](https://github.com/confluentinc/ksql/commit/cdcaa2d))
* add logs/ to .gitignore (MINOR) ([#3353](https://github.com/confluentinc/ksql/pull/3353)) ([81272cf](https://github.com/confluentinc/ksql/commit/81272cf))
* add offest to QueuedCommand and flag to Command ([#3343](https://github.com/confluentinc/ksql/pull/3343)) ([fd112a4](https://github.com/confluentinc/ksql/commit/fd112a4))
* add option for datagen to produce indefinitely (MINOR) ([#3307](https://github.com/confluentinc/ksql/pull/3307)) ([6281738](https://github.com/confluentinc/ksql/commit/6281738))
* add REST /metadata path to display KSQL server information (replaces /info) ([#3313](https://github.com/confluentinc/ksql/pull/3313)) ([8be29b9](https://github.com/confluentinc/ksql/commit/8be29b9))
* add SHOW TYPES to list all custom types ([#3280](https://github.com/confluentinc/ksql/pull/3280)) ([13fde33](https://github.com/confluentinc/ksql/commit/13fde33))
* Add support for average aggregate function ([#3302](https://github.com/confluentinc/ksql/pull/3302)) ([6757d9f](https://github.com/confluentinc/ksql/commit/6757d9f))
* back out Connect auto-import feature ([#3386](https://github.com/confluentinc/ksql/pull/3386)) ([d4c748c](https://github.com/confluentinc/ksql/commit/d4c748c))
* build execution plan from structured package ([#3285](https://github.com/confluentinc/ksql/pull/3285)) ([0d0b1c3](https://github.com/confluentinc/ksql/commit/0d0b1c3))
* change the public API of schema provider method ([#3287](https://github.com/confluentinc/ksql/pull/3287)) ([1324285](https://github.com/confluentinc/ksql/commit/1324285))
* custom comparators for array, map and struct ([#3385](https://github.com/confluentinc/ksql/pull/3385)) ([fe63d21](https://github.com/confluentinc/ksql/commit/fe63d21))
* Implement ROUND() UDF ([#3404](https://github.com/confluentinc/ksql/pull/3404)) ([f9783a9](https://github.com/confluentinc/ksql/commit/f9783a9))
* Implement user defined delimiter for value format ([#3393](https://github.com/confluentinc/ksql/pull/3393)) ([b84d0aa](https://github.com/confluentinc/ksql/commit/b84d0aa))
* move aggregation to plan builder ([#3391](https://github.com/confluentinc/ksql/pull/3391)) ([3aaeb73](https://github.com/confluentinc/ksql/commit/3aaeb73))
* move filters to plan builder ([#3346](https://github.com/confluentinc/ksql/pull/3346)) ([d4d52f3](https://github.com/confluentinc/ksql/commit/d4d52f3))
* move groupBy into plan builders ([#3359](https://github.com/confluentinc/ksql/pull/3359)) ([730c913](https://github.com/confluentinc/ksql/commit/730c913))
* move joins to plan builder ([#3361](https://github.com/confluentinc/ksql/pull/3361)) ([e243c74](https://github.com/confluentinc/ksql/commit/e243c74))
* move selectKey impl to plan builder ([#3362](https://github.com/confluentinc/ksql/pull/3362)) ([f312fcc](https://github.com/confluentinc/ksql/commit/f312fcc))
* move setup of the sink to plan builders ([#3360](https://github.com/confluentinc/ksql/pull/3360)) ([bfbdc20](https://github.com/confluentinc/ksql/commit/bfbdc20))
* remove equalsIgnoreCase from all Name classes (MINOR) ([#3411](https://github.com/confluentinc/ksql/pull/3411)) ([b78619c](https://github.com/confluentinc/ksql/commit/b78619c))
* update query id generation to use command topic record offset ([#3354](https://github.com/confluentinc/ksql/pull/3354)) ([295314e](https://github.com/confluentinc/ksql/commit/295314e))
* **cli:** add the feature to turn of WRAP for CLI output ([#3341](https://github.com/confluentinc/ksql/pull/3341)) ([3814c71](https://github.com/confluentinc/ksql/commit/3814c71))
* **static:** add custom jackson JSON serde for handling LogicalSchema ([#3322](https://github.com/confluentinc/ksql/pull/3322)) ([c571508](https://github.com/confluentinc/ksql/commit/c571508))
* **static:** add forEach() to KsqlStruct (MINOR) ([#3320](https://github.com/confluentinc/ksql/pull/3320)) ([587b545](https://github.com/confluentinc/ksql/commit/587b545))
* **static:** initial syntax for static queries ([#3300](https://github.com/confluentinc/ksql/pull/3300)) ([8917e48](https://github.com/confluentinc/ksql/commit/8917e48))
* **static:** static select support ([#3369](https://github.com/confluentinc/ksql/pull/3369)) ([e4b3275](https://github.com/confluentinc/ksql/commit/e4b3275))
* move toTable kstreams calls to plan builder ([#3334](https://github.com/confluentinc/ksql/pull/3334)) ([06aa252](https://github.com/confluentinc/ksql/commit/06aa252))
* use coherent naming scheme for generated java code ([#3417](https://github.com/confluentinc/ksql/pull/3417)) ([06a2a57](https://github.com/confluentinc/ksql/commit/06a2a57))
* **static:** initial drop of static query functionality ([#3340](https://github.com/confluentinc/ksql/pull/3340)) ([54c5139](https://github.com/confluentinc/ksql/commit/54c5139))
* add ability to DROP custom types ([#3281](https://github.com/confluentinc/ksql/pull/3281)) ([32005ed](https://github.com/confluentinc/ksql/commit/32005ed))
* Add schema resolver method to UDF specification ([#3215](https://github.com/confluentinc/ksql/pull/3215)) ([08855ad](https://github.com/confluentinc/ksql/commit/08855ad))
* add support to register custom types with KSQL (`CREATE TYPE`) ([#3266](https://github.com/confluentinc/ksql/pull/3266)) ([08ffebf](https://github.com/confluentinc/ksql/commit/08ffebf))
* include error message in DESCRIBE CONNECTOR (MINOR) ([#3289](https://github.com/confluentinc/ksql/pull/3289)) ([458f1d8](https://github.com/confluentinc/ksql/commit/458f1d8))
* perform topic permission checks for KSQL service principal ([#3261](https://github.com/confluentinc/ksql/pull/3261)) ([ba1f613](https://github.com/confluentinc/ksql/commit/ba1f613))
* wire in the KS config needed for point queries (MINOR) ([#3251](https://github.com/confluentinc/ksql/pull/3251)) ([5152d06](https://github.com/confluentinc/ksql/commit/5152d06))
* add a new module ksql-execution for the execution plan interfaces ([#3125](https://github.com/confluentinc/ksql/pull/3125)) ([3251d25](https://github.com/confluentinc/ksql/commit/3251d25))
* add an initial set of execution steps ([#3214](https://github.com/confluentinc/ksql/pull/3214)) ([c860793](https://github.com/confluentinc/ksql/commit/c860793))
* add config for custom metrics tags (5.3.x) ([#2996](https://github.com/confluentinc/ksql/pull/2996)) ([76f5590](https://github.com/confluentinc/ksql/commit/76f5590))
* add config for enabling topic access validator ([#3079](https://github.com/confluentinc/ksql/pull/3079)) ([440e247](https://github.com/confluentinc/ksql/commit/440e247))
* add connect templates and simplify JDBC source (MINOR) ([#3231](https://github.com/confluentinc/ksql/pull/3231)) ([ba0fb99](https://github.com/confluentinc/ksql/commit/ba0fb99))
* add DESCRIBE functionality for connectors ([#3206](https://github.com/confluentinc/ksql/pull/3206)) ([a79adb4](https://github.com/confluentinc/ksql/commit/a79adb4))
* add DROP CONNECTOR functionality ([#3245](https://github.com/confluentinc/ksql/pull/3245)) ([103c958](https://github.com/confluentinc/ksql/commit/103c958))
* add extension for custom metrics (5.3.x) ([#2997](https://github.com/confluentinc/ksql/pull/2997)) ([94a8ae7](https://github.com/confluentinc/ksql/commit/94a8ae7))
* add Logarithm, Exponential and Sqrt functions ([#3091](https://github.com/confluentinc/ksql/pull/3091)) ([a4ca934](https://github.com/confluentinc/ksql/commit/a4ca934))
* add SHOW CONNECTORS functionality ([#3210](https://github.com/confluentinc/ksql/pull/3210)) ([0bf31eb](https://github.com/confluentinc/ksql/commit/0bf31eb))
* Add SHOW TOPICS EXTENDED ([#3183](https://github.com/confluentinc/ksql/pull/3183)) ([dd3eb5f](https://github.com/confluentinc/ksql/commit/dd3eb5f)), closes [#1268](https://github.com/confluentinc/ksql/issues/1268)
* add SIGN, REPLACE and INITCAP ([#3189](https://github.com/confluentinc/ksql/pull/3189)) ([ab67684](https://github.com/confluentinc/ksql/commit/ab67684))
* Add window size for time window tables ([#3102](https://github.com/confluentinc/ksql/pull/3102)) ([6ff07d5](https://github.com/confluentinc/ksql/commit/6ff07d5))
* allow for decimals to be used as input types for UDFs ([#3217](https://github.com/confluentinc/ksql/pull/3217)) ([4a2e4b9](https://github.com/confluentinc/ksql/commit/4a2e4b9))
* enhance datagen for use as a load generator ([#3230](https://github.com/confluentinc/ksql/pull/3230)) ([ddb970b](https://github.com/confluentinc/ksql/commit/ddb970b))
* Extend UdfLoader to allow loading specific classes with UDFs ([#3234](https://github.com/confluentinc/ksql/pull/3234)) ([99c79b3](https://github.com/confluentinc/ksql/commit/99c79b3))
* format error messages better (MINOR) ([#3233](https://github.com/confluentinc/ksql/pull/3233)) ([c727d50](https://github.com/confluentinc/ksql/commit/c727d50))
* improved error message and updated error code for PrintTopics command ([#3246](https://github.com/confluentinc/ksql/pull/3246)) ([4b94f22](https://github.com/confluentinc/ksql/commit/4b94f22))
* some robustness improvements for Connect integration ([#3227](https://github.com/confluentinc/ksql/pull/3227)) ([bc1a2f8](https://github.com/confluentinc/ksql/commit/bc1a2f8))
* spin up a connect worker embedded inside the KSQL JVM ([#3241](https://github.com/confluentinc/ksql/pull/3241)) ([4d7ef2a](https://github.com/confluentinc/ksql/commit/4d7ef2a))
* validate createTopic permissions on SandboxedKafkaTopicClient ([#3250](https://github.com/confluentinc/ksql/pull/3250)) ([0ea157b](https://github.com/confluentinc/ksql/commit/0ea157b))
* **data-gen:** support KAFKA format in DataGen ([#3120](https://github.com/confluentinc/ksql/pull/3120)) ([cb7abcc](https://github.com/confluentinc/ksql/commit/cb7abcc))
* **ksql-connect:** poll connect-configs and auto register sources ([#3178](https://github.com/confluentinc/ksql/pull/3178)) ([6dd21fd](https://github.com/confluentinc/ksql/commit/6dd21fd))
* wrap timestamps in ROWTIME expressions with STRINGTOTIMESTAMP ([#3160](https://github.com/confluentinc/ksql/pull/3160)) ([42acd78](https://github.com/confluentinc/ksql/commit/42acd78))
* **ksql-connect:** introduce ConnectClient for REST requests ([#3137](https://github.com/confluentinc/ksql/pull/3137)) ([15548ce](https://github.com/confluentinc/ksql/commit/15548ce))
* **ksql-connect:** introduce syntax for CREATE CONNECTOR (syntax only) ([#3139](https://github.com/confluentinc/ksql/pull/3139)) ([e823659](https://github.com/confluentinc/ksql/commit/e823659))
* **ksql-connect:** wiring for creating connectors ([#3149](https://github.com/confluentinc/ksql/pull/3149)) ([cd20d57](https://github.com/confluentinc/ksql/commit/cd20d57))
* **udfs:** generic support for UDFs ([#3054](https://github.com/confluentinc/ksql/pull/3054)) ([a381c48](https://github.com/confluentinc/ksql/commit/a381c48))
* **cli:** improve CLI transient queries with headers and spacing (partial fix for [#892](https://github.com/confluentinc/ksql/issues/892)) ([#3047](https://github.com/confluentinc/ksql/pull/3047)) ([050b72a](https://github.com/confluentinc/ksql/commit/050b72a))
* **serde:** kafka format ([#3065](https://github.com/confluentinc/ksql/pull/3065)) ([2b5c3d1](https://github.com/confluentinc/ksql/commit/2b5c3d1))
* add basic support for key syntax ([#3034](https://github.com/confluentinc/ksql/pull/3034)) ([ca6478a](https://github.com/confluentinc/ksql/commit/ca6478a))
* Add REST and Websocket authorization hooks and interface ([#3000](https://github.com/confluentinc/ksql/pull/3000)) ([39af991](https://github.com/confluentinc/ksql/commit/39af991))
* Arrays should be indexable from the end too ([#3004](https://github.com/confluentinc/ksql/pull/3004)) ([a166075](https://github.com/confluentinc/ksql/commit/a166075)), closes [#2974](https://github.com/confluentinc/ksql/issues/2974)
* decimal math with other numbers ([#3001](https://github.com/confluentinc/ksql/pull/3001)) ([14d2bb7](https://github.com/confluentinc/ksql/commit/14d2bb7))
* New UNIX_TIMESTAMP and UNIX_DATE datetime functions ([#2459](https://github.com/confluentinc/ksql/pull/2459)) ([39ce7f4](https://github.com/confluentinc/ksql/commit/39ce7f4))



### Performance Improvements

* do not spam the logs with config defs ([#3044](https://github.com/confluentinc/ksql/pull/3044)) ([94904a3](https://github.com/confluentinc/ksql/commit/94904a3))
* Only look up index of new key field once, not per row processed ([#3020](https://github.com/confluentinc/ksql/pull/3020)) ([fda1c7f](https://github.com/confluentinc/ksql/commit/fda1c7f))
* Remove parsing of integer literals ([#3019](https://github.com/confluentinc/ksql/pull/3019)) ([6195b76](https://github.com/confluentinc/ksql/commit/6195b76))



### Bug Fixes

* `/query` rest endpoint should return valid JSON ([#3819](https://github.com/confluentinc/ksql/pull/3819)) ([b278e83](https://github.com/confluentinc/ksql/commit/b278e83))
* address upstream change in KafkaAvroDeserializer ([#3372](https://github.com/confluentinc/ksql/pull/3372)) ([b32e6a9](https://github.com/confluentinc/ksql/commit/b32e6a9))
* address upstream change in KafkaAvroDeserializer (revert previous fix) ([#3437](https://github.com/confluentinc/ksql/pull/3437)) ([bed164b](https://github.com/confluentinc/ksql/commit/bed164b))
* allow streams topic prefixed configs ([#3691](https://github.com/confluentinc/ksql/pull/3691)) ([939c45a](https://github.com/confluentinc/ksql/commit/939c45a)), closes [#817](https://github.com/confluentinc/ksql/issues/817)
* apply filter before flat mapping in logical planner ([#3730](https://github.com/confluentinc/ksql/pull/3730)) ([f4bd083](https://github.com/confluentinc/ksql/commit/f4bd083))
* band-aid around RestQueryTranslationTest ([#3326](https://github.com/confluentinc/ksql/pull/3326)) ([677e03c](https://github.com/confluentinc/ksql/commit/677e03c))
* be more lax on validating config ([#3599](https://github.com/confluentinc/ksql/pull/3599)) ([3c80cf1](https://github.com/confluentinc/ksql/commit/3c80cf1)), closes [#2279](https://github.com/confluentinc/ksql/issues/2279)
* better error message if tz invalid in datetime string ([#3449](https://github.com/confluentinc/ksql/pull/3449)) ([e93c445](https://github.com/confluentinc/ksql/commit/e93c445))
* better error message when users enter old style syntax for query ([#3397](https://github.com/confluentinc/ksql/pull/3397)) ([f948ec0](https://github.com/confluentinc/ksql/commit/f948ec0))
* changes required for compatibility with KIP-479 ([#3466](https://github.com/confluentinc/ksql/pull/3466)) ([567f056](https://github.com/confluentinc/ksql/commit/567f056))
* Created new test for running topology generation manually from the IDE, and removed main() from TopologyFileGenerator ([#3609](https://github.com/confluentinc/ksql/pull/3609)) ([381e563](https://github.com/confluentinc/ksql/commit/381e563))
* do not allow inserts into tables with null key ([#3605](https://github.com/confluentinc/ksql/pull/3605)) ([7e326b7](https://github.com/confluentinc/ksql/commit/7e326b7)), closes [#3021](https://github.com/confluentinc/ksql/issues/3021)
* do not allow WITH clause to be created with invalid WINDOW_SIZE ([#3432](https://github.com/confluentinc/ksql/pull/3432)) ([96bfc11](https://github.com/confluentinc/ksql/commit/96bfc11))
* Don't throw NPE on null columns ([#3647](https://github.com/confluentinc/ksql/pull/3647)) ([6969768](https://github.com/confluentinc/ksql/commit/6969768)), closes [#3617](https://github.com/confluentinc/ksql/issues/3617)
* drop `TopicDescriptionFactory` class ([#3528](https://github.com/confluentinc/ksql/pull/3528)) ([5281c74](https://github.com/confluentinc/ksql/commit/5281c74))
* ensure default server config works with IP6 (fixes [#3309](https://github.com/confluentinc/ksql/issues/3309)) ([#3310](https://github.com/confluentinc/ksql/pull/3310)) ([92b03ec](https://github.com/confluentinc/ksql/commit/92b03ec))
* error message when UDAF has STRUCT type with no schema ([#3407](https://github.com/confluentinc/ksql/pull/3407)) ([49f456e](https://github.com/confluentinc/ksql/commit/49f456e))
* fix Avro schema validation ([#3499](https://github.com/confluentinc/ksql/pull/3499)) ([a59954d](https://github.com/confluentinc/ksql/commit/a59954d))
* fix broken map coersion test case ([#3694](https://github.com/confluentinc/ksql/pull/3694)) ([b5ea24c](https://github.com/confluentinc/ksql/commit/b5ea24c))
* fix NPE when printing records with empty value (MINOR) ([#3470](https://github.com/confluentinc/ksql/pull/3470)) ([47313ff](https://github.com/confluentinc/ksql/commit/47313ff))
* fix parsing issue with LIST property overrides ([#3601](https://github.com/confluentinc/ksql/pull/3601)) ([6459fa1](https://github.com/confluentinc/ksql/commit/6459fa1))
* fix test for KIP-307 final PR ([#3402](https://github.com/confluentinc/ksql/pull/3402)) ([d77db50](https://github.com/confluentinc/ksql/commit/d77db50))
* improve error message on query or print statement on /ksql ([#3337](https://github.com/confluentinc/ksql/pull/3337)) ([dae28eb](https://github.com/confluentinc/ksql/commit/dae28eb))
* improve print topic error message when topic does not exist ([#3464](https://github.com/confluentinc/ksql/pull/3464)) ([0fa4d24](https://github.com/confluentinc/ksql/commit/0fa4d24))
* include lower-case identifiers among those that need quotes ([#3723](https://github.com/confluentinc/ksql/pull/3723)) ([62c47bf](https://github.com/confluentinc/ksql/commit/62c47bf))
* make sure use of non threadsafe UdfIndex is synchronized ([#3486](https://github.com/confluentinc/ksql/pull/3486)) ([618aae8](https://github.com/confluentinc/ksql/commit/618aae8))
* pull queries available on `/query` rest & ws endpoint ([#3820](https://github.com/confluentinc/ksql/pull/3820)) ([9a47eaf](https://github.com/confluentinc/ksql/commit/9a47eaf)), closes [#3672](https://github.com/confluentinc/ksql/issues/3672) [#3495](https://github.com/confluentinc/ksql/issues/3495)
* quoted identifiers for source names ([#3695](https://github.com/confluentinc/ksql/pull/3695)) ([7d3cf92](https://github.com/confluentinc/ksql/commit/7d3cf92))
* race condition in KsStateStore ([#3474](https://github.com/confluentinc/ksql/pull/3474)) ([7336389](https://github.com/confluentinc/ksql/commit/7336389))
* Remove dependencies on test-jars from ksql-functional-tests jar ([#3421](https://github.com/confluentinc/ksql/pull/3421)) ([e09d6ad](https://github.com/confluentinc/ksql/commit/e09d6ad))
* Remove duplicate ksql-common dependency in ksql-streams pom ([#3703](https://github.com/confluentinc/ksql/pull/3703)) ([0620906](https://github.com/confluentinc/ksql/commit/0620906))
* Remove unnecessary arg coercion for UDFs ([#3595](https://github.com/confluentinc/ksql/pull/3595)) ([4c42530](https://github.com/confluentinc/ksql/commit/4c42530))
* Rename Delimiter:parse(char c) to Delimiter.of(char c) ([#3433](https://github.com/confluentinc/ksql/pull/3433)) ([8716c41](https://github.com/confluentinc/ksql/commit/8716c41))
* renamed method to avoid checkstyle error ([#3652](https://github.com/confluentinc/ksql/pull/3652)) ([a8a3588](https://github.com/confluentinc/ksql/commit/a8a3588))
* revert ipv6 address and update docs ([#3314](https://github.com/confluentinc/ksql/pull/3314)) ([0ff4a51](https://github.com/confluentinc/ksql/commit/0ff4a51))
* Revert named stores in expected topologies, disable naming stores from StreamJoined, re-enable join tests. ([#3550](https://github.com/confluentinc/ksql/pull/3550)) ([0b8ccc1](https://github.com/confluentinc/ksql/commit/0b8ccc1)), closes [#3364](https://github.com/confluentinc/ksql/issues/3364)
* should be able to parse empty STRUCT schema (MINOR) ([#3318](https://github.com/confluentinc/ksql/pull/3318)) ([a6549e1](https://github.com/confluentinc/ksql/commit/a6549e1))
* Some renaming around KsqlFunction etc ([#3747](https://github.com/confluentinc/ksql/pull/3747)) ([b30d965](https://github.com/confluentinc/ksql/commit/b30d965))
* standardize KSQL up-casting ([#3516](https://github.com/confluentinc/ksql/pull/3516)) ([7fe8772](https://github.com/confluentinc/ksql/commit/7fe8772))
* support NULL return values from CASE statements ([#3531](https://github.com/confluentinc/ksql/pull/3531)) ([eb9e41b](https://github.com/confluentinc/ksql/commit/eb9e41b))
* support UDAFs with different intermediate schema ([#3412](https://github.com/confluentinc/ksql/pull/3412)) ([70e10e9](https://github.com/confluentinc/ksql/commit/70e10e9))
* switch AdminClient to be sandbox proxy ([#3351](https://github.com/confluentinc/ksql/pull/3351)) ([6747d5c](https://github.com/confluentinc/ksql/commit/6747d5c))
* typo in static query WHERE clause example ([#3423](https://github.com/confluentinc/ksql/pull/3423)) ([7ad3248](https://github.com/confluentinc/ksql/commit/7ad3248))
* Update repartition processor names for KAFKA-9098 ([#3802](https://github.com/confluentinc/ksql/pull/3802)) ([2b86cd8](https://github.com/confluentinc/ksql/commit/2b86cd8))
* Use the correct Immutable interface ([#3488](https://github.com/confluentinc/ksql/pull/3488)) ([a1096bf](https://github.com/confluentinc/ksql/commit/a1096bf))
* **3356:** struct rewritter missed EXPLAIN ([#3398](https://github.com/confluentinc/ksql/pull/3398)) ([daf974b](https://github.com/confluentinc/ksql/commit/daf974b))
* **3441:** stabilize the StaticQueryFunctionalTest ([#3442](https://github.com/confluentinc/ksql/pull/3442)) ([44ae3a0](https://github.com/confluentinc/ksql/commit/44ae3a0)), closes [#3441](https://github.com/confluentinc/ksql/issues/3441)
* **3524:** improve pull query error message ([#3540](https://github.com/confluentinc/ksql/pull/3540)) ([2be8385](https://github.com/confluentinc/ksql/commit/2be8385)), closes [#3524](https://github.com/confluentinc/ksql/issues/3524)
* **3525:** sET should only affect statements after it ([#3529](https://github.com/confluentinc/ksql/pull/3529)) ([5315f1e](https://github.com/confluentinc/ksql/commit/5315f1e)), closes [#3525](https://github.com/confluentinc/ksql/issues/3525)
* address deprecation of getAdminClient ([#3276](https://github.com/confluentinc/ksql/pull/3276)) ([6a50fca](https://github.com/confluentinc/ksql/commit/6a50fca))
* error message with DROP DELETE TOPIC is invalid ([#3279](https://github.com/confluentinc/ksql/pull/3279)) ([4284b8c](https://github.com/confluentinc/ksql/commit/4284b8c))
* find bugs issue in KafkaTopicClientImpl ([#3268](https://github.com/confluentinc/ksql/pull/3268)) ([70e880f](https://github.com/confluentinc/ksql/commit/70e880f))
* fixed how wrapped KsqlTopicAuthorizationException error messages are displayed ([#3258](https://github.com/confluentinc/ksql/pull/3258)) ([63672ae](https://github.com/confluentinc/ksql/commit/63672ae))
* improve escaping of identifiers ([#3295](https://github.com/confluentinc/ksql/pull/3295)) ([04435d7](https://github.com/confluentinc/ksql/commit/04435d7))
* respect reserved words in more clauses for SqlFormatter (MINOR) ([#3284](https://github.com/confluentinc/ksql/pull/3284)) ([6974a80](https://github.com/confluentinc/ksql/commit/6974a80))
* schema converters should handle List and Map impl ([#3290](https://github.com/confluentinc/ksql/pull/3290)) ([af779dc](https://github.com/confluentinc/ksql/commit/af779dc))
* `COLLECT_LIST` can now be applied to tables ([#3104](https://github.com/confluentinc/ksql/pull/3104)) ([c239785](https://github.com/confluentinc/ksql/commit/c239785))
* add ksql-functional-tests to the ksql package ([#3111](https://github.com/confluentinc/ksql/pull/3111)) ([9548135](https://github.com/confluentinc/ksql/commit/9548135))
* authorization filter is logging incorrect username ([#3138](https://github.com/confluentinc/ksql/pull/3138)) ([b15c6d0](https://github.com/confluentinc/ksql/commit/b15c6d0))
* broken build due to bad import statements ([#3204](https://github.com/confluentinc/ksql/pull/3204)) ([8ec4c2b](https://github.com/confluentinc/ksql/commit/8ec4c2b))
* check for other sources using a topic before deleting ([#3070](https://github.com/confluentinc/ksql/pull/3070)) ([b3fa315](https://github.com/confluentinc/ksql/commit/b3fa315))
* default timestamp extractor override is not working ([#3176](https://github.com/confluentinc/ksql/pull/3176)) ([d1db07b](https://github.com/confluentinc/ksql/commit/d1db07b))
* drop succeeds even if missing topic or schema ([#3131](https://github.com/confluentinc/ksql/pull/3131)) ([ba03d6f](https://github.com/confluentinc/ksql/commit/ba03d6f))
* dummy placeholder class in ksql-execution ([#3142](https://github.com/confluentinc/ksql/pull/3142)) ([c9f1cbb](https://github.com/confluentinc/ksql/commit/c9f1cbb))
* expose user/password command line options ([#3129](https://github.com/confluentinc/ksql/pull/3129)) ([1fd70fa](https://github.com/confluentinc/ksql/commit/1fd70fa))
* filter null entries before creating KafkaConfigStore ([#3147](https://github.com/confluentinc/ksql/pull/3147)) ([2852af1](https://github.com/confluentinc/ksql/commit/2852af1))
* fix auth error message with insert values command ([#3257](https://github.com/confluentinc/ksql/pull/3257)) ([abe410a](https://github.com/confluentinc/ksql/commit/abe410a))
* Implement new KIP-455 AdminClient AlterPartitionReassignments and tPartitionReassignments APIs ([#3218](https://github.com/confluentinc/ksql/pull/3218)) ([d951026](https://github.com/confluentinc/ksql/commit/d951026))
* incorrect SR authorization message is displayed ([#3186](https://github.com/confluentinc/ksql/pull/3186)) ([b3b6c82](https://github.com/confluentinc/ksql/commit/b3b6c82))
* logicalSchema toString() to include key fields (MINOR) ([#3123](https://github.com/confluentinc/ksql/pull/3123)) ([0984529](https://github.com/confluentinc/ksql/commit/0984529))
* Remove delete.topic.enable check ([#3089](https://github.com/confluentinc/ksql/pull/3089)) ([71ec1c0](https://github.com/confluentinc/ksql/commit/71ec1c0))
* remove non-standard JavaFx usage of Pair ([#3145](https://github.com/confluentinc/ksql/pull/3145)) ([3508847](https://github.com/confluentinc/ksql/commit/3508847))
* replace nashorn @Immutable with errorprone for JDK12 (MINOR) ([#3239](https://github.com/confluentinc/ksql/pull/3239)) ([0c47a34](https://github.com/confluentinc/ksql/commit/0c47a34))
* request / on makeRootRequest instead of /info ([#3197](https://github.com/confluentinc/ksql/pull/3197)) ([7935488](https://github.com/confluentinc/ksql/commit/7935488))
* the QTTs now run through SqlFormatter & other formatting fixes ([#3222](https://github.com/confluentinc/ksql/pull/3222)) ([79da68c](https://github.com/confluentinc/ksql/commit/79da68c))
* use errorprone Immutable annotation instead of nashorn version ([#3150](https://github.com/confluentinc/ksql/pull/3150)) ([e7f5e17](https://github.com/confluentinc/ksql/commit/e7f5e17))
* `DESCRIBE` now works for sources with decimal types ([#3083](https://github.com/confluentinc/ksql/pull/3083)) ([0eaa101](https://github.com/confluentinc/ksql/commit/0eaa101))
* don't log out to stderr on parser errors ([#3052](https://github.com/confluentinc/ksql/pull/3052)) ([29dea47](https://github.com/confluentinc/ksql/commit/29dea47))
* drop describe topic functionality (MINOR) ([#3072](https://github.com/confluentinc/ksql/pull/3072)) ([1290b82](https://github.com/confluentinc/ksql/commit/1290b82))
* ensure topology generator test runs in build ([#3067](https://github.com/confluentinc/ksql/pull/3067)) ([3168150](https://github.com/confluentinc/ksql/commit/3168150))
* misplaced commas when formatting CTAS statements ([#3058](https://github.com/confluentinc/ksql/pull/3058)) ([c05615d](https://github.com/confluentinc/ksql/commit/c05615d))
* remove any rowtime or rowkey columns from query schema (MINOR) (Fixes 3039) ([#3043](https://github.com/confluentinc/ksql/pull/3043)) ([0346933](https://github.com/confluentinc/ksql/commit/0346933))
* remove last of registered topics stuff from api / cli (MINOR) ([#3068](https://github.com/confluentinc/ksql/pull/3068)) ([24d874c](https://github.com/confluentinc/ksql/commit/24d874c))
* sqlformatter to correctly handle describe ([#3074](https://github.com/confluentinc/ksql/pull/3074)) ([8de57bd](https://github.com/confluentinc/ksql/commit/8de57bd))



### BREAKING CHANGES

* Introduced [`EMIT CHANGES`](https://docs.ksqldb.io/en/latest/operate-and-deploy/ksql-vs-ksqldb/#syntax) syntax to differentiate [push queries](https://docs.ksqldb.io/en/latest/concepts/queries/push/) from new [pull queries](https://docs.ksqldb.io/en/latest/concepts/queries/pull/). Persistent push queries do not yet require an `EMIT CHANGES` clause, but transient push queries do.
* the response from the RESTful API for push queries has changed: it is now a valid JSON document containing a JSON array, where each element is JSON object containing either a row of data, an error message, or a final message.  The `terminal` field has been removed.
* the response from the RESTful API for push queries has changed: it now returns a line with the schema and query id in a `header` field and null fields are not included in the payload.
The CLI is backwards compatible with older versions of the server, though it won't output column headings from older versions.
* If users are relying on the previous behaviour of uppercasing topic names, this change breaks that
* If no value is passed for the KSQL datagen option `iterations`, datagen will now produce indefinitely, rather than terminating after a default of 1,000,000 rows.
* Previously CLI startup permissions were based on whether the user has access to /info, but now it's based on whether the user has access to /. This change implies that if a user has permissions to / but not /info, they now have access to the CLI whereas previously they did not.
* "SHOW TOPICS" no longer includes the "Consumers" and
"ConsumerGroups" columns. You can use "SHOW TOPICS EXTENDED" to get the
output previous emitted from "SHOW TOPICS". See below for examples.
This change splits "SHOW TOPICS" into two commands:
  1. "SHOW TOPICS EXTENDED", which shows what was previously shown by
  "SHOW TOPICS". Sample output:
      ```
      ksql> show topics extended;
      Kafka Topic                                                                                   | Partitions | Partition Replicas | Consumers | ConsumerGroups
      --------------------------------------------------------------------------------------------------------------------------------------------------------------
      _confluent-command                                                                            | 1          | 1                  | 1         | 1
      _confluent-controlcenter-5-3-0-1-actual-group-consumption-rekey                               | 1          | 1                  | 1         | 1
      ```
  2. "SHOW TOPICS", which now no longer queries consumer groups and their
  active consumers. Sample output:
      ```
      ksql> show topics;
      Kafka Topic                                                                                   | Partitions | Partition Replicas
      ---------------------------------------------------------------------------------------------------------------------------------
      _confluent-command                                                                            | 1          | 1
      _confluent-controlcenter-5-3-0-1-actual-group-consumption-rekey                               | 1          | 1
      ```




## Earlier releases

Release notes for KSQL releases prior to ksqlDB v0.6.0 can be found at
[docs/changelog.rst](https://github.com/confluentinc/ksql/blob/5.4.1-post/docs/changelog.rst).
