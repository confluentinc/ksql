# Change Log

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



### Performance Improvements

* do not spam the logs with config defs ([#3044](https://github.com/confluentinc/ksql/pull/3044)) ([94904a3](https://github.com/confluentinc/ksql/commit/94904a3))
* Only look up index of new key field once, not per row processed ([#3020](https://github.com/confluentinc/ksql/pull/3020)) ([fda1c7f](https://github.com/confluentinc/ksql/commit/fda1c7f))
* Remove parsing of integer literals ([#3019](https://github.com/confluentinc/ksql/pull/3019)) ([6195b76](https://github.com/confluentinc/ksql/commit/6195b76))



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

Note: Release notes for releases prior to ksqlDB v0.6.0 can be found at [docs/changelog.rst](docs/changelog.rst).

