# Change Log

## [0.24.0](https://github.com/confluentinc/ksql/releases/tag/v0.24.0) (2022-02-11)

### Features

- new CLI params to provide credentials for cloud connector management ([#8684](https://github.com/confluentinc/ksql/pull/8684)) ([5e5af50](https://github.com/confluentinc/ksql/commit/5e5af50b3fe1ee34f6e3e88fb43b700af161e663))
- [UIF-1113] Add weighting to search results based on domain ([#8565](https://github.com/confluentinc/ksql/pull/8565)) ([f505f31](https://github.com/confluentinc/ksql/commit/f505f31d7d13a4b8c10fa28d7d3ca80e31d81c3c))
- Add int/bigint/double conversion functions from bytes ([#8426](https://github.com/confluentinc/ksql/pull/8426)) ([da77c8a](https://github.com/confluentinc/ksql/commit/da77c8a339e6d4d08e547b546960b98a783deffd))
- Add is_json_string UDF ([#8600](https://github.com/confluentinc/ksql/pull/8600)) ([c12a745](https://github.com/confluentinc/ksql/commit/c12a7459b05216f2ca61a1f8fb774f38a82a149f))
- Add json_array_length UDF ([#8602](https://github.com/confluentinc/ksql/pull/8602)) ([f7c1334](https://github.com/confluentinc/ksql/commit/f7c1334a8b97f52db60f2e2ca84230c31c7e9da0))
- add json_keys UDF ([#8603](https://github.com/confluentinc/ksql/pull/8603)) ([bb78fcd](https://github.com/confluentinc/ksql/commit/bb78fcd41971a97db142dcdfb5394c70bbde1362))
- Add json_records, to_json_string, and json_concat UDFs ([#8632](https://github.com/confluentinc/ksql/pull/8632)) ([3b6d828](https://github.com/confluentinc/ksql/commit/3b6d8286e7ef12f71bc29bd75e6c9112ce3cb5ba))
- add max task usage and num stateful tasks metrics ([#8549](https://github.com/confluentinc/ksql/pull/8549)) ([ba27e3a](https://github.com/confluentinc/ksql/commit/ba27e3ab54c359c920b0a4f6d5b587250476aab7))
- add support for connect specific https configs ([#8553](https://github.com/confluentinc/ksql/pull/8553)) ([37507ab](https://github.com/confluentinc/ksql/commit/37507ab86542de3352324e3cf341641ef4708b96))
- Adds support to /query-stream for http1.1 and StreamedRow json format ([#8449](https://github.com/confluentinc/ksql/pull/8449)) ([dd61db3](https://github.com/confluentinc/ksql/commit/dd61db30e33ecaa433acc849438a809c6b54c46d))
- allow custom connect auth header configuration ([#8635](https://github.com/confluentinc/ksql/pull/8635)) ([aebec54](https://github.com/confluentinc/ksql/commit/aebec546fe31e7be0e6b105fbecf9665c12358d2))
- Allow to plug-in custom error handling for Connect server errors ([#8480](https://github.com/confluentinc/ksql/pull/8480)) ([c4b4d67](https://github.com/confluentinc/ksql/commit/c4b4d67ed993b9bab0b5cfe4b1d705bfd62060e7))
- Change API to return Position from Streams ([#8590](https://github.com/confluentinc/ksql/pull/8590)) ([b2b6a73](https://github.com/confluentinc/ksql/commit/b2b6a73c5100f7ab90a3185059a5b4cf1665ee3a))
- Continuation tokens and ALOS for SPQs ([#8342](https://github.com/confluentinc/ksql/pull/8342)) ([16c2820](https://github.com/confluentinc/ksql/commit/16c2820bfa00880d796460460633b53350d97654))
- custom auth configs for ksql connector requests ([#8476](https://github.com/confluentinc/ksql/pull/8476)) ([7a9a61a](https://github.com/confluentinc/ksql/commit/7a9a61a64eb7b918e714d0c2df5ad9b1d1b3551b))
- don't book keep sources in runtime as well ([#8532](https://github.com/confluentinc/ksql/pull/8532)) ([110acc7](https://github.com/confluentinc/ksql/commit/110acc784e87ca038630599ffab1f4f8713b5244))
- support custom headers for connector requests ([#8357](https://github.com/confluentinc/ksql/pull/8357)) ([97fa117](https://github.com/confluentinc/ksql/commit/97fa117ec33d42e145518568cf4a441f0bd8cb33))
- un-synchronize PullQueryQueue row queuing when limit absent ([#8563](https://github.com/confluentinc/ksql/pull/8563)) ([c7793a8](https://github.com/confluentinc/ksql/commit/c7793a8677d0648c250376485e6979eb9a43eb5c))
- Use IQv2 when executing range and table scan pull queries ([#8556](https://github.com/confluentinc/ksql/pull/8556)) ([6eda7a4](https://github.com/confluentinc/ksql/commit/6eda7a4bd63f5c573bab1fb69a8c2cdc1315f9cc))
- Validate connector config before creating it ([#8445](https://github.com/confluentinc/ksql/pull/8445)) ([62c8021](https://github.com/confluentinc/ksql/commit/62c8021d7df10cbe4f8ca22d55b0429edb3de981))
- pull query LIMIT clause ([#8333](https://github.com/confluentinc/ksql/pull/8333)) ([5c9be20](https://github.com/confluentinc/ksql/commit/5c9be20917f1f88b0587310134cc3b48b396e345))
- expose Kafka headers to ksqlDB ([#8350](https://github.com/confluentinc/ksql/pull/8350),[#8366](https://github.com/confluentinc/ksql/pull/8366),[#8416](https://github.com/confluentinc/ksql/pull/8416),[#8417](https://github.com/confluentinc/ksql/pull/8417),[#8475](https://github.com/confluentinc/ksql/pull/8475),[#8496](https://github.com/confluentinc/ksql/pull/8496),[#8516](https://github.com/confluentinc/ksql/pull/8516)) ([db76b3e](https://github.com/confluentinc/ksql/commit/db76b3ec11c97edf7ba870a073e5805c340b9ec6),[12dbdad](https://github.com/confluentinc/ksql/commit/12dbdad94816659b781f0d7b1ddc345a98b30c36),[e030f10](https://github.com/confluentinc/ksql/commit/e030f106b76338d13675d68169936ce891e87594),[0239a95](https://github.com/confluentinc/ksql/commit/0239a95e96c4f08d33dac4924eeefbdbd25d86b6),[065de82](https://github.com/confluentinc/ksql/commit/065de82707197155ea12acdff3560eccee801773),[bd452aa](https://github.com/confluentinc/ksql/commit/bd452aa029942b7d6d2dc234f992dba3d560e8c3),[b18fb09](https://github.com/confluentinc/ksql/commit/b18fb090de721fff2b63979cb559cf0a8312ad1b))
- Allow schema id in source creation ([#8441](https://github.com/confluentinc/ksql/pull/8441),[#8421](https://github.com/confluentinc/ksql/pull/8421),[#8411](https://github.com/confluentinc/ksql/pull/8411),[#8401](https://github.com/confluentinc/ksql/pull/8401),[#8185](https://github.com/confluentinc/ksql/pull/8185),[#8572](https://github.com/confluentinc/ksql/pull/8572),[a60bb21](https://github.com/confluentinc/ksql/commit/a60bb21ac2daae78788143ab7c42185ece2292c2),[b490204](https://github.com/confluentinc/ksql/commit/b490204eefcdbf3fbdf8ae34a16beaa9c229ec1b),[8a11ac2](https://github.com/confluentinc/ksql/commit/8a11ac218923ab7617ec7853385c52034a4662e4),[8aa55b1](https://github.com/confluentinc/ksql/commit/8aa55b175d891fee06ad40d43f038c58b02285f9),[7adc5cb](https://github.com/confluentinc/ksql/commit/7adc5cb85f24b3d3f90c6ab548266d6021c665e0))

### Bug Fixes

- fix create connector config handling for name config ([d51c6db](https://github.com/confluentinc/ksql/commit/d51c6dbca31a199ce4ee434e479a8fb4395ff10b))
- 404 for /topics connect endpoint logs a warn instead of showing up in CLI ([#8462](https://github.com/confluentinc/ksql/pull/8462)) ([d722743](https://github.com/confluentinc/ksql/commit/d722743fdca952f3c288a3122038a521845ad852))
- add GRACE and PERIOD to the nonReserved and automate testing ([#8596](https://github.com/confluentinc/ksql/pull/8596)) ([b975086](https://github.com/confluentinc/ksql/commit/b975086d1b89a885a128bb87d6fa4eaa7545af50))
- Allow disabling Scalable Push Query ALOS to be set via config ([#8542](https://github.com/confluentinc/ksql/pull/8542)) ([7ac36e0](https://github.com/confluentinc/ksql/commit/7ac36e00ff5d5593894a28e26b296089c59e634e))
- Consider topics created by join operations internal ([#8520](https://github.com/confluentinc/ksql/pull/8520)) ([ebe0c49](https://github.com/confluentinc/ksql/commit/ebe0c494c912e9977a95b421e9347c93f656f2ce))
- disallow negative integers in pull query limit clause ([#8491](https://github.com/confluentinc/ksql/pull/8491)) ([3b5d91b](https://github.com/confluentinc/ksql/commit/3b5d91b3448d07833c60f1c474b962807d735b10))
- disallow persistent query limit clause ([#8506](https://github.com/confluentinc/ksql/pull/8506)) ([60e9efa](https://github.com/confluentinc/ksql/commit/60e9efae8883a0cb692c399958b86a31a0fe87cf))
- explicitly set confluent log4j version to 1.2.17-cp5 ([#8497](https://github.com/confluentinc/ksql/pull/8497)) ([b2470f3](https://github.com/confluentinc/ksql/commit/b2470f3259a3f8c99dc580a24bd0523477dc7193))
- Fix DROP STREAM IF EXISTS DELETE TOPIC ([#8559](https://github.com/confluentinc/ksql/pull/8559)) ([ab09de6](https://github.com/confluentinc/ksql/commit/ab09de663eeaed2157835333edda1fa86622cd60))
- Fix some typos ([#8518](https://github.com/confluentinc/ksql/pull/8518)) ([795629f](https://github.com/confluentinc/ksql/commit/795629f502223eaabeee457f2e9feaa729557fbe))
- Fixes an NPE which was causing terminate to fail ([#8593](https://github.com/confluentinc/ksql/pull/8593)) ([796267a](https://github.com/confluentinc/ksql/commit/796267a69eb5e4f61d5a274c564976516e6968c6))
- Fixes the AllHostsLocatorTest ([#8664](https://github.com/confluentinc/ksql/pull/8664)) ([802f130](https://github.com/confluentinc/ksql/commit/802f1304a2417a926c3460fa3fa1bcee8c42ab74))
- invoke per-query listeners upon state change in shared runtime ([#8509](https://github.com/confluentinc/ksql/pull/8509)) ([998cc15](https://github.com/confluentinc/ksql/commit/998cc15db4d840a3553641eec41c279505ea40a2))
- make adding to pull query queue threadsafe ([#8544](https://github.com/confluentinc/ksql/pull/8544)) ([6454ef1](https://github.com/confluentinc/ksql/commit/6454ef16032674fcdd015ed27ac13214368dc152))
- make HEADER and HEADERS non-reserved ([#8490](https://github.com/confluentinc/ksql/pull/8490)) ([c8c4438](https://github.com/confluentinc/ksql/commit/c8c4438336bc03efc9ac1f48f627169ef001d2b6))
- Make JaasPrincipal public ([#8567](https://github.com/confluentinc/ksql/pull/8567)) ([8da7fe5](https://github.com/confluentinc/ksql/commit/8da7fe5e2346f611f1b38ec279e6f7d68e5aed28))
- make PullQueryConsistencyFunctionalTest more reliable ([#8530](https://github.com/confluentinc/ksql/pull/8530)) ([482880a](https://github.com/confluentinc/ksql/commit/482880a1d1a66a5df275fac14e7cfdbf69827e2f))
- make PullQueryLimitHARoutingTest more stable ([#8448](https://github.com/confluentinc/ksql/pull/8448)) ([85e5d29](https://github.com/confluentinc/ksql/commit/85e5d295a9cbdb8623caa8cd473cb8f031bfaf70))
- make tutorial work ([#8625](https://github.com/confluentinc/ksql/pull/8625)) ([52b6d67](https://github.com/confluentinc/ksql/commit/52b6d67d2a7af44e28045a981ab92e409c09eb10))
- maybe fix flaky test ([#8457](https://github.com/confluentinc/ksql/pull/8457)) ([acd80ef](https://github.com/confluentinc/ksql/commit/acd80efd698865ed87d4b2b0787cbd2da05d1d4d))
- Prevents internal http2 requests from having shared connections closed ([#8507](https://github.com/confluentinc/ksql/pull/8507)) ([5b0be58](https://github.com/confluentinc/ksql/commit/5b0be58dc852c3bd248bddf3c9036976f1b8e8c8))
- pull queries are supported on source streams now. ([#8597](https://github.com/confluentinc/ksql/pull/8597)) ([9e6ec15](https://github.com/confluentinc/ksql/commit/9e6ec15042c4b5f3464a77231752d6611330d444))
- refactoring properties replacement before restarting runtime ([#8510](https://github.com/confluentinc/ksql/pull/8510)) ([be89c60](https://github.com/confluentinc/ksql/commit/be89c60d78a937b23addf35d061554a182c41b75))
- remove unnecessary import ([#8451](https://github.com/confluentinc/ksql/pull/8451)) ([33ef63f](https://github.com/confluentinc/ksql/commit/33ef63f9d7a4de8a635c9560791f87f69255a5e1))
- remove usages of internal Streams API/variable that was removed upstream ([#8660](https://github.com/confluentinc/ksql/pull/8660)) ([9615a02](https://github.com/confluentinc/ksql/commit/9615a0296bfacd6eea9f054958319987792865f4))
- SandboxKafkaTopicClient should use default replication factor if applicable ([#8551](https://github.com/confluentinc/ksql/pull/8551)) ([5c8c186](https://github.com/confluentinc/ksql/commit/5c8c1863e83efd56e6d3b6c1bb180d02ab5e94d9))
- Streams overrides bugfix ([#8514](https://github.com/confluentinc/ksql/pull/8514)) ([366af1f](https://github.com/confluentinc/ksql/commit/366af1fe7ca5a7de0fe2a62a7b7c18c880601621))
- typo in TimestampExtractionPolicyFactory ([#8539](https://github.com/confluentinc/ksql/pull/8539)) ([ce1e9b2](https://github.com/confluentinc/ksql/commit/ce1e9b2908ab9be7589fccf2a0e7402df3edfb11))
- variable substitution with CREATE CONNECTOR in migrations tool ([#8547](https://github.com/confluentinc/ksql/pull/8547)) ([baa9422](https://github.com/confluentinc/ksql/commit/baa9422bd2cf1e683c029641a88e96f7ab69f8c6))
- allow valid @ character on JSON extract functions ([#8422](https://github.com/confluentinc/ksql/pull/8422)) ([290ff98](https://github.com/confluentinc/ksql/commit/290ff982527d46455df3e4ac0a73fe6edee157e0))
- fix broken test ([#8439](https://github.com/confluentinc/ksql/pull/8439)) ([6b15250](https://github.com/confluentinc/ksql/commit/6b15250f8f012022945b26ed5eade85dd7674dbc))
- Fixes flaky ScalablePushBandwidthThrottleIntegrationTest caused by timeout ([#8410](https://github.com/confluentinc/ksql/pull/8410)) ([fbbc076](https://github.com/confluentinc/ksql/commit/fbbc076a9e2471744ae47e96511ea4cf09c52540))
- make to_bytes work with lowercase hex ([#8423](https://github.com/confluentinc/ksql/pull/8423)) ([fee904e](https://github.com/confluentinc/ksql/commit/fee904e07802195cd64432454d5e05dd6fb87099))
- tolerate out-of-order execution on HARouting tests ([#8415](https://github.com/confluentinc/ksql/pull/8415)) ([c4cc6d5](https://github.com/confluentinc/ksql/commit/c4cc6d5c58dcbc82d2259192cea70eb2ae65ffba))
- fix warning ([#8397](https://github.com/confluentinc/ksql/pull/8397)) ([66c96f4](https://github.com/confluentinc/ksql/commit/66c96f4424f97464d460851ba4dc767f36c24c25))
- synchronizing writes to localcommand file ([#8406](https://github.com/confluentinc/ksql/pull/8406)) ([ed50b05](https://github.com/confluentinc/ksql/commit/ed50b05ff7789df2afc7e651b8a3c61ffeefed5a))
- Fixes a timestamp bug with Scalable Push Queries ([#8377](https://github.com/confluentinc/ksql/pull/8377)) ([507bdb5](https://github.com/confluentinc/ksql/commit/507bdb5208697473206ccf278e9bc17c8ee1346d))
- return error message if table functions are used inside a CASE ([#8327](https://github.com/confluentinc/ksql/pull/8327)) ([f0b9e96](https://github.com/confluentinc/ksql/commit/f0b9e962d23ef8ae225e50bd5c7012158428dd2e))
- upgrade Netty for 5.5.x ([#8389](https://github.com/confluentinc/ksql/pull/8389)) ([e95ff38](https://github.com/confluentinc/ksql/commit/e95ff381b2ea57e4248f3ad39ed67b97fa1ad833))
- Makes PullQueryRoutingFunctionalTest more reliable and adds back test ([#8351](https://github.com/confluentinc/ksql/pull/8351)) ([152eea8](https://github.com/confluentinc/ksql/commit/152eea8c0b711473c438d282d885c72bf95e4f1b))
- Makes tests wait only 500ms for consumer group cleanup ([#8367](https://github.com/confluentinc/ksql/pull/8367)) ([1a2f7b2](https://github.com/confluentinc/ksql/commit/1a2f7b27a2dff93b2dff4d22b7873e80d286d451))
- Pull Query Key Extraction Optimizations ([#8346](https://github.com/confluentinc/ksql/pull/8346)) ([6ad333e](https://github.com/confluentinc/ksql/commit/6ad333ef6d6613155a5b4848caafcbbdce6a8a58))
- update client to fix build ([#8369](https://github.com/confluentinc/ksql/pull/8369)) ([9ad563d](https://github.com/confluentinc/ksql/commit/9ad563d3b81d2bb50d89a46059e793436f02b566))
- port changes from [#7863](https://github.com/confluentinc/ksql/issues/7863) to 5.4.x ([#8409](https://github.com/confluentinc/ksql/pull/8409)) ([25c9ca5](https://github.com/confluentinc/ksql/commit/25c9ca5f1f59136f574b261be43eb09947af264b))

### BREAKING CHANGES

- When creating connectors through ksqlDB, ksqlDB no longer automatically sets the `key.converter` config to be the `StringConverter`, because ksqlDB has supported key types other than strings for many releases now.

### Reverts

- Revert "Bump Confluent to 7.2.0-0, Kafka to 7.2.0-0" ([b6e795f](https://github.com/confluentinc/ksql/commit/b6e795f95a74a1ec3096fc8e7c941072bd47c411))

## [0.23.1](https://github.com/confluentinc/ksql/releases/tag/v0.23.1-rc9) (2021-12-14)

### Features

- [UIF-1010] Add swiftype metatag for site of ksqldb ([#8308](https://github.com/confluentinc/ksql/pull/8308)) ([916f38b](https://github.com/confluentinc/ksql/commit/916f38b1e34f8b1108923f1edb5cf59adf1c2d85))
- Add consistency vector handling to CLI and Java client ([#8264](https://github.com/confluentinc/ksql/pull/8264)) ([a651677](https://github.com/confluentinc/ksql/commit/a651677e95f888f0436e9daa40e780ecc9f66884))
- add detailed scalable push query metrics with type breakdown ([#8178](https://github.com/confluentinc/ksql/pull/8178)) ([561af53](https://github.com/confluentinc/ksql/commit/561af5335a6178fd12fdd78f5728b9dff0c7890f))
- Add metrics for stream pull queries to WS ([#8174](https://github.com/confluentinc/ksql/pull/8174)) ([d75f254](https://github.com/confluentinc/ksql/commit/d75f25466ded174f7ae894db919c445c6a51de3a))
- Add support for TIMESTAMP type in the WITH/TIMESTAMP property ([#8271](https://github.com/confluentinc/ksql/pull/8271)) ([ecb43e2](https://github.com/confluentinc/ksql/commit/ecb43e2ffb8450b3902968a2709c86898291dbc7))
- enable ROWPARTITION and ROWOFFSET pseudo columns (KLIP-50) ([#8245](https://github.com/confluentinc/ksql/pull/8245)) ([7bdc41d](https://github.com/confluentinc/ksql/commit/7bdc41d5e05910ce9778abc25246c5021dc290f8))
- Re-enable GRACE period with new stream-stream joins semantics ([#8236](https://github.com/confluentinc/ksql/pull/8236)) ([f640f5e](https://github.com/confluentinc/ksql/commit/f640f5e40db333f3559cd540e4afe327aa84fc5d)), closes [#8020](https://github.com/confluentinc/ksql/issues/8020) [#8027](https://github.com/confluentinc/ksql/issues/8027) [#8028](https://github.com/confluentinc/ksql/issues/8028) [#8047](https://github.com/confluentinc/ksql/issues/8047)

### Bug Fixes

- Add user-friendly error message for SELECT null AS column ([#8276](https://github.com/confluentinc/ksql/pull/8276)) ([57b7dea](https://github.com/confluentinc/ksql/commit/57b7dea6cef876e0c4e06b699aaee7b3c9874874))
- allow insertion of null values in migrations tool ([#8297](https://github.com/confluentinc/ksql/pull/8297)) ([f42d40a](https://github.com/confluentinc/ksql/commit/f42d40a5f12dfe9d4365f19d6592fe7ab4e936b0))
- allow more time for consumer group cleanup ([#8189](https://github.com/confluentinc/ksql/pull/8189)) ([d2ff1ac](https://github.com/confluentinc/ksql/commit/d2ff1acbe8d59ad339e23118c69dd8e8e3cfcbcf))
- Allows remote pull queries to be cancelled ([#8252](https://github.com/confluentinc/ksql/pull/8252)) ([4efa445](https://github.com/confluentinc/ksql/commit/4efa445b2a5e5ed4ee08d12393cd8482a628d434))
- ClassCastException when dropping sources with 2+ insert queries ([#8205](https://github.com/confluentinc/ksql/pull/8205)) ([a7c6ebe](https://github.com/confluentinc/ksql/commit/a7c6ebeb946df62aed9d0ca1c8724e6f7dd6a18e))
- close command runner when command topic is deleted ([#8208](https://github.com/confluentinc/ksql/pull/8208)) ([294171c](https://github.com/confluentinc/ksql/commit/294171c0e2a92391c3ad6bb453f4c6f6eaf96a29))
- doesnt print error ([#8232](https://github.com/confluentinc/ksql/pull/8232)) ([901b968](https://github.com/confluentinc/ksql/commit/901b968c3530f08fb2e8b61c47d59420b2ec1a6e))
- dont throw error on processing local commands, just log ([#8310](https://github.com/confluentinc/ksql/pull/8310)) ([8bc57a2](https://github.com/confluentinc/ksql/commit/8bc57a2dd133455b03e91c194a469cb60e7053c7))
- During ksql startup, avoid recreating deleted command topic when a valid backup exists [#7753](https://github.com/confluentinc/ksql/issues/7753) ([#8257](https://github.com/confluentinc/ksql/pull/8257)) ([f3f1d5c](https://github.com/confluentinc/ksql/commit/f3f1d5c4463f2d5e8056a6e6fcc2be1bd6e2596f))
- fix_application_id_to_work_with_acls ([#8277](https://github.com/confluentinc/ksql/pull/8277)) ([64f58e8](https://github.com/confluentinc/ksql/commit/64f58e8a31d54b3f397550dd03d7d0ff5999028d))
- get rows returned metric working ([#8230](https://github.com/confluentinc/ksql/pull/8230)) ([da4f71e](https://github.com/confluentinc/ksql/commit/da4f71e91089b5055b8bb514c4c1f910610fb722))
- if not exists return type ([#8322](https://github.com/confluentinc/ksql/pull/8322)) ([9da204c](https://github.com/confluentinc/ksql/commit/9da204c4aa0d1b71c454a296e8a2172f3c66f2e2))
- make parse_date able to parse partial dates ([#8330](https://github.com/confluentinc/ksql/pull/8330)) ([6a82026](https://github.com/confluentinc/ksql/commit/6a820269407152f3f07296eff2baeaa037453156))
- Print only failed line on parsing exception ([#8282](https://github.com/confluentinc/ksql/pull/8282)) ([701db5f](https://github.com/confluentinc/ksql/commit/701db5f0c5ce0ce7218448286710527a3789f37d))
- Pull query table scans support LIKE and BETWEEN operators ([#8299](https://github.com/confluentinc/ksql/pull/8299)) ([bc3ea64](https://github.com/confluentinc/ksql/commit/bc3ea64d355097cf16a8874b9b2b4d96d4c4cbbe))
- Refactor ConnectFormatSchemaTranslator to take translator object instead of lamda function ([#8177](https://github.com/confluentinc/ksql/pull/8177)) ([6d3b351](https://github.com/confluentinc/ksql/commit/6d3b3510502e24b370b2178095bb003f98f8107a))
- Specify source type in DROP referential integrity errors ([#8253](https://github.com/confluentinc/ksql/pull/8253)) ([03bd713](https://github.com/confluentinc/ksql/commit/03bd7137cfe894cb3435b7c1ceed0a34ea6f2534))
- update error msg ([#8221](https://github.com/confluentinc/ksql/pull/8221)) ([8c30780](https://github.com/confluentinc/ksql/commit/8c30780a15e6bd2880f2bf5f447eaf9e746caa15))

### Reverts

## [0.22.0](https://github.com/confluentinc/ksql/releases/tag/v0.22.0-ksqldb) (2021-11-03)

### Features

- add configurations around endpoint logging ([#8249](https://github.com/confluentinc/ksql/pull/8249)) ([21f4e03](https://github.com/confluentinc/ksql/commit/21f4e039c4fe0a4206fbd0c1b7c82342cf27c3fe))
- add consumerGroupId to QueryDescription ([#8073](https://github.com/confluentinc/ksql/pull/8073)) ([cce585b](https://github.com/confluentinc/ksql/commit/cce585b0991a5a7fa39d3e24f327fdd84783f2ff))
- add CumulativeSum total bytes metric ([#7987](https://github.com/confluentinc/ksql/pull/7987)) ([aa213bd](https://github.com/confluentinc/ksql/commit/aa213bd3153cc8d9d9d5b218210dfb6b5806948d))
- add SOURCE streams/tables [#8085](https://github.com/confluentinc/ksql/pull/8085),[#8063](https://github.com/confluentinc/ksql/pull/8063),[#8061](https://github.com/confluentinc/ksql/pull/8061),[#8004](https://github.com/confluentinc/ksql/pull/8004),[#7945](https://github.com/confluentinc/ksql/pull/7945),[#8022](https://github.com/confluentinc/ksql/pull/8022),[#8043](https://github.com/confluentinc/ksql/pull/8043),[#8009](https://github.com/confluentinc/ksql/pull/8009)) ([5416cde](https://github.com/confluentinc/ksql/commit/5416cde808dea59e3091eb637ab733723ff9bdcc),[88c6192](https://github.com/confluentinc/ksql/commit/88c6192db32fe3926483de23367a51ddb78bc374),[e2c3211](https://github.com/confluentinc/ksql/commit/e2c3211c23eac9c92806d013c7ad8e4f1bef7ae7),[0d0e85a](https://github.com/confluentinc/ksql/commit/0d0e85a0d85b02ad506f3096db6ae556f1191ac0),[70565f2](https://github.com/confluentinc/ksql/commit/70565f2969df109c6fd4ae6bb7c48c5365c45f9d),[a322310](https://github.com/confluentinc/ksql/commit/a3223108d4df741a9679d69ea2661ede3cfd7061),[381b7bf](https://github.com/confluentinc/ksql/commit/381b7bf6c943b11136cb0e952109b77c4b95653c),[2c22b8f](https://github.com/confluentinc/ksql/commit/2c22b8f3b3a0f03975e70a27551ea9f343b67204))
- add methods and classes to execute low level HTTP requests in the ksqldb-api-client ([#8118](https://github.com/confluentinc/ksql/pull/8118)) ([aae5f95](https://github.com/confluentinc/ksql/commit/aae5f959da6e3b5851e9eb360051f49cd3f11544)), closes [#8042](https://github.com/confluentinc/ksql/issues/8042)
- add pull queries on streams ([#8126](https://github.com/confluentinc/ksql/pull/8126),[#8168](https://github.com/confluentinc/ksql/pull/8168),[#8124](https://github.com/confluentinc/ksql/pull/8124),[#8143](https://github.com/confluentinc/ksql/pull/8143),[#8115](https://github.com/confluentinc/ksql/pull/8115),[#8064](https://github.com/confluentinc/ksql/pull/8064),[#8045](https://github.com/confluentinc/ksql/pull/8045)) ([e651891](https://github.com/confluentinc/ksql/commit/e651891e336ba3b31fa0ce54057dfef69ec1f8ce),[c09c362](https://github.com/confluentinc/ksql/commit/c09c362294c8a51c6484e4a3ee995d25bbf487d1),[a722184](https://github.com/confluentinc/ksql/commit/a722184a4fbba904493512e0b994d64f65e0670c),[57626f3](https://github.com/confluentinc/ksql/commit/57626f36e9387be2746d808acda5da0055141595),[a14f957](https://github.com/confluentinc/ksql/commit/a14f957c1c094920b81552e87c15e72a34e93d0c),[4831e6b](https://github.com/confluentinc/ksql/commit/4831e6b099cca2be51a659d2bb4112ead0cd8387),[886da99](https://github.com/confluentinc/ksql/commit/886da99988b0ff19cf2d731dec258d68f67b2123))
- add persistent query saturation metric ([#7955](https://github.com/confluentinc/ksql/pull/7955)) ([eed625b](https://github.com/confluentinc/ksql/commit/eed625b331efb5ad7a1e0709c7e4d67d44d2964c))
- add shared runtime config to QueryPlan ([#8074](https://github.com/confluentinc/ksql/pull/8074)) ([351f695](https://github.com/confluentinc/ksql/commit/351f695f1f74cce7f315f2a8ec7f0740bd214883))
- optimize key-range queries in pull queries ([#7993](https://github.com/confluentinc/ksql/pull/7993)) ([22a79bc](https://github.com/confluentinc/ksql/commit/22a79bc7695795494d4aaed8618bf3de252271a4))
- scalable push query bandwidth throttling ([#8087](https://github.com/confluentinc/ksql/pull/8087)) ([d5af6a1](https://github.com/confluentinc/ksql/commit/d5af6a18ad62e89a281a4bebaeb4e75badab1a64))
- update storage utilization metrics to start when app is initialized ([#8095](https://github.com/confluentinc/ksql/pull/8095)) ([8add4ac](https://github.com/confluentinc/ksql/commit/8add4ac34f3597310de231fe687f74224b65b80a))
- add storage utilization metrics ([#7868](https://github.com/confluentinc/ksql/pull/7868)) ([22a8741](https://github.com/confluentinc/ksql/commit/22a87418d4fb6770c55c1ca45bcd151f1ca6b8d8))
- Allow scalable push queries to handle rebalances ([#7988](https://github.com/confluentinc/ksql/pull/7988)) ([b3dbed3](https://github.com/confluentinc/ksql/commit/b3dbed35936684170355588e358d7a2d25692ed5))
- fail scalable push query if error is detected in subscribed persistent query ([#7996](https://github.com/confluentinc/ksql/pull/7996)) ([eed501c](https://github.com/confluentinc/ksql/commit/eed501cb5611868c2546dda56c196a8608009b7e))
- perform SchemaRegistry permissions on C\*AS sink subjects ([#8039](https://github.com/confluentinc/ksql/pull/8039)) ([6878233](https://github.com/confluentinc/ksql/commit/6878233418157b8bddd27c76a9c8fe88aaa5feba))
- shared runtimes ([#7721](https://github.com/confluentinc/ksql/pull/7721)) ([44e8129](https://github.com/confluentinc/ksql/commit/44e812952daff844bc646cb92b935ac738f074c1))
- terminate transient queries by id ([#7947](https://github.com/confluentinc/ksql/pull/7947)) ([1ee8487](https://github.com/confluentinc/ksql/commit/1ee84878996fc5fc02c7876d7e0d7902c84fd554))
- updated minor syntax updates in docs ([#7949](https://github.com/confluentinc/ksql/pull/7949)) ([248dc91](https://github.com/confluentinc/ksql/commit/248dc91a8c13ab5a5c8273a890c58ac04013507f))

### Bug Fixes

- add logcal cluster id to observability metrics ([#8141](https://github.com/confluentinc/ksql/pull/8141)) ([d76a4fc](https://github.com/confluentinc/ksql/commit/d76a4fc7386dc03f65ff8afdc647a3bb292de8e7))
- add new types to udaf functions ([#8081](https://github.com/confluentinc/ksql/pull/8081)) ([a3ea6a4](https://github.com/confluentinc/ksql/commit/a3ea6a46ae74c18af1fac8e4de2a0caba7496a0a))
- add time types to Java client ([#8091](https://github.com/confluentinc/ksql/pull/8091)) ([fd9faf2](https://github.com/confluentinc/ksql/commit/fd9faf2c24ae5e0301c2c67156afa6e0186e7e78))
- Allow multiple EXTRACTJSONFIELD invocations on different paths ([#8122](https://github.com/confluentinc/ksql/pull/8122)) ([#8123](https://github.com/confluentinc/ksql/issues/8123)) ([7f1d407](https://github.com/confluentinc/ksql/commit/7f1d40775b317fa3f2a6df6bc3cb00fa13de86e1))
- Always dec. concurrency counter, even hitting rate limits ([#8165](https://github.com/confluentinc/ksql/pull/8165)) ([0e05519](https://github.com/confluentinc/ksql/commit/0e0551970190861edaa1f7dc4c68ab279597d30b))
- change metrics tag ([#8268](https://github.com/confluentinc/ksql/pull/8268)) ([ff277ba](https://github.com/confluentinc/ksql/commit/ff277ba76d05ffe5c9d9f0276d345b08b12271cb))
- CREATE OR REPLACE TABLE on an existing query fails while initializing the kafka streams ([#8130](https://github.com/confluentinc/ksql/pull/8130)) ([f03755e](https://github.com/confluentinc/ksql/commit/f03755e2c965a3bd15f9090e2e1d8940ea4ecd3d))
- Enables ALPN for internal requests when http2 and tls are in use ([#8094](https://github.com/confluentinc/ksql/pull/8094)) ([7faf77c](https://github.com/confluentinc/ksql/commit/7faf77cf91aa8e4a9437649c448f3ce2142c454c))
- Ensure that we always close /query writer ([#8164](https://github.com/confluentinc/ksql/pull/8164)) ([f7c2002](https://github.com/confluentinc/ksql/commit/f7c20021a24f3a4f80a681a3833249df0447a01c))
- Ensures background timer completes for scalable push queries ([#8132](https://github.com/confluentinc/ksql/pull/8132)) ([1a1297a](https://github.com/confluentinc/ksql/commit/1a1297ab59db0af416a924d4135b93dcc5003561))
- fix spq bandwidth throttling on http2 ([#8119](https://github.com/confluentinc/ksql/pull/8119)) ([d5d413d](https://github.com/confluentinc/ksql/commit/d5d413dd82ccbbe12c4a5be923619f50a9a645af))
- Fixes errors and increased latency for pull queries from closing connection ([#8248](https://github.com/confluentinc/ksql/pull/8248)) ([d98f50a](https://github.com/confluentinc/ksql/commit/d98f50aff316a411c2eb84dc1817463624403564))
- issue 7948. Allow insert into table using Java API ([#8114](https://github.com/confluentinc/ksql/pull/8114)) ([a6c2cac](https://github.com/confluentinc/ksql/commit/a6c2cac8cca9a1ca1194f1fb09a0cf7a99ae47ea))
- Removes config check to insert SPQ Processor ([#8062](https://github.com/confluentinc/ksql/pull/8062)) ([a0be1d5](https://github.com/confluentinc/ksql/commit/a0be1d582b5cc27d01de98ab745d0559e9ad5ff9))
- skip adding invalid if not exists to cmd topic ([#8206](https://github.com/confluentinc/ksql/pull/8206)) ([e164b18](https://github.com/confluentinc/ksql/commit/e164b186f74ce4b0a450be9a8ba88d5921bc2205))
- stream pull query internal overrides shouldn't clash with query configs ([#8166](https://github.com/confluentinc/ksql/pull/8166)) ([0c57258](https://github.com/confluentinc/ksql/commit/0c57258bd6b19765a3d40173ffba6f21d864e50f))
- use a looser check on the error msg for overflow ([#8098](https://github.com/confluentinc/ksql/pull/8098)) ([0088fb1](https://github.com/confluentinc/ksql/commit/0088fb1b22b0abc929c14f856dd58494372137a7))
- date parsing functions: case-insensitive name parsing ([#8015](https://github.com/confluentinc/ksql/pull/8015)) ([73bdb18](https://github.com/confluentinc/ksql/commit/73bdb184c1f225030577bb7771a55ab95ce73cb8))
- Fixes pull query latency distribution metrics ([#7992](https://github.com/confluentinc/ksql/pull/7992)) ([c798cd7](https://github.com/confluentinc/ksql/commit/c798cd76f4d4888c959bdec0a0b3c8c11fdc5dd9))
- make KsqlAvroSerializerTest work with Java 16 ([#7873](https://github.com/confluentinc/ksql/pull/7873)) ([bab8874](https://github.com/confluentinc/ksql/commit/bab887417fab1b4b609c28db3462fd9781479373))

## [0.21.0](https://github.com/confluentinc/ksql/releases/tag/v0.21.0-ksqldb) (2021-09-15)

### Features

- add `ARRAY_CONCAT` UDF ([#7761](https://github.com/confluentinc/ksql/pull/7761)) ([1de9ef8](https://github.com/confluentinc/ksql/commit/1de9ef8108ff0d5a472a993ee27dae13e65d0b39))
- add BYTES type to ksqlDB ([#7778](https://github.com/confluentinc/ksql/pull/7778),[#7804](https://github.com/confluentinc/ksql/pull/7804),[#7791](https://github.com/confluentinc/ksql/pull/7791),[#7823](https://github.com/confluentinc/ksql/pull/7823)) ([06657ba](https://github.com/confluentinc/ksql/commit/06657bafe73aa6066723dfbaf0742e92a289fb80),[02352f2](https://github.com/confluentinc/ksql/commit/02352f20594b1029c65b78332366dc443825e3d5),[2ae4cae](https://github.com/confluentinc/ksql/commit/2ae4caeb2e5edfba8f5b6c15611d6f37aef0d60b),[df5964e](https://github.com/confluentinc/ksql/commit/df5964ed5a22ef9c2a316d4fdf32f927b548ecb4))
- add interface for metrics reporter ([#7788](https://github.com/confluentinc/ksql/pull/7788)) ([0ee06d2](https://github.com/confluentinc/ksql/commit/0ee06d2462b5d6a94d1b333a6d7fe1baec7d0fcd))
- add observability metric skeleton ([#7769](https://github.com/confluentinc/ksql/pull/7769)) ([3362c00](https://github.com/confluentinc/ksql/commit/3362c00e18fed4615b4224b3814ee238ceb7e8d4))
- add TO_BYTES/FROM_BYTES functions for bytes/string conversions ([#7831](https://github.com/confluentinc/ksql/pull/7831)) ([cea0989](https://github.com/confluentinc/ksql/commit/cea09894b89712b43a6e9cd58b0d6ac445ce8ba5))
- allow expressions on left table columns in FK-joins ([#7904](https://github.com/confluentinc/ksql/pull/7904)) ([a9668de](https://github.com/confluentinc/ksql/commit/a9668de461566f401d79666c4d5fff9ff0c255a6))
- allow Java clients to set HTTP/2 multiplex limit ([#7871](https://github.com/confluentinc/ksql/pull/7871)) ([790d6fe](https://github.com/confluentinc/ksql/commit/790d6fe2c39265c77493ec0a331b416ed147a8cd))
- don't start queries when corruption is detected during startup ([#7821](https://github.com/confluentinc/ksql/pull/7821)) ([4c0c181](https://github.com/confluentinc/ksql/commit/4c0c181b5cc3bad69d7f140518c25fde94d62a65))
- enable BYTES for LPAD and RPAD ([#7909](https://github.com/confluentinc/ksql/pull/7909)) ([f0c23b1](https://github.com/confluentinc/ksql/commit/f0c23b111285e7738fe614522a1d3deecded09ae))
- enables BYTES for CONCAT and CONCAT_WS ([#7876](https://github.com/confluentinc/ksql/pull/7876)) ([f631c2b](https://github.com/confluentinc/ksql/commit/f631c2b3bcfdc044851667b5e1a6dd0d7793d6cf))
- make SchemaRegistry permission validations on KSQL requests ([#7773](https://github.com/confluentinc/ksql/pull/7773)) ([ad01b72](https://github.com/confluentinc/ksql/commit/ad01b72ee25ce8848d3c67fa6321924c309867a5))
- update len function to accept BYTES ([#7865](https://github.com/confluentinc/ksql/pull/7865)) ([eaaa0db](https://github.com/confluentinc/ksql/commit/eaaa0db39d1274445f874708517cc3b608aa2289))
- Update SUBSTRING function to accept BYTES types ([#7861](https://github.com/confluentinc/ksql/pull/7861)) ([fccc56d](https://github.com/confluentinc/ksql/commit/fccc56d36db58f01665ebab39b747c09c7c2c419))

## [0.20.0](https://github.com/confluentinc/ksql/releases/tag/v0.20.0-ksqldb) (2021-07-26)

### Features

- add `LEAST` and `GREATEST` UDFs ([#7683](https://github.com/confluentinc/ksql/pull/7683)) ([0d84733](https://github.com/confluentinc/ksql/commit/0d847335b4e048ebbb8bc98a5c40f9af26c77c05))
- add DATEADD and DATESUB functions ([#7744](https://github.com/confluentinc/ksql/pull/7744)) ([c63e924](https://github.com/confluentinc/ksql/commit/c63e924081971f70b68b6ac4545649d370eb1a29))
- add FROM_DAYS and update UNIX_DATE function ([#7742](https://github.com/confluentinc/ksql/pull/7742)) ([3c68710](https://github.com/confluentinc/ksql/commit/3c6871089e290be60787dda260ff8f8be0422483))
- add PARSE_DATE and FORMAT_DATE functions ([#7733](https://github.com/confluentinc/ksql/pull/7733)) ([5a64ed7](https://github.com/confluentinc/ksql/commit/5a64ed7e762a06b9a2969def7c493b84451c0ac1))
- add PARSE_TIME and FORMAT_TIME functions ([#7722](https://github.com/confluentinc/ksql/pull/7722)) ([9a381a8](https://github.com/confluentinc/ksql/commit/9a381a8ec8efc3e49632068d65de6e8b235d0527))
- add TIMEADD and TIMESUB functions ([#7727](https://github.com/confluentinc/ksql/pull/7727)) ([75806a0](https://github.com/confluentinc/ksql/commit/75806a0233d838b3980a9b367cab42d7ab3b62d3))
- pull query bandwidth based throttling ([#7738](https://github.com/confluentinc/ksql/pull/7738)) ([8f01ad9](https://github.com/confluentinc/ksql/commit/8f01ad986c6d9e61be3da9b011941fef45ab2ec7))
- add the DATE and TIME sql types ([#7641](https://github.com/confluentinc/ksql/pull/7641),[#7664](https://github.com/confluentinc/ksql/pull/7664),[#7718](https://github.com/confluentinc/ksql/pull/7718),[#7734](https://github.com/confluentinc/ksql/pull/7734),[#7708](https://github.com/confluentinc/ksql/pull/7708),[#7740](https://github.com/confluentinc/ksql/pull/7740),[#7674](https://github.com/confluentinc/ksql/pull/7674),[#7700](https://github.com/confluentinc/ksql/pull/7700))([661f198](https://github.com/confluentinc/ksql/commit/661f19836502442ee2fff155f9fbc2a6fbf0063f),[7537d87](https://github.com/confluentinc/ksql/commit/7537d87a77f37afc557d061fd2b816dfcdbfb12a),[a94f1f2](https://github.com/confluentinc/ksql/commit/a94f1f234607fb594fe4b617c83bba1b97a812d2),[78b9ae8](https://github.com/confluentinc/ksql/commit/78b9ae8666f0a74460aa9b1bed01d437930be2ce),[18cc030](https://github.com/confluentinc/ksql/commit/18cc030f453adcaa525d79e6bb773e35827a2ba7),[79d14fb](https://github.com/confluentinc/ksql/commit/79d14fb60d7d784b3e099bcd59f102012c6a0d63),[7718955](https://github.com/confluentinc/ksql/commit/771895510f9dbeadf4504dbaa562490da124dd63),[4175ad5](https://github.com/confluentinc/ksql/commit/4175ad5f46d9ee15a0469cfcffb2a217c41b3c1e))

### Bug Fixes

- block out of order migrations in migrations tool ([#7693](https://github.com/confluentinc/ksql/pull/7693)) ([1d617d3](https://github.com/confluentinc/ksql/commit/1d617d38d2928b8ec39d48df8060d4f43649382d))
  ([#7678](https://github.com/confluentinc/ksql/pull/7678)) ([9bf9abf](https://github.com/confluentinc/ksql/commit/9bf9abf0d8093d2b53859abd5fe54a0d280a3cc7))
- enable schema inference for timestamp/time/date ([#7737](https://github.com/confluentinc/ksql/pull/7737)) ([35b1cad](https://github.com/confluentinc/ksql/commit/35b1cadc08faea68ca0b76e6c08ac664a5122c0b))
- enable time unit functions for interpreter ([#7709](https://github.com/confluentinc/ksql/pull/7709)) ([a26a297](https://github.com/confluentinc/ksql/commit/a26a297af55ad46c3f46efae78f470a33d0df251))
- fixed nondeterminism in UdfIndex ([#7719](https://github.com/confluentinc/ksql/pull/7719)) ([cd1a988](https://github.com/confluentinc/ksql/commit/cd1a9880aa39e0fb9b4e84d9f713235c8569b104))
- make current java clients compatible with pre-0.15 servers ([#7667](https://github.com/confluentinc/ksql/pull/7667)) ([8f2d799](https://github.com/confluentinc/ksql/commit/8f2d79944050af0402a208a9cd939eaf92d37851))
- remove time/date component when casting timestamp to date/time ([#7724](https://github.com/confluentinc/ksql/pull/7724)) ([87cd3c7](https://github.com/confluentinc/ksql/commit/87cd3c727c7a27b717d89f51459a133cfa2d7cad))
- return an error message on http2 /query-stream endpoint ([#7750](https://github.com/confluentinc/ksql/pull/7750)) ([3a4348b](https://github.com/confluentinc/ksql/commit/3a4348b9f394c9ac0dd3a3eeb64bd2e6b4663f69))
- Fixes race condition exposing uninitialized query ([#7627](https://github.com/confluentinc/ksql/pull/7627)) ([98b1e3c](https://github.com/confluentinc/ksql/commit/98b1e3c220a6891321eecca0330adc8229eb089b))

### BREAKING CHANGES

- Existing queries that relied on vague implicit casting will not be started after an upgrade, and new queries that rely on vague implicit casting will be rejected. For example, foo(INT, INT) will not be able to resolve against two underlying function signatures of foo(BIGINT, BIGINT) and foo(DOUBLE, DOUBLE). Calling a function whose only parameter is variadic with an explicit null will also result in the call being rejected as vague.

It's worth noting that queries which relied on this vague implicit casting were never truly supported, as they would have been nondeterministic. UDFs most likely to be adversely effected are ones which have multiple numerical overloads with repeated logic. For example, if you had two UDFs foo(INT, INT) and foo(BIGINT, BIGINT), both which relied on logic which returns null if both input parameters are null, then the prior behavior would have been to nondeterministically route to one of the two functions, at which null would be returned either way (due to the duplicate logic). This change will break existing queries that relied on this nondeterministic routing.

## [0.19.0](https://github.com/confluentinc/ksql/releases/tag/v0.19.0-ksqldb) (2021-06-08)

### Features

- add idle timeout ([#7556](https://github.com/confluentinc/ksql/pull/7556)) ([db35b98](https://github.com/confluentinc/ksql/commit/db35b9885da05cfd265d8d332a70d564cfebb590)), closes [#6970](https://github.com/confluentinc/ksql/issues/6970)
- added NULLIF function ([#6567](https://github.com/confluentinc/ksql/pull/6567)) ([#6685](https://github.com/confluentinc/ksql/issues/6685)) ([d7c9e43](https://github.com/confluentinc/ksql/commit/d7c9e4360b2ce2a52be51484790ee492b82b9605))
- Adds Scalable Push Query physical operators ([#7430](https://github.com/confluentinc/ksql/pull/7430)) ([100767d](https://github.com/confluentinc/ksql/commit/100767d73dfe013a41745ba540532b717261eeb8))
- Adds Scalable Push Query Routing ([#7587](https://github.com/confluentinc/ksql/pull/7587)) ([278a261](https://github.com/confluentinc/ksql/commit/278a2610d808988c4d32bacd4079650455362c8d))
- Adds ScalablePushRegistry and peeking ability in a persistent query ([#7424](https://github.com/confluentinc/ksql/pull/7424)) ([89c1588](https://github.com/confluentinc/ksql/commit/89c1588440c26b6467ad58026ec664fbd0b6b85b))
- Allow ksqlDB to detect FK-join table-table join condition ([#7452](https://github.com/confluentinc/ksql/pull/7452)) ([344d36d](https://github.com/confluentinc/ksql/commit/344d36d8263e1e574f62465c551f46c08ebb5ebd))
- build physical plan for FK table-table joins ([#7517](https://github.com/confluentinc/ksql/pull/7517)) ([744fa36](https://github.com/confluentinc/ksql/commit/744fa36e7aaf526f3a7cd83b45d31d60353a0fc0))
- made class to compute essential meta-data ([#7434](https://github.com/confluentinc/ksql/pull/7434)) ([e53aa13](https://github.com/confluentinc/ksql/commit/e53aa13a89b46ae85d36e2955feb395ff6af6767))
- ungate support for foreign key joins ([#7591](https://github.com/confluentinc/ksql/pull/7591)) ([061fb4a](https://github.com/confluentinc/ksql/commit/061fb4acbe18ac098393437a85d2c6455d305a22))
- use Connect default precision for Avro decimals if unspecified (MINOR) ([#7615](https://github.com/confluentinc/ksql/pull/7615)) ([1abdb0d](https://github.com/confluentinc/ksql/commit/1abdb0d93749f96ff88cd83a743e4b7acd3e9c0a))

### Bug Fixes

- allow for KSQL_GC_LOG_OPTS env variable to be passed through ([#7577](https://github.com/confluentinc/ksql/pull/7577)) ([aa2ed0a](https://github.com/confluentinc/ksql/commit/aa2ed0aebf5fe8f488ca22e35cdabb8be824c527))
- better error message on illegal arithmetic with NULL values ([#7554](https://github.com/confluentinc/ksql/pull/7554)) ([867a587](https://github.com/confluentinc/ksql/commit/867a58722d78b3d5b518d08d68fe860c6a74241c))
- change the isError to not use the state ([#7483](https://github.com/confluentinc/ksql/pull/7483)) ([41ec430](https://github.com/confluentinc/ksql/commit/41ec430e1a22e7838c8c2ade751112762b62fe4e))
- DROP stream for persistent query doesn't always drop underlying query ([#7601](https://github.com/confluentinc/ksql/pull/7601)) ([b751cad](https://github.com/confluentinc/ksql/commit/b751cad6b1df1ec8eead73663c0923b3f5d90f14))
- extended query anonymizer tests with functional tests queries ([#7480](https://github.com/confluentinc/ksql/pull/7480)) ([9a67191](https://github.com/confluentinc/ksql/commit/9a67191cecdcb5265efcd92cf795420a178ecc1c))
- fix shouldNotBeAbleToUseWssIfClientDoesNotTrustServerCert test ([#7614](https://github.com/confluentinc/ksql/pull/7614)) ([39ef7bb](https://github.com/confluentinc/ksql/commit/39ef7bb52cfbef32bc450994ec90d6f4ffaf0252))
- fix the 5.2.x build to be compatible with newer jetty/jackson versions ([#7575](https://github.com/confluentinc/ksql/pull/7575)) ([103eeec](https://github.com/confluentinc/ksql/commit/103eeec50dbf826adcbd2b7507db51cf30e38deb)), closes [#5725](https://github.com/confluentinc/ksql/issues/5725)
- Fixes org.mock-server to be 5.11.2 to utilize newer netty ([#7458](https://github.com/confluentinc/ksql/pull/7458)) ([0df7dfc](https://github.com/confluentinc/ksql/commit/0df7dfc2b7410e165d24ef4f68189e8aa3466940))
- multi-column keys are broken in some scenarios when rearranged ([#7477](https://github.com/confluentinc/ksql/pull/7477)) ([453ca8b](https://github.com/confluentinc/ksql/commit/453ca8b8093aae3322c682562224ada012648426))
- qualified select \* now works in n-way joins with repartitions and multiple layers of nesting ([#7585](https://github.com/confluentinc/ksql/pull/7585)) ([0879cb0](https://github.com/confluentinc/ksql/commit/0879cb08c1a2c4aa3e505dea5bb68394b3bacc23))
- reject mismatched decimals from avro topics ([#7544](https://github.com/confluentinc/ksql/pull/7544)) ([85ba0f1](https://github.com/confluentinc/ksql/commit/85ba0f1d4800eda58fe89f89bb6ddbb61a570aff))
- Update Maven wrapper and document its use to fix version resolution ([#7620](https://github.com/confluentinc/ksql/pull/7620)) ([535fc09](https://github.com/confluentinc/ksql/commit/535fc0921f038bd55b5b80d79b7a1df126a3adf9)), closes [confluentinc/maven#1](https://github.com/confluentinc/maven/issues/1)
- Use Java's Base64 instead of Jersey's. ([#7534](https://github.com/confluentinc/ksql/pull/7534)) ([0ea0ae3](https://github.com/confluentinc/ksql/commit/0ea0ae341ad2288355085e1805a9539cb683e112))
- Fix: ksqlDB engine does not infer the Struct type correctly from protobuf schema ([#7642](https://github.com/confluentinc/ksql/issues/7642))

## [0.18.0](https://github.com/confluentinc/ksql/releases/tag/v0.18.0-ksqldb) (2021-05-26)

### Features

- implemented working query anonymizer ([#7357](https://github.com/confluentinc/ksql/pull/7357)) ([fa0445f](https://github.com/confluentinc/ksql/commit/fa0445fe466511c5126ae30f2979bcc48f2792c4))
- add 'show connector plugins' syntax ([#7284](https://github.com/confluentinc/ksql/pull/7284)) ([be50d2d](https://github.com/confluentinc/ksql/commit/be50d2db3681ac9d91501d203fec9d19bcfdc716))
- Detailed pull query metrics broken down by type and source ([#7272](https://github.com/confluentinc/ksql/pull/7272)) ([9c173a6](https://github.com/confluentinc/ksql/commit/9c173a6022f9e95ef7cbc1ddbf8abd61ec9d2320))
- emit an error reason before closing websocket ([#7390](https://github.com/confluentinc/ksql/pull/7390)) ([c2d9372](https://github.com/confluentinc/ksql/commit/c2d9372715be0a1d3d8013c91c5f07b0aec3e871))
- Materialize Table-Table join results ([#7246](https://github.com/confluentinc/ksql/pull/7246)) ([4ae1b31](https://github.com/confluentinc/ksql/commit/4ae1b31913c1d118894aed377bef086461fc1062))
- include task metadata information from remote hosts in query descriptions ([#7331](https://github.com/confluentinc/ksql/pull/7331)) ([c0e1e73](https://github.com/confluentinc/ksql/commit/c0e1e73fc5e7b1c05562aaf618c77902ea779dc8))
- print stats/errors breakdown by host in cli ([#7296](https://github.com/confluentinc/ksql/pull/7296)) ([20a4ea5](https://github.com/confluentinc/ksql/commit/20a4ea5a84d134b44ff5d41335bdf925a8ce98f5))
- include aggregated metrics in source descriptions ([#7252](https://github.com/confluentinc/ksql/pull/7252)) ([0b30ed9](https://github.com/confluentinc/ksql/commit/0b30ed97aac906c6639da00588d580f148ffeb7f)), ([#7235](https://github.com/confluentinc/ksql/pull/7235)) ([924fc5b](https://github.com/confluentinc/ksql/commit/924fc5baacd7ff7002566641dab936cfabda2532))
- add --define flag to migrations tool apply command ([#7401](https://github.com/confluentinc/ksql/pull/7401)) ([165e972](https://github.com/confluentinc/ksql/commit/165e9729110f8059ab6338052074e2e9bb10c8bf))
- support DEFINE and UNDEFINE statements in migrations tool ([#7366](https://github.com/confluentinc/ksql/pull/7366)) ([330db93](https://github.com/confluentinc/ksql/commit/330db939861f22da7c6914a66a39824e08972419))
- enable variable substitution for /query-stream and /ksql endpoints ([#7271](https://github.com/confluentinc/ksql/pull/7271)) ([f6dd212](https://github.com/confluentinc/ksql/commit/f6dd212a50a6c247f2c7a946da3144a0f1460fc7))
- enable variable substitution for java client ([#7335](https://github.com/confluentinc/ksql/pull/7335)) ([c82a072](https://github.com/confluentinc/ksql/commit/c82a072e40fd64085faf11cdead6c255e0096708))
- Add connector functions to java client ([#7222](https://github.com/confluentinc/ksql/pull/7222)) ([7766195](https://github.com/confluentinc/ksql/commit/77661957c447ee3eb6bbc162ed2a8976a06ac71b))

### Bug Fixes

- Add line breaks to error message ([#7324](https://github.com/confluentinc/ksql/pull/7324)) ([1695f39](https://github.com/confluentinc/ksql/commit/1695f390cf1b3c676fba703c87987a0828e668df)), closes [#7205](https://github.com/confluentinc/ksql/issues/7205)
- Append state.dir directive to ksql-server.properties ([#7003](https://github.com/confluentinc/ksql/pull/7003)) ([4893e90](https://github.com/confluentinc/ksql/commit/4893e90fd0dcbcc26626a36579558e048a6ad9d8))
- Bubble up errors from HARouting unless using StandbyFallbackException ([#7238](https://github.com/confluentinc/ksql/pull/7238)) ([ec12516](https://github.com/confluentinc/ksql/commit/ec12516799993cb99880d0430b609ce4d0ab81dc))
- fix NPE when backing a record that has null key/values ([#7268](https://github.com/confluentinc/ksql/pull/7268)) ([0cbd4e8](https://github.com/confluentinc/ksql/commit/0cbd4e847326010d0acd321331e8427f7ea731e9))
- preserve the rest of a struct when one field has a processing error ([#7373](https://github.com/confluentinc/ksql/pull/7373)) ([6d708db](https://github.com/confluentinc/ksql/commit/6d708db41fe7fa6c1a3cd74dcebfcd66c66e0aaa))
- stop long-running queries from blocking the main event loop ([#7420](https://github.com/confluentinc/ksql/pull/7420)) ([242fefb](https://github.com/confluentinc/ksql/commit/242fefb268923650592996f6764f198488759d2e)), closes [#7358](https://github.com/confluentinc/ksql/issues/7358)
- stop worker-poll tasks from blocking main loop ([#7427](https://github.com/confluentinc/ksql/pull/7427)) ([0b0bf65](https://github.com/confluentinc/ksql/commit/0b0bf65df6c2e178a5c249de6ac6bf7a13abe70c)), closes [#7358](https://github.com/confluentinc/ksql/issues/7358)
- allow java client to accept statements with more than one semicolon ([#7243](https://github.com/confluentinc/ksql/pull/7243)) ([4086acb](https://github.com/confluentinc/ksql/commit/4086acbeff89cc5f8c0b5b03e800074894be8183))
- fix NPE when closing transient queries ([#7530](https://github.com/confluentinc/ksql/pull/7530)) ([bc64edd](https://github.com/confluentinc/ksql/commit/bc64eddd88c6ae936c0518f816c8ef477070b20b))

## [0.17.0](https://github.com/confluentinc/ksql/releases/tag/v0.17.0-ksqldb) (2021-04-26)

### Features

- adds support for lambda functions ([#6955](https://github.com/confluentinc/ksql/pull/6955)) ([1b39ab5](https://github.com/confluentinc/ksql/commit/1b39ab56360612d385554d61c137814655dbf4b9)), ([#6868](https://github.com/confluentinc/ksql/pull/6868)) ([dd3f365](https://github.com/confluentinc/ksql/commit/dd3f365eba725c77cca63f9196dac174394ecb58)), ([#7075](https://github.com/confluentinc/ksql/pull/7075)) ([d6529a3](https://github.com/confluentinc/ksql/commit/d6529a32d4d280a0b8c66c56228dedfc45a2bf03)), ([#7148](https://github.com/confluentinc/ksql/pull/7148)) ([c8f745e](https://github.com/confluentinc/ksql/commit/c8f745e8ac8f034661234ec7334009df4b50aac1)), ([#6966](https://github.com/confluentinc/ksql/pull/6966)) ([d09c99e](https://github.com/confluentinc/ksql/commit/d09c99edb29485182e5b18f0612eae338daa41e1)), ([#7056](https://github.com/confluentinc/ksql/pull/7056)) ([1a042cd](https://github.com/confluentinc/ksql/commit/1a042cd2bb9babc52a7aa071638aa408a1e47b73)), ([#6994](https://github.com/confluentinc/ksql/pull/6994)) ([563ff9b](https://github.com/confluentinc/ksql/commit/563ff9b91a9095045c1597a7008359439a8a217c))
- Adds ability to bypass cache for pull queries ([#6891](https://github.com/confluentinc/ksql/pull/6891)) ([4b3bc96](https://github.com/confluentinc/ksql/commit/4b3bc96830fbea8051cfaf4e9d27c4399032813f))
- Allows pull queries with generic WHERE clauses ([#6939](https://github.com/confluentinc/ksql/pull/6939)) ([c3fe8a1](https://github.com/confluentinc/ksql/commit/c3fe8a1aaf17c40f1205374f13a633ca6ebdbe19))
- Adds an expression interpreter to improve pull query performance ([#7006](https://github.com/confluenchtinc/ksql/pull/7006)) ([5d2cd83](https://github.com/confluentinc/ksql/commit/5d2cd8398ba5ae09abf0a1edb47113028aa8e0b7))
- implements migrations tool and corresponding commands ([#6988](https://github.com/confluentinc/ksql/pull/6988)) ([8bdb09a](https://github.com/confluentinc/ksql/commit/8bdb09a2751365966cf5a4a1436276d7a3e0d9e1)), ([#7161](https://github.com/confluentinc/ksql/pull/7161)) ([2c614cd](https://github.com/confluentinc/ksql/commit/2c614cd80aeae81c71107de16fbf2649ce222b1c)), ([#7190](https://github.com/confluentinc/ksql/pull/7190)) ([4151bae](https://github.com/confluentinc/ksql/commit/4151bae93632b9b2ed425db093051d4b58bffa7d)), ([#7137](https://github.com/confluentinc/ksql/pull/7137)) ([608cb5e](https://github.com/confluentinc/ksql/commit/608cb5eabb13cfc65bd6388993a5912da5f6b764)), ([#7099](https://github.com/confluentinc/ksql/pull/7099)) ([a75c355](https://github.com/confluentinc/ksql/commit/a75c355943ff819aaecc6c73702d138efcf6c089)), ([#7153](https://github.com/confluentinc/ksql/pull/7153)) ([2e546b6](https://github.com/confluentinc/ksql/commit/2e546b625d7beaf83645f742839cff44d6e1bc62)), ([#7133](https://github.com/confluentinc/ksql/pull/7133)) ([c91e23c](https://github.com/confluentinc/ksql/commit/c91e23c0f99b71912cecc00149d07b367d5c81cd)), ([#7145](https://github.com/confluentinc/ksql/pull/7145)) ([956e799](https://github.com/confluentinc/ksql/commit/956e799e70cf23e7450302888609bd35d2b56823)), ([#7087](https://github.com/confluentinc/ksql/pull/7087)) ([d4cf400](https://github.com/confluentinc/ksql/commit/d4cf400cb77e7a9dd0d7810e7de53562197f32fa))
- add maven wrapper ([#7307](https://github.com/confluentinc/ksql/pull/7307)) ([d0ab425](https://github.com/confluentinc/ksql/commit/d0ab4253d243d11783b8f81fba20fb7fae77038a))
- CLI should fail for unsupported server version ([#7097](https://github.com/confluentinc/ksql/pull/7097)) ([a0745b9](https://github.com/confluentinc/ksql/commit/a0745b9e1a855be5be3c4cf5c2ffc7e384d29105))
- Add timestamp arithmetic functionality ([#6901](https://github.com/confluentinc/ksql/pull/6901)) ([e2c06dc](https://github.com/confluentinc/ksql/commit/e2c06dc47c594215bc956fe29c5a1619508012af))
- added script to export antlr tokens to use in frontend editor ([#7118](https://github.com/confluentinc/ksql/pull/7118)) ([7e1c180](https://github.com/confluentinc/ksql/commit/7e1c1808e535c43c9d5991e24bb8490909b46c13))
- makes tables queryable through efficient and flexible table scans ([#7155](https://github.com/confluentinc/ksql/pull/7155)) ([71becea](https://github.com/confluentinc/ksql/commit/71becea4a1fe193004d0e480df70ac30e0a8bc0a)), ([#7188](https://github.com/confluentinc/ksql/pull/7188)) ([55f5403](https://github.com/confluentinc/ksql/commit/55f5403eab223ca727ca5a6d4efb10b06b1dd97b)), ([#7085](https://github.com/confluentinc/ksql/pull/7085)) ([65d0df1](https://github.com/confluentinc/ksql/commit/65d0df1c49cd38a208cf16ee05af19559e688787))
- limit the number of active push queries everywhere using "ksql.max.push.queries" config ([#7109](https://github.com/confluentinc/ksql/pull/7109)) ([906f4c5](https://github.com/confluentinc/ksql/commit/906f4c5dee491cfe1366e70b1f06bd45b280f51c))
- classify authorization exception as user error ([#7061](https://github.com/confluentinc/ksql/pull/7061)) ([a74b77c](https://github.com/confluentinc/ksql/commit/a74b77c15e30319bf98b3d686cf127e97ed44154))

### Bug Fixes

- check for null inputs for various timestamp functions ([#7180](https://github.com/confluentinc/ksql/pull/7180)) ([42496c1](https://github.com/confluentinc/ksql/commit/42496c17b7bb4d2122a62e32d91b1ff321d71433))
- Ensures BaseSubscriber.makeRequest is called on context in PollableSubscriber ([#7212](https://github.com/confluentinc/ksql/pull/7212)) ([da67bd9](https://github.com/confluentinc/ksql/commit/da67bd91062a58e93842579f0919092245ab630f))
- fix the cache max bytes buffering check ([#7181](https://github.com/confluentinc/ksql/pull/7181)) ([f383800](https://github.com/confluentinc/ksql/commit/f3838002fc4c8c085a3f62bfd34073e232416de4))
- get all available restore commands even on poll timeout ([#6985](https://github.com/confluentinc/ksql/pull/6985)) ([28a7ba9](https://github.com/confluentinc/ksql/commit/28a7ba9a3d05a821eea702dcfaff046463a5394c))
- ksql.service.id should not be usable as a query parameter ([#7192](https://github.com/confluentinc/ksql/pull/7192)) ([cc5cd81](https://github.com/confluentinc/ksql/commit/cc5cd81cadc7fb8fb7cbf34c9e98a3a567df746e))
- Make pull query metrics apply only to pull and not also push ([#6944](https://github.com/confluentinc/ksql/pull/6944)) ([1db18b3](https://github.com/confluentinc/ksql/commit/1db18b3e8b161ae7315db67917bb1f5d89da8c2a))
- prevent IOB when printing topics with a key/value with an empty string ([#7162](https://github.com/confluentinc/ksql/pull/7162)) ([177d0db](https://github.com/confluentinc/ksql/commit/177d0dbff72adcded82f9da9f9637b23fc6ffa3c))
- Pull Queries: Avoids KsqlConfig copy with overrides since this is very inefficient ([#7193](https://github.com/confluentinc/ksql/pull/7193)) ([b36a3ce](https://github.com/confluentinc/ksql/commit/b36a3ce7d02dbeec7740d32801d443c4d7bca1e2))
- update default kafka log4j appender configs for sync sends ([#7078](https://github.com/confluentinc/ksql/pull/7078)) ([8bc16b3](https://github.com/confluentinc/ksql/commit/8bc16b3f8b6d6a785f65c73c510180db443ef4aa))

## [0.16.0](https://github.com/confluentinc/ksql/releases/tag/v0.16.0-ksqldb) (Not released publicly, build hiccups)

### Features

- **client:** add getInfo method to java client ([#7030](https://github.com/confluentinc/ksql/pull/7030)) ([b09f003](https://github.com/confluentinc/ksql/commit/b09f003f31e45080301f65d329b132fe71eeb78b))
- add describe option to list streams/tables ([#6827](https://github.com/confluentinc/ksql/pull/6827)) ([26e3dea](https://github.com/confluentinc/ksql/commit/26e3deab8242df235f0541e7d2c90df5ed17b815))
- add functions to support use of timestamp data type ([#6852](https://github.com/confluentinc/ksql/pull/6852)) ([2cee618](https://github.com/confluentinc/ksql/commit/2cee618ef8ca8a24768e0f7b1b88ccd2e5364248))
- add standard deviation udf ([#6845](https://github.com/confluentinc/ksql/pull/6845)) ([f2fdbb3](https://github.com/confluentinc/ksql/commit/f2fdbb389d06aafc808493fe009719f202351728))
- display precision and scale when describing DECIMAL columns ([#6872](https://github.com/confluentinc/ksql/pull/6872)) ([fb89998](https://github.com/confluentinc/ksql/commit/fb89998ede01922a4741448f37c7064aa966147e))
- enable decimal for Protobuf ([#6884](https://github.com/confluentinc/ksql/pull/6884)) ([6b5877c](https://github.com/confluentinc/ksql/commit/6b5877c2910023263421b247824af4eadb7db640))
- Make pull queries streamed asynchronously ([#6813](https://github.com/confluentinc/ksql/pull/6813)) ([b69e3f8](https://github.com/confluentinc/ksql/commit/b69e3f8e459c3d0a055ec0738ae71e58bda7b28c))
- make UDAFs configurable and remove limit on COLLECT_LIST/SET ([#6851](https://github.com/confluentinc/ksql/pull/6851)) ([63ae169](https://github.com/confluentinc/ksql/commit/63ae169ab753aab980f0ee4cbf52fbe34291418d))
- Rewrites pull query WHERE clause to be in DNF and allow more expressions ([#6874](https://github.com/confluentinc/ksql/pull/6874)) ([b8e0c99](https://github.com/confluentinc/ksql/commit/b8e0c991a982229afd6d42dc8752a29a35fa937e))
- Support timestamp protobuf serde ([#6927](https://github.com/confluentinc/ksql/pull/6927)) ([5ea1ce4](https://github.com/confluentinc/ksql/commit/5ea1ce4c9129260272608aa8d8263e22a23a9188))
- timestamp support - casting, comparisons and serde ([#6806](https://github.com/confluentinc/ksql/pull/6806)) ([a27df46](https://github.com/confluentinc/ksql/commit/a27df465040bf3aa409ca34509c0117fadf8982a))

### Bug Fixes

- client/server SSL settings fail when 'ssl.key.password' is set ([#6763](https://github.com/confluentinc/ksql/pull/6763)) ([3e48540](https://github.com/confluentinc/ksql/commit/3e48540fbe1a02ed02faa667e424b225f9d7ba20))
- do not log data on JSON deserialization errors (MINOR) ([#6930](https://github.com/confluentinc/ksql/pull/6930)) ([1834da6](https://github.com/confluentinc/ksql/commit/1834da678cf86e7cb333d57a9c97d1884d09d7af))
- ff-4240-upgrade httpclient version ([#6935](https://github.com/confluentinc/ksql/pull/6935)) ([19f4d63](https://github.com/confluentinc/ksql/commit/19f4d632bc4c854904f366b359d4bf70a6f54d6d))
- fix how the buffer limit check evaluates streams config ([#6876](https://github.com/confluentinc/ksql/pull/6876)) ([ad1cc2a](https://github.com/confluentinc/ksql/commit/ad1cc2aca3b3a51f6f9e3aee86b1f41de449ae15))
- format cast arguments with passed context (6.0.x) ([#7032](https://github.com/confluentinc/ksql/pull/7032)) ([8c0d93d](https://github.com/confluentinc/ksql/commit/8c0d93db4ad94f820639ccb993dbc53a81175e53))
- make formattimestamp and parsetimestamp default to utc ([#6954](https://github.com/confluentinc/ksql/pull/6954)) ([a5ea98a](https://github.com/confluentinc/ksql/commit/a5ea98affd13b99d17c21832c09394e45b1f86ac))
- Make Select \* avoid code gen for projections ([#6846](https://github.com/confluentinc/ksql/pull/6846)) ([0896b85](https://github.com/confluentinc/ksql/commit/0896b856a86c1f7b212d8b5ebe3f9140f5679cd6))
- npe when getting topic configs ([#6946](https://github.com/confluentinc/ksql/pull/6946)) ([5e026d4](https://github.com/confluentinc/ksql/commit/5e026d4ad9d02629566593705e79d568296e96bd))
- remove mutable subscriber field on an endpoint ([#6905](https://github.com/confluentinc/ksql/pull/6905)) ([98c7d73](https://github.com/confluentinc/ksql/commit/98c7d735c2506a0204ecce8d8861f07eb62d968d))
- use the right name for fallback subject for transient queries ([#6821](https://github.com/confluentinc/ksql/pull/6821)) ([d044d64](https://github.com/confluentinc/ksql/commit/d044d645cefe6aba9ae495a47b3051413e55ff29)), closes [#6817](https://github.com/confluentinc/ksql/issues/6817)

## [0.15.0](https://github.com/confluentinc/ksql/releases/tag/v0.15.0-ksqldb) (2021-01-20)

### Highlights

- expose support for array and struct keys ([#6722](https://github.com/confluentinc/ksql/pull/6722)) ([c7fc2b0](https://github.com/confluentinc/ksql/commit/c7fc2b0df7b64bcd0370054eb9c82d2880a2de53))
- support PARTITION BY on multiple expressions ([#6803](https://github.com/confluentinc/ksql/pull/6803)) ([5a6b48e](https://github.com/confluentinc/ksql/commit/5a6b48efa63f0a0f007021c8cd73c71165b483fa))
- ungate support for multi-column GROUP BY ([#6786](https://github.com/confluentinc/ksql/pull/6786)) ([9900623](https://github.com/confluentinc/ksql/commit/9900623bdc669ad46c0321ef9f4989454bcd1108))

### Features

- expose AVRO and JSON_SR as key formats ([#6694](https://github.com/confluentinc/ksql/pull/6694)) ([07dc0c7](https://github.com/confluentinc/ksql/commit/07dc0c7ec138914d85e46d72a2aec45f6798cc46))
- support PROTOBUF keys ([#6692](https://github.com/confluentinc/ksql/pull/6692)) ([821faac](https://github.com/confluentinc/ksql/commit/821faacda8382fce1b74e4517dfd6ab704e93d46))
- add partitions to PRINT TOPIC output ([#6641](https://github.com/confluentinc/ksql/pull/6641)) ([1f4eff8](https://github.com/confluentinc/ksql/commit/1f4eff89b18e847db6f4c428f2474b5bd4bad391))
- Adds logging for every request to ksqlDB ([#6615](https://github.com/confluentinc/ksql/pull/6615)) ([57b0c91](https://github.com/confluentinc/ksql/commit/57b0c91380754e1de36527bd3254f67d42b9083e))
- optional `KAFKA_TOPIC` ([862c59e](https://github.com/confluentinc/ksql/commit/862c59e9c7eaa3ed06a9e75055db3cebe0ba0d89))
- support table joins on key format mismatch ([#6708](https://github.com/confluentinc/ksql/pull/6708)) ([989e52b](https://github.com/confluentinc/ksql/commit/989e52b241dc906289ac1607a4049ee1704a346a))
- cli to show tombstones in transient query output ([#6462](https://github.com/confluentinc/ksql/pull/6462)) ([ef3039a](https://github.com/confluentinc/ksql/commit/ef3039a078b7814efd5bdec932af4a42ba11ee71))
- new syntax to interact with session variables (define/undefine/show variables) ([#6474](https://github.com/confluentinc/ksql/pull/6474)) ([df98ef4](https://github.com/confluentinc/ksql/commit/df98ef4f26451656c051125383137a969d2292ee))
- terminate persistent query on DROP command ([#6143](https://github.com/confluentinc/ksql/pull/6143)) ([b5ac1bd](https://github.com/confluentinc/ksql/commit/b5ac1bd6f156447f9109545bacf3d74644000e75))
- update ksql restore command to skip incompatible commands if flag set ([#6524](https://github.com/confluentinc/ksql/pull/6524)) ([4d0c997](https://github.com/confluentinc/ksql/commit/4d0c997ad2f2889941fa6dcc8dd2e9d3e4e27d7d))

### Bug Fixes

- catch stack overflow error when parsing/preparing statements ([#6727](https://github.com/confluentinc/ksql/pull/6727)) ([37371cc](https://github.com/confluentinc/ksql/commit/37371cc427e93a3b7518587a6c817d7b390b2003))
- change locate() error message for a more user-friendly message ([#6709](https://github.com/confluentinc/ksql/pull/6709)) ([e6ba436](https://github.com/confluentinc/ksql/commit/e6ba436536a10678eb9cbafa150c3fd0501d31ab))
- CREATE IF NOT EXISTS does not work at all ([#6073](https://github.com/confluentinc/ksql/pull/6073)) ([6edf7ec](https://github.com/confluentinc/ksql/commit/6edf7ec27aa346198d2548287b1f9f11b9729a3b))
- don't create threads per request ([#6665](https://github.com/confluentinc/ksql/pull/6665)) ([132d50d](https://github.com/confluentinc/ksql/commit/132d50d7b1ee953d07dfa304e8b5eb7a7b705291))
- fix error categorization on NPE from streams ([#6655](https://github.com/confluentinc/ksql/pull/6655)) ([db6ad5b](https://github.com/confluentinc/ksql/commit/db6ad5b0ea14604e11cee8e2045c5b60b9c259d2))
- Fixes bug in latests-by-offset when using nulls and sessions windows ([#6699](https://github.com/confluentinc/ksql/pull/6699)) ([8ff52ca](https://github.com/confluentinc/ksql/commit/8ff52ca3e178c55e285e74f806d12f9d7f8f921c))
- include 'ksql.streams.topic.\*' prefix properties on LIST PROPERTIES output ([#6753](https://github.com/confluentinc/ksql/pull/6753)) ([8071af2](https://github.com/confluentinc/ksql/commit/8071af2385edd6e3bc5968c51197bf4983beb8da))
- LDAP Authentication ([#6800](https://github.com/confluentinc/ksql/pull/6800)) ([1db8b5b](https://github.com/confluentinc/ksql/commit/1db8b5b67ad0b642130023915b7c26b4fd6a9e51))
- Makes response codes rate limited as well as prints a message when it is hit ([#6701](https://github.com/confluentinc/ksql/pull/6701)) ([bdec3dd](https://github.com/confluentinc/ksql/commit/bdec3dd647c61c69ac7fff4c5e01b085070bc5bd))
- Removes orphaned topics from transient queries ([#6714](https://github.com/confluentinc/ksql/pull/6714)) ([06d6e3e](https://github.com/confluentinc/ksql/commit/06d6e3e7b6aafa659de5273af1244987d3f9b3ab))
- throw error message on create source with no value columns ([#6680](https://github.com/confluentinc/ksql/pull/6680)) ([14465a2](https://github.com/confluentinc/ksql/commit/14465a270d8249d5ea47fc1e6319eddd5eee5a48))
- allow reserved keywords on variables names ([#6572](https://github.com/confluentinc/ksql/pull/6572)) ([2da360a](https://github.com/confluentinc/ksql/commit/2da360adfaf96832ef9ad46717b627bff4418d59))
- Bypass window store cache when doing windowed pull queries ([#6548](https://github.com/confluentinc/ksql/pull/6548)) ([8f84e41](https://github.com/confluentinc/ksql/commit/8f84e417654fa1ee19ee2407587452405c447c21))
- cannot reference variables in DEFINE statement ([#6573](https://github.com/confluentinc/ksql/pull/6573)) ([ee31fde](https://github.com/confluentinc/ksql/commit/ee31fde6fa98ce2ab4581487d2d962bc793f3e88))
- Check for index before removing value in undo of COLLECT_LIST ([#6603](https://github.com/confluentinc/ksql/pull/6603)) ([2d92144](https://github.com/confluentinc/ksql/commit/2d92144b379b842bf205695cc9ce48342c19e333))
- propagate null-valued records in repartition ([#6647](https://github.com/confluentinc/ksql/pull/6647)) ([d3007f2](https://github.com/confluentinc/ksql/commit/d3007f255567d1457dc6755bc75f174be1ca3538))
- (minor) don't use deprecated jersey calls ([#6732](https://github.com/confluentinc/ksql/pull/6732)) ([a44b3e9](https://github.com/confluentinc/ksql/commit/a44b3e98abb188802dc61fd9cee99a53b6b1eea8))
- use Java's Base64 instead of jersey's ([#6702](https://github.com/confluentinc/ksql/pull/6702)) ([f9fb523](https://github.com/confluentinc/ksql/commit/f9fb523499da72dd8eb8bfc8c724a655222ecbdb))

### BREAKING CHANGES

- Queries with GROUP BY clauses that contain multiple grouping expressions now result in multiple key columns, one for each grouping expression, rather than a single key column that is the string-concatenation of the grouping expressions. Note that this new behavior (and breaking change) apply only to new queries; existing queries will continue to run uninterrupted with the previous behavior, even across ksqlDB server upgrades.
- stream-table key-to-key joins on mismatched formats will now repartition the table (right hand side) instead of the stream. Old enqueued commands will not be affected, so this change should remain invisible to the end-user.

## [0.14.0](https://github.com/confluentinc/ksql/releases/tag/v0.14.0-ksqldb) (2020-10-28)

### Highlights

- Add support for IN clause to pull queries ([#6409](https://github.com/confluentinc/ksql/pull/6409)) ([d5fc365](https://github.com/confluentinc/ksql/commit/d5fc3658a8d7df1fe267a416dfcd3e0721b8a9c4))
- Add support for ALTER STREAM|TABLE ([#6400](https://github.com/confluentinc/ksql/pull/6400)) ([a58e041](https://github.com/confluentinc/ksql/commit/a58e041cba721a1ad5f28bb0be1c792cb73bec14))
- support variable substitution in SQL statements ([#6504](https://github.com/confluentinc/ksql/pull/6504)) ([e185c1f](https://github.com/confluentinc/ksql/commit/e185c1fcf86821f5024e51736a77c6f2876fb9ab))
- enable support for `JSON` key format ([#6411](https://github.com/confluentinc/ksql/pull/6411)) ([fe97cde](https://github.com/confluentinc/ksql/commit/fe97cdeb51542417c9f0cac939fcba43ca5167eb))
- support for `DELIMITED` key format ([#6344](https://github.com/confluentinc/ksql/pull/6344)) ([04af65c](https://github.com/confluentinc/ksql/commit/04af65c0890bbf0996aaf4d8c753262c766c61ae))

### Features

- `NONE` format for key-less streams ([#6349](https://github.com/confluentinc/ksql/pull/6349)) ([25bb352](https://github.com/confluentinc/ksql/commit/25bb3520955d6f9ec64b0beeffe78069c2496a7f))
- add aggregated rocksdb metrics ([#6354](https://github.com/confluentinc/ksql/pull/6354)) ([ecc6625](https://github.com/confluentinc/ksql/commit/ecc6625b4d329e53c8b8a9ff5bf8d6d5bcd5f0c4))
- Add an endpoint for returning the query limit configuration ([#6353](https://github.com/confluentinc/ksql/pull/6353)) ([84d202d](https://github.com/confluentinc/ksql/commit/84d202de588fa3590f01ab75326a0db4325859ad))
- add commandRunnerCheck to healthcheck detail ([#6346](https://github.com/confluentinc/ksql/pull/6346)) ([5f64d05](https://github.com/confluentinc/ksql/commit/5f64d05540039282739526ec24c0b3ba7f0f67ae))
- Add metrics for pull query request/response size in bytes ([#6148](https://github.com/confluentinc/ksql/pull/6148)) ([946d2d3](https://github.com/confluentinc/ksql/commit/946d2d3e34746900235d3395ab4148d571b908e2))
- avoid spurious tombstones in table output ([#6405](https://github.com/confluentinc/ksql/pull/6405)) ([4c7c9b5](https://github.com/confluentinc/ksql/commit/4c7c9b51c49bf7157c28aa45f59f783139fe04dd))
- new CLI parameter to execute a command and quit (without CLI interaction) ([#6267](https://github.com/confluentinc/ksql/pull/6267)) ([0d60246](https://github.com/confluentinc/ksql/commit/0d60246f95bbd43e7f8c3f33105e351762461aa6))
- support Comparisons on complex types ([#6149](https://github.com/confluentinc/ksql/pull/6149)) ([0695213](https://github.com/confluentinc/ksql/commit/0695213f757d457ca4fd6ab67952a982874e16bd))
- new command to restore ksqlDB command topic backups ([#6361](https://github.com/confluentinc/ksql/pull/6361)) ([036df20](https://github.com/confluentinc/ksql/commit/036df2075f97f561edc44575d564e4bd983abea4))
- new CLI parameter (-f,--file) to execute commands from a file ([6440](https://github.com/confluentinc/ksql/pull/6440)) ([0e03a38 ](https://github.com/confluentinc/ksql/commit/0e03a387f03756a2ec2cfb2f8d09871b6b9627aa))
- new syntax to interact with session variables (define/undefine/show variables) ([6474](https://github.com/confluentinc/ksql/pull/6474)) ([df98ef4](https://github.com/confluentinc/ksql/commit/df98ef4f26451656c051125383137a969d2292ee))

### Bug Fixes

- [#6319](https://github.com/confluentinc/ksql/issues/6319) default port for CLI: set default port if not mentionned ([#6410](https://github.com/confluentinc/ksql/pull/6410)) ([d46b147](https://github.com/confluentinc/ksql/commit/d46b1474049efbc83f80515be6184d48f5f91d3b))
- add back configs for setting TLS protocols and cipher suites ([#6558](https://github.com/confluentinc/ksql/pull/6558)) ([cf7de69](https://github.com/confluentinc/ksql/commit/cf7de6903c33458bd21b811219ea145941aec267))
- avoid RUN SCRIPT to override CLI session variables/properties ([#6551](https://github.com/confluentinc/ksql/pull/6551)) ([8603ec8](https://github.com/confluentinc/ksql/commit/8603ec84a6f16f9ab1d1fc274fd81aa172713ece))
- backup files are re-created on every restart ([#6348](https://github.com/confluentinc/ksql/pull/6348)) ([28b8486](https://github.com/confluentinc/ksql/commit/28b84867d7e55b6ee2e27a692f42760d27fb5aca))
- check for nested UnspportedVersionException during auth op check ([#6467](https://github.com/confluentinc/ksql/pull/6467)) ([369c3f1](https://github.com/confluentinc/ksql/commit/369c3f152e936b92880f8614b15f0ee38d097ead))
- Internal Server Error for /healthcheck endpoint in RBAC-enabled ([#6482](https://github.com/confluentinc/ksql/pull/6482)) ([ebee5ec](https://github.com/confluentinc/ksql/commit/ebee5ecde99009413a3eafc928ae08374a3a8c92))
- JSON format to set correct scale of decimals ([#6295](https://github.com/confluentinc/ksql/pull/6295)) ([57b7b2e](https://github.com/confluentinc/ksql/commit/57b7b2edf475affb0a8d40d604ba9091cb6eb556))
- Properly clean up state when executing a command fails ([#6437](https://github.com/confluentinc/ksql/pull/6437)) ([242a32e](https://github.com/confluentinc/ksql/commit/242a32ecef79545436de3ae0b6cc5486a0b3cd95))
- recovery hangs when using TERMINATE ALL ([#6397](https://github.com/confluentinc/ksql/pull/6397)) ([7a57b3c](https://github.com/confluentinc/ksql/commit/7a57b3c898aba161351724dcee09fbf6b41b731b))
- support joins on key formats with different default serde features ([#6550](https://github.com/confluentinc/ksql/pull/6550)) ([61e4073](https://github.com/confluentinc/ksql/commit/61e407354f64e6fdcf43a3483a4ca4f80725746b))
- support unwrapped struct value inference ([#6446](https://github.com/confluentinc/ksql/pull/6446)) ([ed3ca5e](https://github.com/confluentinc/ksql/commit/ed3ca5e28de7ce703bd357a3cf0f65dc722fe502))
- NPE in PARTITON BY on null value ([#6508](https://github.com/confluentinc/ksql/pull/6508)) ([dbc7867](https://github.com/confluentinc/ksql/commit/dbc78674b9e84c9914ff6b01838b8a4c461ec1ff))
- clean up leaked admin client threads when issuing join query to query-stream endpoint ([#6532](https://github.com/confluentinc/ksql/pull/6532)) ([9cc7698](https://github.com/confluentinc/ksql/commit/9cc769897970d493bc5d9c3d6ac465a6f49ab45b))
- CASE expressions can now handle 12+ conditions in docker + cloud ([#6535](https://github.com/confluentinc/ksql/pull/6535)) ([#6541](https://github.com/confluentinc/ksql/issues/6541)) ([f6c39dc](https://github.com/confluentinc/ksql/commit/f6c39dc1a8e61f2b695f9f5ebc5ce265428d43b6))

### BREAKING CHANGES

- This change fixes a _bug_ where unnecessary tombstones where being emitted when a `HAVING` clause filtered out a row from the source that is not in the output table.
  For example, given:
  `sql -- source stream: CREATE STREAM FOO (ID INT KEY, VAL INT) WITH (...); -- aggregate into a table: CREATE TABLE BAR AS SELECT ID, SUM(VAL) AS SUM FROM FOO GROUP BY ID HAVING SUM(VAL) > 0; -- insert some values into the stream: INSERT INTO FOO VALUES(1, -5); INSERT INTO FOO VALUES(1, 6); INSERT INTO FOO VALUES(1, -2); INSERT INTO FOO VALUES(1, -1); `
  Where previously the contents of the sink topic `BAR` would have contained records:
  | Key | Value | Notes |
  |:----|:--------|:------------------------------------------------------------------------------------------------|
  | 1. | null. | Spurious tombstone: the table does not contain a row with key `1`, so no tombstone is required. |
  | 1. | {sum=1} | Row added as HAVING criteria now met |
  | 1. | null. | Row deleted as HAVING criteria now not met |
  | 1. | null. | Spurious tombstone: the table does not contain a row with key `1`, so no tombstone is required. |

        The topic will now contain:

  | Key | Value   |
  | :-- | :------ |
  | 1.  | {sum=1} |
  | 1.  | null.   |

- Adds support for using primitive types in joins. (#4132) ([d595985](https://github.com/confluentinc/ksql/commit/d595985853703f19611bac1a63957b447260b38e)), closes [#4132](https://github.com/confluentinc/ksql/issues/4132)

## [0.13.0](https://github.com/confluentinc/ksql/releases/tag/v0.13.0-ksqldb) (2020-09-29)

### Features

- add KSQL processing log message on uncaught streams exceptions ([#6253](https://github.com/confluentinc/ksql/pull/6253)) ([ac8875f](https://github.com/confluentinc/ksql/commit/ac8875f62d1e0e068386bcd7ff44f5c7dd74b8db))
- Adds support for 0x, X'...', x'...' type hex strings in udf:encode ([#6118](https://github.com/confluentinc/ksql/pull/6118)) ([d492556](https://github.com/confluentinc/ksql/commit/d492556d5ff38acc83487b4cefaa1c828ffb58b7))
- clarify key or value in (de)serialization processing log messages ([#6109](https://github.com/confluentinc/ksql/pull/6109)) ([7a16b91](https://github.com/confluentinc/ksql/commit/7a16b91850c6428bd71d858e4dfd100a0088df36))
- CommandRunner enters degraded state when corruption detected in metastore ([#6164](https://github.com/confluentinc/ksql/pull/6164)) ([2b29ee0](https://github.com/confluentinc/ksql/commit/2b29ee080633d320b2ec3c2daa975143bc6957eb))
- latest and earliest ByOffset UDFs to capture N values ([#6014](https://github.com/confluentinc/ksql/pull/6014)) ([96bb12a](https://github.com/confluentinc/ksql/commit/96bb12a51cacdd9cf4c349e3d205dabcb443fa9a))
- Support for IF NOT EXISTS on CREATE CONNECTOR ([#6036](https://github.com/confluentinc/ksql/pull/6036)) ([8466197](https://github.com/confluentinc/ksql/commit/8466197627bd6f15616c7668180111380e073dcc))
- Support IF NOT EXISTS on CREATE TYPE ([#6173](https://github.com/confluentinc/ksql/pull/6173)) ([a0f381b](https://github.com/confluentinc/ksql/commit/a0f381b1bce8ae58b658f51463001256d2fc3721))
- support PARTITION BY NULL for creating keyless stream ([#6096](https://github.com/confluentinc/ksql/pull/6096)) ([81e3142](https://github.com/confluentinc/ksql/commit/81e31420d32aea3f3b3efea815150cdef0d79b21))
- surface error to user when command topic deleted while server running ([#6240](https://github.com/confluentinc/ksql/pull/6240)) ([c5d6b56](https://github.com/confluentinc/ksql/commit/c5d6b56a3188f100ea473ef8f13959da79d1c79e))

### Performance Improvements

- substantially improve avro deserialization ([#6201](https://github.com/confluentinc/ksql/pull/6201)) ([325a009](https://github.com/confluentinc/ksql/commit/325a00911648b2a17bd9c4e7126a265c9a1cadd4))

### Bug Fixes

- allow expressions in flat map ([#6163](https://github.com/confluentinc/ksql/pull/6163)) ([52a897b](https://github.com/confluentinc/ksql/commit/52a897b3ce4f3090bf373e5643546baa3b361b42))
- create /var/lib/kafka-streams directory on RPM installation ([#6126](https://github.com/confluentinc/ksql/pull/6126)) ([31ed3d3](https://github.com/confluentinc/ksql/commit/31ed3d3aca2de26fb44ca42bb651a1ddfc86f671))
- delete zombie consumer groups 🧟 ([#6160](https://github.com/confluentinc/ksql/pull/6160)) ([2d1697a](https://github.com/confluentinc/ksql/commit/2d1697a9014c397ab8924cd12102cf374ebb166f))
- delimited format should write decimals in a format it can read ([#6238](https://github.com/confluentinc/ksql/pull/6238)) ([626965e](https://github.com/confluentinc/ksql/commit/626965e542cbca45351f5c37d9fc52fe1164edd4))
- don't use queryId of last terminate command after restore ([#6278](https://github.com/confluentinc/ksql/pull/6278)) ([2753ccd](https://github.com/confluentinc/ksql/commit/2753ccd7295d978e967ec309aa062334c16b36a1))
- earliest/latest_by_offset should accept nulls ([#5729](https://github.com/confluentinc/ksql/pull/5729)) ([6eb5a41](https://github.com/confluentinc/ksql/commit/6eb5a414d7e69795563a21dadc45bdf094edb8eb))
- fail on non-string MAP keys ([#6182](https://github.com/confluentinc/ksql/pull/6182)) ([9d4cc6d](https://github.com/confluentinc/ksql/commit/9d4cc6d1800c31879321f06c905c4f9175ca175f))
- improve error handling of invalid Avro identifier ([#6239](https://github.com/confluentinc/ksql/pull/6239)) ([8dd3942](https://github.com/confluentinc/ksql/commit/8dd394246094d37b50a5ddc4058b1bc22deb8339))
- missing topic classifier now uses MissingSourceTopicException ([#6172](https://github.com/confluentinc/ksql/pull/6172)) ([cf8e15d](https://github.com/confluentinc/ksql/commit/cf8e15d8b7de874e22175765730b4068b88b0c12))
- register correct unwrapped schema ([#6188](https://github.com/confluentinc/ksql/pull/6188)) ([cb25f9c](https://github.com/confluentinc/ksql/commit/cb25f9c53f32997190f58ddfa6585f565b05ca67))
- scale of ROUND() return value ([#6236](https://github.com/confluentinc/ksql/pull/6236)) ([42ab721](https://github.com/confluentinc/ksql/commit/42ab72160f4ae665cbdead1e3ffced4df534335d))

## [0.12.0](https://github.com/confluentinc/ksql/releases/tag/v0.12.0-ksqldb) (2020-09-01)

### Features

- support CREATE OR REPLACE w/ config guard but w/o restrictions ([#5766](https://github.com/confluentinc/ksql/pull/5766)) ([e7ff81a](https://github.com/confluentinc/ksql/commit/e7ff81a9f8d7d2f6852c729ec0edc3deef5fc6f0))
- add a serverStatus to ServerInfo and display the status in the CLI ([#6040](https://github.com/confluentinc/ksql/pull/6040)) ([1921d0e](https://github.com/confluentinc/ksql/commit/1921d0eaf7f9005014cb5c1158fa12e29f1c2fd2))
- Add consumer offsets to DESCRIBE EXTENDED ([#5476](https://github.com/confluentinc/ksql/pull/5476)) ([9ce3c97](https://github.com/confluentinc/ksql/commit/9ce3c9746d21d544e6d39dde4c2b6932d738a5e0))
- add Ksql warning to KsqlResource response when CommandRunner degraded ([#6039](https://github.com/confluentinc/ksql/pull/6039)) ([6d547da](https://github.com/confluentinc/ksql/commit/6d547da0be73df38ac64d351e68db858f4631f9b))
- add serialization exceptions to processing logger ([#6084](https://github.com/confluentinc/ksql/pull/6084)) ([8ab98a5](https://github.com/confluentinc/ksql/commit/8ab98a5b5bee3335c2d791a7b9bcd5b2ccfd5736))
- add service to restart failed persistent queries ([#5807](https://github.com/confluentinc/ksql/pull/5807)) ([0ffe3e2](https://github.com/confluentinc/ksql/commit/0ffe3e2872726d8e800e8d58696413746c4971b9))
- Allow udfs to be provided via a new flag to KsqlTestingTool ([#5964](https://github.com/confluentinc/ksql/pull/5964)) ([a85ef14](https://github.com/confluentinc/ksql/commit/a85ef142bfd3202f9f4f0ddb09cb18e2fadee35a))
- CommandRunner enters degraded states when it processes command with higher version than it supports ([#6032](https://github.com/confluentinc/ksql/pull/6032)) ([a841443](https://github.com/confluentinc/ksql/commit/a841443c75b83c8fbaffaf79266ae219f2729473))
- DistributingExecutor fails DDL statement if CommandRunner DEGRADED ([#6031](https://github.com/confluentinc/ksql/pull/6031)) ([62b6d9a](https://github.com/confluentinc/ksql/commit/62b6d9ae4af4a0e239558cb3eb68ce92b1cebcc5))
- Enable datagen to set the message timestamp ([#5849](https://github.com/confluentinc/ksql/pull/5849)) ([3cba869](https://github.com/confluentinc/ksql/commit/3cba869362041e9e98ae8d14716c335aa7228909))
- hard delete schemas for push queries ([#6061](https://github.com/confluentinc/ksql/pull/6061)) ([a036b8b](https://github.com/confluentinc/ksql/commit/a036b8babc9dbf36a7625dbb4ce267c2d68cde88))
- introduce the sql-based testing tool (YATT) ([#6051](https://github.com/confluentinc/ksql/pull/6051)) ([33e71c3](https://github.com/confluentinc/ksql/commit/33e71c30a1eea305f2a98d855346275859fd0325))
- move command topic deserialization to CommandRunner and introduce DEGRADED CommandRunnerStatus ([#6012](https://github.com/confluentinc/ksql/pull/6012)) ([ab8cec2](https://github.com/confluentinc/ksql/commit/ab8cec26d8f2f87ad782e96d49fc3c8762bbf26b))
- New ksql.properties.overrides.denylist to deny clients configs overrides ([#5877](https://github.com/confluentinc/ksql/pull/5877)) ([7d1ad25](https://github.com/confluentinc/ksql/commit/7d1ad25f751dd670ef027a48529613424e3aed3e))
- Support [IF EXISTS] on DROP TYPE command ([#5962](https://github.com/confluentinc/ksql/pull/5962)) ([431c2ff](https://github.com/confluentinc/ksql/commit/431c2ff651a455d1c327616fa7f8bd005ffe79f4))
- Support IF EXISTS keyword on DROP CONNECTOR ([#6067](https://github.com/confluentinc/ksql/pull/6067)) ([2c99df9](https://github.com/confluentinc/ksql/commit/2c99df9104c562385fe93cef13340e5be716928a))
- Support subscript and nested functions in grouping queries ([#5998](https://github.com/confluentinc/ksql/pull/5998)) ([8d383db](https://github.com/confluentinc/ksql/commit/8d383db7d730bc3a63f462994913ead5146e7edd))
- **client:** support describe source in Java client ([#5944](https://github.com/confluentinc/ksql/pull/5944)) ([a154373](https://github.com/confluentinc/ksql/commit/a154373f85373a11212e8d635418648550e94ce5))
- support UDAFs with and without init Args with same param type ([#5982](https://github.com/confluentinc/ksql/pull/5982)) ([0bf3296](https://github.com/confluentinc/ksql/commit/0bf3296b51cd85bc8736c76be87f5e7a5d387492))

### Bug Fixes

- allow implicit cast of numbers literals to decimals on insert/select ([#6005](https://github.com/confluentinc/ksql/pull/6005)) ([2bc15dd](https://github.com/confluentinc/ksql/commit/2bc15ddeec9938e3613cce20d1dbbd7af120c899))
- allow joins in windowed aggregations ([9452ab6](https://github.com/confluentinc/ksql/commit/9452ab6d072882264c60042b655af52c5dc07cbf)), closes [#5898](https://github.com/confluentinc/ksql/issues/5898) [#5931](https://github.com/confluentinc/ksql/issues/5931)
- avoid losing cause of processing errors ([#5937](https://github.com/confluentinc/ksql/pull/5937)) ([014c049](https://github.com/confluentinc/ksql/commit/014c049cfd6b964ae63ac39185ac44b7370c5fbb))
- NPE when udf metrics enabled ([#5960](https://github.com/confluentinc/ksql/pull/5960)) ([e32183f](https://github.com/confluentinc/ksql/commit/e32183fb4d77371ed3f4b610f5abf68d888c65f4))
- protobuf format does not support unwrapping ([#6033](https://github.com/confluentinc/ksql/pull/6033)) ([6c08e5d](https://github.com/confluentinc/ksql/commit/6c08e5dd0f374518db2d22a39f829961af339503))
- remove unnecessary parser token ([#6048](https://github.com/confluentinc/ksql/pull/6048)) ([a6c3864](https://github.com/confluentinc/ksql/commit/a6c386423e0d069c1693de1bd5524de906cdc29e))
- set restarted query as healthy during a time threshold ([#6018](https://github.com/confluentinc/ksql/pull/6018)) ([ae5c215](https://github.com/confluentinc/ksql/commit/ae5c2153c73e3307528b1434e593dbd6d20826c7))
- Use a SandboxedPersistentQueryMetadata to not interact with KafkStreams ([#6066](https://github.com/confluentinc/ksql/pull/6066)) ([e000b41](https://github.com/confluentinc/ksql/commit/e000b419ac84d7ee25f47aae62758dce01fbe3de))
- Uses pull query metrics for all paths, not just /query ([#5983](https://github.com/confluentinc/ksql/pull/5983)) ([143849c](https://github.com/confluentinc/ksql/commit/143849c89aa2cd69f6aee813cca660540b4675d4))

## [0.11.0](https://github.com/confluentinc/ksql/releases/tag/v0.11.0-ksqldb) (2020-08-03)

### Features

- **client:** support streaming inserts in Java client ([#5641](https://github.com/confluentinc/ksql/pull/5641)) ([1e109bf](https://github.com/confluentinc/ksql/commit/1e109bf))
- **client:** support admin operations in Java client ([#5671](https://github.com/confluentinc/ksql/pull/5671)) ([7d0079a](https://github.com/confluentinc/ksql/commit/7d0079a))
- **client:** support list queries in Java client ([#5682](https://github.com/confluentinc/ksql/pull/5682)) ([4d860f8](https://github.com/confluentinc/ksql/commit/4d860f8))
- **client:** support DDL/DML statements in Java client ([#5775](https://github.com/confluentinc/ksql/pull/5775)) ([53ca76f](https://github.com/confluentinc/ksql/commit/53ca76f))
- support WINDOWEND in WHERE of pull queries ([#5680](https://github.com/confluentinc/ksql/pull/5680)) ([40f2f13](https://github.com/confluentinc/ksql/commit/40f2f13))
- Adds SSL mutual auth support to intra-cluster requests ([#5482](https://github.com/confluentinc/ksql/pull/5482)) ([82b137f](https://github.com/confluentinc/ksql/commit/82b137f))
- new array_remove UDF ([#5843](https://github.com/confluentinc/ksql/pull/5843)) ([4ebeff2](https://github.com/confluentinc/ksql/commit/4ebeff2))
- Replay command topic to local file to backup KSQL Metastore ([#5831](https://github.com/confluentinc/ksql/pull/5831)) ([8523051](https://github.com/confluentinc/ksql/commit/8523051))
- organize UDFs by category ([#5813](https://github.com/confluentinc/ksql/pull/5813)) ([7f5b843](https://github.com/confluentinc/ksql/commit/7f5b843))
- expose query ID in CommandStatusEntity (MINOR) ([#5814](https://github.com/confluentinc/ksql/pull/5814)) ([bb29185](https://github.com/confluentinc/ksql/commit/bb29185))

### Bug Fixes

- circumvent KAFKA-10179 by forcing changelog topics for tables ([#5781](https://github.com/confluentinc/ksql/pull/5781)) ([ef8fa4f](https://github.com/confluentinc/ksql/commit/ef8fa4f))
- always use the changelog subject in table state stores ([#5823](https://github.com/confluentinc/ksql/pull/5823)) ([e69acb4](https://github.com/confluentinc/ksql/commit/e69acb4))
- remove schema compat check if schema exists ([#5872](https://github.com/confluentinc/ksql/pull/5872)) ([6338270](https://github.com/confluentinc/ksql/commit/6338270))
- ensure null values cast to varchar/string remain null ([#5769](https://github.com/confluentinc/ksql/pull/5769)) ([530eb7f](https://github.com/confluentinc/ksql/commit/530eb7f))
- ksqlDB should not truncate decimals ([#5763](https://github.com/confluentinc/ksql/pull/5763)) ([ba833f7](https://github.com/confluentinc/ksql/commit/ba833f7))
- Make sure UDTF describe shows actual function description ([#5744](https://github.com/confluentinc/ksql/pull/5744)) ([afe85d9](https://github.com/confluentinc/ksql/commit/afe85d9))
- Reuse KsqlClient instance for inter node requests ([#5742](https://github.com/confluentinc/ksql/pull/5742)) ([cd7f540](https://github.com/confluentinc/ksql/commit/cd7f540))
- SEC-1034: log4j migration to confluent repackaged version ([#5783](https://github.com/confluentinc/ksql/pull/5783)) ([4563d02](https://github.com/confluentinc/ksql/commit/4563d02))
- show overridden props in CLI ([#5750](https://github.com/confluentinc/ksql/pull/5750)) ([f6fd2ee](https://github.com/confluentinc/ksql/commit/f6fd2ee))
- simplify pull query error message ([#5672](https://github.com/confluentinc/ksql/pull/5672)) ([9bc4755](https://github.com/confluentinc/ksql/commit/9bc4755))
- Upgrade to Vert.x 3.9.1 which depends on version of Netty which allows backported ALPN in JDK 1.8.0_252 to be used, and provide warning if openSSL is not installed ([#5818](https://github.com/confluentinc/ksql/pull/5818)) ([36e44a6](https://github.com/confluentinc/ksql/commit/36e44a6))
- windowed tables now have cleanup policy compact+delete ([#5743](https://github.com/confluentinc/ksql/pull/5743)) ([2038770](https://github.com/confluentinc/ksql/commit/2038770))
- configure topic retention based on retention clause for windowed tables ([#5835](https://github.com/confluentinc/ksql/pull/5835)) ([b509c99](https://github.com/confluentinc/ksql/commit/b509c99))
- set Schema Registry port in tutorials docker compose ([f46d358](https://github.com/confluentinc/ksql/commit/f46d358))
- create the metastore backups directory if it does not exist ([#5879](https://github.com/confluentinc/ksql/pull/5879)) ([d77d8b7](https://github.com/confluentinc/ksql/commit/d77d8b7cadc8bdbf412cd58b0db82ff89a1b5045))
- close query on invalid use of HTTP/2 with /query endpoint ([#5883](https://github.com/confluentinc/ksql/pull/5883)) ([bcab116](https://github.com/confluentinc/ksql/commit/bcab116))
- adds a handler to gracefully shutdown ([#5895](https://github.com/confluentinc/ksql/pull/5895)) ([5fbf171](https://github.com/confluentinc/ksql/commit/5fbf171))

### BREAKING CHANGES

- ksqlDB now creates windowed tables with cleanup policy "compact,delete", rather than "compact". Also, topics that back streams are always created with cleanup policy "delete", rather than the broker default (by default, "delete").

## [0.10.2](https://github.com/confluentinc/ksql/releases/tag/v0.10.2-ksqldb) (2020-10-05)

### Bug Fixes

- adds a handler to gracefully shutdown ([#5895](https://github.com/confluentinc/ksql/pull/5895)) ([5fbf171](https://github.com/confluentinc/ksql/commit/5fbf17188282684834ba45077b45ea65c028c9cb))
- allow expressions in flat map ([#6165](https://github.com/confluentinc/ksql/pull/6165)) ([b6ad9bc](https://github.com/confluentinc/ksql/commit/b6ad9bccf1fbac1ade2c586fa6bd40b33b19d35a))
- allow joins in windowed aggregations ([9452ab6](https://github.com/confluentinc/ksql/commit/9452ab6d072882264c60042b655af52c5dc07cbf)), closes [#5898](https://github.com/confluentinc/ksql/issues/5898) [#5931](https://github.com/confluentinc/ksql/issues/5931)
- always use the changelog subject in table state stores ([#5823](https://github.com/confluentinc/ksql/pull/5823)) ([#5837](https://github.com/confluentinc/ksql/issues/5837)) ([c87aa69](https://github.com/confluentinc/ksql/commit/c87aa6940aa480dbecf1393ff16a70d53a9d51d5))
- avoid losing cause of processing errors ([#5937](https://github.com/confluentinc/ksql/pull/5937)) ([014c049](https://github.com/confluentinc/ksql/commit/014c049cfd6b964ae63ac39185ac44b7370c5fbb))
- close query on invalid use of HTTP/2 with /query endpoint ([#5885](https://github.com/confluentinc/ksql/pull/5885)) ([0b75411](https://github.com/confluentinc/ksql/commit/0b75411628df58ccb68ca04501e99828e63b22f3))
- create /var/lib/kafka-streams directory on RPM installation ([#6126](https://github.com/confluentinc/ksql/pull/6126)) ([31ed3d3](https://github.com/confluentinc/ksql/commit/31ed3d3aca2de26fb44ca42bb651a1ddfc86f671))
- ksqlDB should not truncate decimals ([#5763](https://github.com/confluentinc/ksql/pull/5763)) ([ba833f7](https://github.com/confluentinc/ksql/commit/ba833f7849f101f5faacba490faad30a551795c7))
- remove schema compat check if schema exists ([#5871](https://github.com/confluentinc/ksql/pull/5871)) ([9bcb5b0](https://github.com/confluentinc/ksql/commit/9bcb5b05c892a1ae437ca25281afc08263cae70f))
- Reuse KsqlClient instance for inter node requests ([#5742](https://github.com/confluentinc/ksql/pull/5742)) ([#5844](https://github.com/confluentinc/ksql/issues/5844)) ([7645abd](https://github.com/confluentinc/ksql/commit/7645abd7c8e00e3c0e83796fe592357684776a90))
- windowed table topic retention fixes ([#5842](https://github.com/confluentinc/ksql/pull/5842)) ([b3db23c](https://github.com/confluentinc/ksql/commit/b3db23ca536ffe32c3fd78449d5554568bc1118a))

### BREAKING CHANGES

- ksqlDB now creates windowed tables with cleanup policy "compact,delete", rather than "compact". Also, topics that back streams are always created with cleanup policy "delete", rather than the broker default (by default, "delete").

## [0.10.1](https://github.com/confluentinc/ksql/releases/tag/v0.10.1-ksqldb) (2020-07-09)

### Bug Fixes

- allow empty structs in schema inference ([#5656](https://github.com/confluentinc/ksql/pull/5656)) ([3c38c8c](https://github.com/confluentinc/ksql/commit/3c38c8ce3771ad3418645f81ee0caf1370e784b1))
- do not overwrite schemas from a CREATE STREAM/TABLE ([#5756](https://github.com/confluentinc/ksql/pull/5756)) ([5aa0b72](https://github.com/confluentinc/ksql/commit/5aa0b7241b46e54a821eef06b3a1e64b31ccdbaa))
- make sure old query stream doesn't block on close ([#5730](https://github.com/confluentinc/ksql/pull/5730)) ([663a67b](https://github.com/confluentinc/ksql/commit/663a67b0078d8d97fecdf7cbaa5c7c5d80b434d2))
- query w/ scoped all columns, join and where clause ([#5684](https://github.com/confluentinc/ksql/pull/5684)) ([304eb0c](https://github.com/confluentinc/ksql/commit/304eb0c48530068c02839f216aca0f71fc84f7ca))
- support CASE statements returning NULL ([#5703](https://github.com/confluentinc/ksql/pull/5703)) ([5062942](https://github.com/confluentinc/ksql/commit/506294245d83dc741e8da3046a584e47c7b05a99))
- UDTF don't return null on keyless stream ([#5761](https://github.com/confluentinc/ksql/pull/5761)) ([190f9e2](https://github.com/confluentinc/ksql/commit/190f9e265a6360c6308cc4e39a378e12e9ad1cbb))

## [0.10.0](https://github.com/confluentinc/ksql/releases/tag/v0.10.0-ksqldb) (2020-06-25)

### Features

- Any key name ([#5093](https://github.com/confluentinc/ksql/pull/5093)) ([1f0ca3e](https://github.com/confluentinc/ksql/commit/1f0ca3efb0f5ecadc7a604f219cbc16b2e120d8d))
- Explicit keys ([#5533](https://github.com/confluentinc/ksql/pull/5533)) ([d0db0cf](https://github.com/confluentinc/ksql/commit/d0db0cfac050cef94019c6daa59cd765ca0f7379))
- add extra log messages for pull queries ([#4909](https://github.com/confluentinc/ksql/pull/4909)) ([d622ecc](https://github.com/confluentinc/ksql/commit/d622eccb845698c245340556b6b4965fec075305))
- Adds the ability have internal endpoints listen on ksql.internal.listener ([#5212](https://github.com/confluentinc/ksql/pull/5212)) ([46acb73](https://github.com/confluentinc/ksql/commit/46acb73f8e0df7118876e1d991fd8ba0946905f7))
- create MetricsContext for ksql metrics reporters ([#5528](https://github.com/confluentinc/ksql/pull/5528)) ([50561a5](https://github.com/confluentinc/ksql/commit/50561a55cdf7a4264481ca51de8d3db17d3aeea6))
- drop WITH(KEY) syntax ([#5363](https://github.com/confluentinc/ksql/pull/5363)) ([bb43d23](https://github.com/confluentinc/ksql/commit/bb43d23ad36ec3519a06d05c25ca99f726244ad0))
- expose JMX metric that classifies an error state ([#5374](https://github.com/confluentinc/ksql/pull/5374)) ([52271bf](https://github.com/confluentinc/ksql/commit/52271bf6ccbfdb26e83e5c30c2e11db0c72e8efc))
- Expose Vert.x metrics ([#5340](https://github.com/confluentinc/ksql/pull/5340)) ([e82f762](https://github.com/confluentinc/ksql/commit/e82f7626236cc62d98e62e1d51ff33e4d6ff1519))
- introduce RegexClassifier for classifying errors via cfg ([#5412](https://github.com/confluentinc/ksql/pull/5412)) ([b25dd98](https://github.com/confluentinc/ksql/commit/b25dd987b219dcb8a3105beb7251503828ade023))
- Pull Queries: QPS check utilizes internal API flag to determine if forwarded ([#5392](https://github.com/confluentinc/ksql/pull/5392)) ([08b428f](https://github.com/confluentinc/ksql/commit/08b428f368a6f6238f9ffd9caab9f27afad9d1b9))
- reload TLS certificate without restarting server ([#5516](https://github.com/confluentinc/ksql/pull/5516)) ([a5920b0](https://github.com/confluentinc/ksql/commit/a5920b0f31fe2f32adcfe8cd03e1f2bd14c1368e))
- support TIMESTAMP being a key column ([#5542](https://github.com/confluentinc/ksql/pull/5542)) ([286ce08](https://github.com/confluentinc/ksql/commit/286ce0850e43e06ef0c7263546d31946c3c05302))
- turn on snappy compression for produced data ([#5495](https://github.com/confluentinc/ksql/pull/5495)) ([27d8ad5](https://github.com/confluentinc/ksql/commit/27d8ad5d3d9d2f731b0281a525b5e80edb24a637))
- **client:** Java client with push + pull query support ([#5200](https://github.com/confluentinc/ksql/pull/5200)) ([280ef0c](https://github.com/confluentinc/ksql/commit/280ef0ca8aa02693db1427ece08cd7863abed98d))
- **client:** support (non-streaming) insert into in Java client ([#5448](https://github.com/confluentinc/ksql/pull/5448)) ([9e8234a](https://github.com/confluentinc/ksql/commit/9e8234ad93745f73dc8e0d7463e44f33bb0739a9))
- **client:** support push query termination in Java client ([#5371](https://github.com/confluentinc/ksql/pull/5371)) ([62dacca](https://github.com/confluentinc/ksql/commit/62dacca7efc8fc2a4202c69caa58596051c67fee))
- New UDF/UDAF
  - Adds UDF regexp_extract_all ([#5507](https://github.com/confluentinc/ksql/pull/5507)) ([e19233c](https://github.com/confluentinc/ksql/commit/e19233c6bba4e5f4be9a93c224265c949626d317))
  - Adds UDF regexp_replace ([#5504](https://github.com/confluentinc/ksql/pull/5504)) ([30309bf](https://github.com/confluentinc/ksql/commit/30309bf07fbc706fb5200996ce33318d379cd97f))
  - Adds udf regexp_split_to_array ([#5501](https://github.com/confluentinc/ksql/pull/5501)) ([3766129](https://github.com/confluentinc/ksql/commit/3766129fe4511cedc2f7102427944876fb927a8a))
  - new UDFs for array max/min/sort ([#5505](https://github.com/confluentinc/ksql/pull/5505)) ([415d930](https://github.com/confluentinc/ksql/commit/415d93049670d30d48d68b294f7eb724fb8f90c8))
  - new UDFs for set-like operations on Arrays ([#5548](https://github.com/confluentinc/ksql/pull/5548)) ([50428c7](https://github.com/confluentinc/ksql/commit/50428c74089dddd43fb96e8299a28c507c615e4a))
  - new UDFs for working with Maps ([#5536](https://github.com/confluentinc/ksql/pull/5536)) ([bc9ad2e](https://github.com/confluentinc/ksql/commit/bc9ad2ea93b08ace708d6e74684582fa3e9c2d4d))
  - new UUID UDF ([#5535](https://github.com/confluentinc/ksql/pull/5535)) ([cfa65da](https://github.com/confluentinc/ksql/commit/cfa65da59ee057840a6c394e3038eb741a161e06))
  - new split_to_map udf ([#5563](https://github.com/confluentinc/ksql/pull/5563)) ([a68b9ad](https://github.com/confluentinc/ksql/commit/a68b9add94ddb9f97e1cff51fb57dddd1c9458eb))
  - new string UDFs LPad, RPad ([#5546](https://github.com/confluentinc/ksql/pull/5546)) ([00f5083](https://github.com/confluentinc/ksql/commit/00f5083cf3320ddb773d25cc160fffb414173ddc))
  - implement earliest_by_offset() UDAF ([#5273](https://github.com/confluentinc/ksql/pull/5273)) ([2a356ac](https://github.com/confluentinc/ksql/commit/2a356acbc1d55c868f0cde95b72835476615f8d6))
  - INSTR function [#881](https://github.com/confluentinc/ksql/issues/881) ([#5385](https://github.com/confluentinc/ksql/pull/5385)) ([ca86bbf](https://github.com/confluentinc/ksql/commit/ca86bbfad51e479d6847f84a0c7928f31d8a2b26))
  - add CHR UDF ([#5559](https://github.com/confluentinc/ksql/pull/5559)) ([5a746e8](https://github.com/confluentinc/ksql/commit/5a746e85502235753cf11bfa507ab69e1e19064c))
  - implements ARRAY_JOIN as requested in ([#5028](https://github.com/confluentinc/ksql/pull/5028)) ([#5474](https://github.com/confluentinc/ksql/issues/5474)) ([#5638](https://github.com/confluentinc/ksql/issues/5638)) ([6c67866](https://github.com/confluentinc/ksql/commit/6c678665cee4a67e7737460710eac2875bc8c2e2))
  - Add encode udf ([#5523](https://github.com/confluentinc/ksql/pull/5523)) ([b02f1ce](https://github.com/confluentinc/ksql/commit/b02f1ce8ae564688eda385268a51b1217dc9e277))

### Bug Fixes

- /inserts-stream endpoint now accepts complex types ([#5469](https://github.com/confluentinc/ksql/pull/5469)) ([0840160](https://github.com/confluentinc/ksql/commit/0840160e81b52b7cd0d55c6af7581c6be0a9eb85))
- allow dynamic construction of an ARRAY of STRUCTS with duplicate values [#5436](https://github.com/confluentinc/ksql/issues/5436) ([#5506](https://github.com/confluentinc/ksql/pull/5506)) ([0b1162c](https://github.com/confluentinc/ksql/commit/0b1162cc94bef391531bdc33b726aede4b3aab04))
- allow setting auto.offset.reset=latest on /query-stream endpoint ([#5455](https://github.com/confluentinc/ksql/pull/5455)) ([d91c016](https://github.com/confluentinc/ksql/commit/d91c01625acab2e6c3ce56339ec76d9f5b7a81aa))
- allow structs in schema provider return types ([#5287](https://github.com/confluentinc/ksql/pull/5287)) ([2e604f0](https://github.com/confluentinc/ksql/commit/2e604f04daf56e720927c9f66219853e0d4fabef))
- Allow value delimiter to be specified for datagen ([#5332](https://github.com/confluentinc/ksql/pull/5332)) ([865e834](https://github.com/confluentinc/ksql/commit/865e834f9c075705cbe43515925943149e638984))
- avoid unnecessary warnings from PRINT ([#5459](https://github.com/confluentinc/ksql/pull/5459)) ([080fba9](https://github.com/confluentinc/ksql/commit/080fba94770bfccaa78db2e26e6ed6424b93ee2f))
- Block writer thread if response output buffer is full ([#5386](https://github.com/confluentinc/ksql/pull/5386)) ([0edda40](https://github.com/confluentinc/ksql/commit/0edda40c04f25d26a60cb135a3827198f4d5c684))
- deals with issue [#5521](https://github.com/confluentinc/ksql/issues/5521) by adding more descriptive error message ([#5529](https://github.com/confluentinc/ksql/pull/5529)) ([86f8b67](https://github.com/confluentinc/ksql/commit/86f8b67ba6855dbdb8f77c15657aeab567b15bb7))
- disallow requests to /inserts-stream if insert values disabled ([#5592](https://github.com/confluentinc/ksql/pull/5592)) ([e277f25](https://github.com/confluentinc/ksql/commit/e277f2541f35f329339f1d617c7ca60d632a1cba))
- exclude window bounds from persistent query value & schema match ([#5425](https://github.com/confluentinc/ksql/pull/5425)) ([3596ceb](https://github.com/confluentinc/ksql/commit/3596ceb09965faf09e2a9e0e2460126b8c9ebf0b))
- fail AVRO/Protobuf/JSON Schema statements if SR is missing ([#5597](https://github.com/confluentinc/ksql/pull/5597)) ([85a0320](https://github.com/confluentinc/ksql/commit/85a0320e774d236bda4898f82eb5d5c2f7b42cec))
- fail on WINDOW clause without matching GROUP BY ([#5431](https://github.com/confluentinc/ksql/pull/5431)) ([68354d4](https://github.com/confluentinc/ksql/commit/68354d488aec07dc0bc9a7354ee8b8794bfef4e6))
- improve print topic ([#5552](https://github.com/confluentinc/ksql/pull/5552)) ([e193576](https://github.com/confluentinc/ksql/commit/e193576c0f6c0295da6f4357391639e49a45863b))
- Improve pull query error logging ([#5477](https://github.com/confluentinc/ksql/pull/5477)) ([f23412e](https://github.com/confluentinc/ksql/commit/f23412e9103531d2b8ce96cfcee3eff92784239b))
- KSQL does not accept more queries when running QueryLimit - 1 queries ([#5461](https://github.com/confluentinc/ksql/pull/5461)) ([d64f1bc](https://github.com/confluentinc/ksql/commit/d64f1bc7ba4359b3d7a8cf3ba28d315eef47389e))
- make stream and column names case-insensitive in /inserts-stream ([#5591](https://github.com/confluentinc/ksql/pull/5591)) ([e9e3042](https://github.com/confluentinc/ksql/commit/e9e3042f1bdbd6da384146410f866aab6b12b987))
- NPE in latest_by_offset if first element being processed has null… ([#4975](https://github.com/confluentinc/ksql/pull/4975)) ([a9668d2](https://github.com/confluentinc/ksql/commit/a9668d245db797e193d1e4bcc8734eab29ba99b3))
- Prevent memory leaks caused by pull query logging ([#5532](https://github.com/confluentinc/ksql/pull/5532)) ([723b6cb](https://github.com/confluentinc/ksql/commit/723b6cb764950a4d9d639f5d40c4b4e6c615fba6))
- remove leading zeros when casting decimal to string ([#5270](https://github.com/confluentinc/ksql/pull/5270)) ([e1cc8ad](https://github.com/confluentinc/ksql/commit/e1cc8ad8db9f9f34c3a2f26b60c3d0a7631b23a2))
- Remove stacktrace from error message ([#5478](https://github.com/confluentinc/ksql/pull/5478)) ([b63d7e8](https://github.com/confluentinc/ksql/commit/b63d7e8d3fb325a74120e40e3fc96d51e9c35856))
- Retry on connection closed ([#5515](https://github.com/confluentinc/ksql/pull/5515)) ([8eb1f88](https://github.com/confluentinc/ksql/commit/8eb1f88c165aa0bb494288c46cd96ac911d7cfdf))
- set retention.ms to -1 instead of Long.MAX_VALUE ([#5560](https://github.com/confluentinc/ksql/pull/5560)) ([22da8a0](https://github.com/confluentinc/ksql/commit/22da8a091f0020bbd541ad5f812054c7744f0785))
- update Concat UDF to new framework and make variadic ([#5513](https://github.com/confluentinc/ksql/pull/5513)) ([cab6a86](https://github.com/confluentinc/ksql/commit/cab6a8672f97d86da75a3e5b1e70985146129640))
- zero decimal bug ([#5531](https://github.com/confluentinc/ksql/pull/5531)) ([1b9094b](https://github.com/confluentinc/ksql/commit/1b9094bd5ead8a8441ef8f10a5f86722dd7d3988))
- /query-stream endpoint should serialize Struct (MINOR) ([#5205](https://github.com/confluentinc/ksql/pull/5205)) ([12b092b](https://github.com/confluentinc/ksql/commit/12b092bda160f614ef0b8096f77b7013efc3b190))
- Move Cors handler in front of /chc handlers ([#5239](https://github.com/confluentinc/ksql/pull/5239)) ([004ced2](https://github.com/confluentinc/ksql/commit/004ced255479f6a5975848270746a45746a4437a))
- use schema in annotation as schema provider if present ([1a90eeb](https://github.com/confluentinc/ksql/commit/1a90eeb71ae8292410fc99d49c24fdafadcf9de7))
- use sr's jackson-jsonschema version ([#5213](https://github.com/confluentinc/ksql/pull/5213)) ([0b3899a](https://github.com/confluentinc/ksql/commit/0b3899a4a31f668a897d82d98c59365aae8022ec))
- /inserts-stream endpoint now supports nested types ([#5621](https://github.com/confluentinc/ksql/pull/5621)) ([866ae34](https://github.com/confluentinc/ksql/commit/866ae3499a9d59068461d17ed15e1353759a0334))
- don't fail if broker does not support AuthorizedOperations ([#5617](https://github.com/confluentinc/ksql/pull/5617)) ([0feb081](https://github.com/confluentinc/ksql/commit/0feb081c89d00f326e151035ac5616d19f610307))
- ensure only deserializable cmds are written to command topic ([#5645](https://github.com/confluentinc/ksql/pull/5645)) ([4ad2bde](https://github.com/confluentinc/ksql/commit/4ad2bde44f31ea3c15fdd898ce12d50a595013d1))
- support GROUP BY with no source columns used ([#5644](https://github.com/confluentinc/ksql/pull/5644)) ([a8e6630](https://github.com/confluentinc/ksql/commit/a8e66304081879335c62948d5a8dd5f5531766be))

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
Kafka topic's record. But starting in version 0.10.0, the projection must include the key columns, and ksqlDB stores these columns

in the _key_ of the underlying Kafka record. Optionally, you may provide an alias for

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
_order dependent_, so dropping the `ID` value column would result in a

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

- add multi-join expression support ([#5081](https://github.com/confluentinc/ksql/pull/5081)) ([002cd5a](https://github.com/confluentinc/ksql/commit/002cd5ad6473cbb000291e32ebe5a459d2870b61))
- support more advanced suite of LIKE expressions ([#5013](https://github.com/confluentinc/ksql/pull/5013)) ([67cd9d9](https://github.com/confluentinc/ksql/commit/67cd9d91690039cc01f43438db1fbcd3fe4924ff))
- add COALESCE function ([#4829](https://github.com/confluentinc/ksql/pull/4829)) ([251c237](https://github.com/confluentinc/ksql/commit/251c237c455f4cf3843628f41e4bbf57795cfd12))
- add GROUP BY support for any key names ([#4899](https://github.com/confluentinc/ksql/pull/4899)) ([e7cbdfc](https://github.com/confluentinc/ksql/commit/e7cbdfcc8c9853e2ae6dfcaf670e04f07ccd5444)), closes [#4898](https://github.com/confluentinc/ksql/issues/4898)
- partition-by primitive key support (#4098) ([7addf88](https://github.com/confluentinc/ksql/commit/7addf8856a6d62a6890a5f2520eead26538233a6)), closes [#4098](https://github.com/confluentinc/ksql/issues/4098)
- add KsqlQueryStatus to decouple from KafkaStreams.State ([#5029](https://github.com/confluentinc/ksql/pull/5029)) ([e8cbcde](https://github.com/confluentinc/ksql/commit/e8cbcde548e1f3078b1396173a3f55f86ee20626))
- Adds rate limiting to pull queries ([#4951](https://github.com/confluentinc/ksql/pull/4951)) ([6284111](https://github.com/confluentinc/ksql/commit/6284111652252b63cfc233bdfc3e08dbff983bbd))
- Do not allow access to new streaming endpoints using HTTP1.x ([#5193](https://github.com/confluentinc/ksql/pull/5193)) ([8b90035](https://github.com/confluentinc/ksql/commit/8b90035c6facf47d81dfd1616784b191141dce31))
- fail startup if command contains incompatible version ([#5104](https://github.com/confluentinc/ksql/pull/5104)) ([a1751b1](https://github.com/confluentinc/ksql/commit/a1751b1689532ef9abd436d5f27fe9aa8ff555ac))
- klip-14 - rowtime as pseduocolumn ([#5150](https://github.com/confluentinc/ksql/pull/5150)) ([d541420](https://github.com/confluentinc/ksql/commit/d541420acf61f0e0e7b25d91120886007dbdae01))
- 'SHOW QUERIES [EXTENDED]' statement returns results for all nodes in the cluster ([#4875](https://github.com/confluentinc/ksql/pull/4875)) ([7385a31](https://github.com/confluentinc/ksql/commit/7385a31dd33293b76e594649213da2abb81b8ddf))
- transient queries added to show queries output ([#5105](https://github.com/confluentinc/ksql/pull/5105)) ([e8a2a63](https://github.com/confluentinc/ksql/commit/e8a2a63210219cbac7d979ec0915f1d2016398e2))

### Bug Fixes

- 'drop (stream|table) if exists' should not fail if source does not exist ([#4872](https://github.com/confluentinc/ksql/pull/4872)) ([b0669a0](https://github.com/confluentinc/ksql/commit/b0669a044b53407fba3ceb11bddee73e69c0006f))
- add deserializer for SqlType ([#4830](https://github.com/confluentinc/ksql/pull/4830)) ([eed9912](https://github.com/confluentinc/ksql/commit/eed99123a7f673eec9c1f5224c164a5737e6dc41))
- allow trailing slash in listeners config (MINOR) ([#5012](https://github.com/confluentinc/ksql/pull/5012)) ([13b0455](https://github.com/confluentinc/ksql/commit/13b04557d01aed36dcf11e8d6c8300dae4143615))
- Allows unclosed quote characters to exist in comments ([#4993](https://github.com/confluentinc/ksql/pull/4993)) ([fd65021](https://github.com/confluentinc/ksql/commit/fd6502115170c97f6a2c61cabf513145ea9c4cdc))
- avoid duplicate column name errors from auto-generated aliases ([#4827](https://github.com/confluentinc/ksql/pull/4827)) ([258d0b0](https://github.com/confluentinc/ksql/commit/258d0b06d81954af2c4e2c17fd7f26c74f315368))
- avoid long possible format lists on PRINT TOPIC for nulls ([#4867](https://github.com/confluentinc/ksql/pull/4867)) ([a66e489](https://github.com/confluentinc/ksql/commit/a66e489d176e1ed6ea715af3d9cfd75a351e0447))
- better error code on shutdown ([#4754](https://github.com/confluentinc/ksql/pull/4754)) ([17d758d](https://github.com/confluentinc/ksql/commit/17d758d0137102aa30493eee458e1a7c2ea54f82))
- Catch server startup exceptions ([#4974](https://github.com/confluentinc/ksql/pull/4974)) ([898f3a1](https://github.com/confluentinc/ksql/commit/898f3a104f88fba5cf79e02f12ef43760065eaae))
- CommandRunner metric has correct metric displayed when thread dies ([#4653](https://github.com/confluentinc/ksql/pull/4653)) ([1db542b](https://github.com/confluentinc/ksql/commit/1db542bfea5006fc9b3af59bca28d4af48fc5d57))
- do not allow GROUP BY and PARTITION BY on boolean expressions ([#4940](https://github.com/confluentinc/ksql/pull/4940)) ([d84d2d1](https://github.com/confluentinc/ksql/commit/d84d2d110d5ac502de4ba16602aa32babe24b4ca))
- do not allow grouping sets ([#4942](https://github.com/confluentinc/ksql/pull/4942)) ([51bb9f6](https://github.com/confluentinc/ksql/commit/51bb9f6f4bcef2d75f6d6d96c4c883559a21999b))
- do not allow implicit casting in UDAF function lookup ([#5145](https://github.com/confluentinc/ksql/pull/5145)) ([6709010](https://github.com/confluentinc/ksql/commit/6709010b06fed487334338e5e102bf53d51b4c46))
- do not filter out rows where PARTITION BY resolves to null ([#4823](https://github.com/confluentinc/ksql/pull/4823)) ([e75a792](https://github.com/confluentinc/ksql/commit/e75a79286927a6e43b7d580b32215fb18b00d707))
- Filter out hosts with no lag info by default ([#4859](https://github.com/confluentinc/ksql/pull/4859)) ([e10bbce](https://github.com/confluentinc/ksql/commit/e10bbcede42f209765e96b2b2bb392f4d83b2560))
- fix repartition semantics ([#4816](https://github.com/confluentinc/ksql/pull/4816)) ([609e9e2](https://github.com/confluentinc/ksql/commit/609e9e28ca5cbbc9302e66f0fc54a983afd256de))
- generated aliases for struct field access no longer require quoting ([#4977](https://github.com/confluentinc/ksql/pull/4977)) ([2002458](https://github.com/confluentinc/ksql/commit/2002458f6b668b7b17b05b192a7cc3fab0a23b20))
- handle leap days correctly during timestamp extraction ([#4878](https://github.com/confluentinc/ksql/pull/4878)) ([c54db81](https://github.com/confluentinc/ksql/commit/c54db81708c56a73c082df642f8ab94d8e276142)), closes [#4864](https://github.com/confluentinc/ksql/issues/4864)
- If startup hangs (esp on preconditions), shutdown server correctly ([#4889](https://github.com/confluentinc/ksql/pull/4889)) ([20c4b59](https://github.com/confluentinc/ksql/commit/20c4b5903c13fb8435aa94e486b637074b5bb769))
- Improve error message for where/having type errors ([#5023](https://github.com/confluentinc/ksql/pull/5023)) ([23eb80d](https://github.com/confluentinc/ksql/commit/23eb80d06ea0275b28edac9b0fb5453ea4a966f8))
- improve handling of NULLs ([#5019](https://github.com/confluentinc/ksql/pull/5019)) ([c53dd68](https://github.com/confluentinc/ksql/commit/c53dd68bd912d79ced54403650ba13597ffe35a0)), closes [#4912](https://github.com/confluentinc/ksql/issues/4912)
- include lower-case identifiers among those that need quotes ([#3723](https://github.com/confluentinc/ksql/pull/3723)) ([#5139](https://github.com/confluentinc/ksql/issues/5139)) ([3bcbcf4](https://github.com/confluentinc/ksql/commit/3bcbcf45e1fc03245a57005d318133c568fb12c6))
- make endpoints available while waiting for precondition ([#5069](https://github.com/confluentinc/ksql/pull/5069)) ([1136162](https://github.com/confluentinc/ksql/commit/113616260cc0db89124330ed5e935e4b9e5af281))
- Make sure internal client is configured for TLS ([#5059](https://github.com/confluentinc/ksql/pull/5059)) ([37c7713](https://github.com/confluentinc/ksql/commit/37c7713920c7654c9b5c2f81ed72225233e1176c))
- NPE in latest_by_offset if first two elements processed are both null ([#4975](https://github.com/confluentinc/ksql/pull/4975)) ([4c72f93](https://github.com/confluentinc/ksql/commit/4c72f93813af19f6fd074b3a691018434731d545))
- only create processing log stream if it doesn't exist ([#4805](https://github.com/confluentinc/ksql/pull/4805)) ([8dead0f](https://github.com/confluentinc/ksql/commit/8dead0f4ea82de1efa34614a715b267f0246f224))
- output valid multiline queries when running SHOW QUERIES ([#4956](https://github.com/confluentinc/ksql/pull/4956)) ([ec74a33](https://github.com/confluentinc/ksql/commit/ec74a337c93e834278938f9fb692aab143d06276))
- query id for TERMINATE should be case insensitive ([#5005](https://github.com/confluentinc/ksql/pull/5005)) ([588c1e9](https://github.com/confluentinc/ksql/commit/588c1e905fc92c40635ca3271089dfecdfd4f3f6))
- reject requests to new API server if server is not ready ([#5048](https://github.com/confluentinc/ksql/pull/5048)) ([d988722](https://github.com/confluentinc/ksql/commit/d9887220bb5a688b6e8682227189341e88a61967))
- Remove unnecessary error logging for heartbeat ([#4809](https://github.com/confluentinc/ksql/pull/4809)) ([fa84576](https://github.com/confluentinc/ksql/commit/fa845763d8c52c54a65a7618a044300e8f0043f6))
- replace 'null' in explain plan with correct op type ([#5075](https://github.com/confluentinc/ksql/pull/5075)) ([f9bc0e6](https://github.com/confluentinc/ksql/commit/f9bc0e68e2b50201085352b27c56cc120eb72225))
- Returns empty lag info for a dead host rather than last received lags ([#4837](https://github.com/confluentinc/ksql/pull/4837)) ([3d98527](https://github.com/confluentinc/ksql/commit/3d985274bb4658b168758eb6c20cba38aeb1ab51))
- Stop PARTITION BY and UDTFs that fail from terminating the query ([#4822](https://github.com/confluentinc/ksql/pull/4822)) ([522fe84](https://github.com/confluentinc/ksql/commit/522fe84f407b56dbc82309db4a6502d0c9b8fc38))
- support quotes in explain statements (MINOR) ([#5142](https://github.com/confluentinc/ksql/pull/5142)) ([bee2fe0](https://github.com/confluentinc/ksql/commit/bee2fe0f523ee7e763e223bd2d1f4fc6f178ffec))
- throw error when column does not exist during INSERT INTO ([#4926](https://github.com/confluentinc/ksql/pull/4926)) ([89e261d](https://github.com/confluentinc/ksql/commit/89e261da5aff6b62901bc7d73c12baecee7bf95c))
- remove immutable properties from headless mode ([#4936](https://github.com/confluentinc/ksql/pull/4936)) ([5550880](https://github.com/confluentinc/ksql/commit/5550880de097255adbe536907d0656ba8deea0cc))
- rename ksqldb in normal logging path (MINOR) ([#4915](https://github.com/confluentinc/ksql/pull/4915)) ([16172ba](https://github.com/confluentinc/ksql/commit/16172ba8c820bb50c838ea820873b3593e062c6c))
- support trailing slashes in listener URLs ([#5076](https://github.com/confluentinc/ksql/pull/5076)) ([e9e0431](https://github.com/confluentinc/ksql/commit/e9e0431f6eafd7ef18dd2cb9e17730ec3ba1e226))
- logic when closing ksqlEngine fixed ([#4917](https://github.com/confluentinc/ksql/pull/4917)) ([a217eb9](https://github.com/confluentinc/ksql/commit/a217eb9a8a80b38c970021faad76a4b9fcade609))
- use describeTopics() for Kafka healtcheck probe ([#4814](https://github.com/confluentinc/ksql/pull/4814)) ([578d0d5](https://github.com/confluentinc/ksql/commit/578d0d543bce685ca6e253a8c6f9bcfbf191093d))
- Do not allow access to new streaming endpoints using HTTP1.x ([#5193](https://github.com/confluentinc/ksql/pull/5193)) ([8b90035](https://github.com/confluentinc/ksql/commit/8b90035c6facf47d81dfd1616784b191141dce31))
- speed up restarts by not building topologies for terminated queries ([#5002](https://github.com/confluentinc/ksql/pull/5002)) ([2472382](https://github.com/confluentinc/ksql/commit/24723824635e7ca175bc5be1fbf6a415010b00bb))

### BREAKING CHANGES

- Select star, i.e. `select *`, no longer expands to include `ROWTIME` column(s). Instead, `ROWTIME` is only included in the results of queries if explicitly included in the projection, e.g. `select rowtime, *`.
  This only affects new statements. Any view previously created via a `CREATE STREAM AS SELECT` or `CREATE TABLE AS SELECT` statement is unaffected.
- This release changes the system generated column name for any columns in projections that are struct field dereferences. Previously, the full path was used when generating the name, now only the final field name is used. For example, `SELECT someStruct->someField, ...` previously generated a column name of `SOMESTRUCT__SOMEFIELD` and now generates a name of `SOMEFIELD`. Generated column names may have a numeral appended to the end to ensure uniqueness, for example `SOMEFIELD_2`.

  Note: it is recommended that you do not rely on system generated column names for production systems, because naming logic may change between releases. Providing an explicit alias ensures consistent naming across releases, for example, `SELECT someStruct->someField AS someField`.
  Backward compatibility: existing running queries will not be affected by this change, and they will continue to run with the same column names. Any statements executed after the upgrade will use the new names where no explicit alias is provided. Add explicit aliases to your statements if you require the old names, for example: `SELECT someStruct->someField AS SOMESTRUCT__SOMEFIELD, ...`

- Existing queries that reference a single GROUP BY column in the projection would fail if they were resubmitted, due to a duplicate column. The same existing queries will continue to run if already running, i.e. this is only a change for newly submitted queries. Existing queries will use the old query semantics.
- Push queries, which rely on auto-generated column names, may see changes in column names. Pull queries and any existing persistent queries are unaffected, e.g. those created with `CREATE STREAM AS SELECT`, `CREATE TABLE AS SELECT` or `INSERT INTO`.
- The ksqlDB server no longer ships with Jetty. This means that when you start the server, you must supply Jetty-specific dependencies, like certain login modules used for basic authentication, by using the KSQL_CLASSPATH environment variable for them to be found.

  See [how to configure ksqlDB for basic HTTP authentication](https://github.com/confluentinc/ksql/blob/0.9.0-ksqldb/docs/operate-and-deploy/installation/server-config/security.md#configure-ksqldb-for-basic-http-authentication)

## Upgrading

If you're upgrading from a ksqlDB version before 0.7, follow these [upgrade instructions](https://docs.ksqldb.io/en/latest/operate-and-deploy/installation/upgrading/#upgrade-notes).

## [0.8.1](https://github.com/confluentinc/ksql/releases/tag/v0.8.1-ksqldb) (2020-03-30)

### Bug Fixes

- Don't wait for streams thread to be in running state ([#4908](https://github.com/confluentinc/ksql/pull/4908)) ([2f83119](https://github.com/confluentinc/ksql/commit/2f83119))
- Infer TLS based on scheme of server string ([#4893](https://github.com/confluentinc/ksql/pull/4893)) ([a519ed3](https://github.com/confluentinc/ksql/commit/a519ed3))

## [0.8.0](https://github.com/confluentinc/ksql/releases/tag/v0.8.0-ksqldb) (2020-03-18)

### Features

- support Protobuf in ksqlDB ([#4469](https://github.com/confluentinc/ksql/pull/4469)) ([a77cebe](https://github.com/confluentinc/ksql/commit/a77cebe))
- introduce JSON_SR format ([#4596](https://github.com/confluentinc/ksql/pull/4596)) ([daa04d2](https://github.com/confluentinc/ksql/commit/daa04d2))
- support for tunable retention, grace period for windowed tables ([#4733](https://github.com/confluentinc/ksql/pull/4733)) ([30d49b3](https://github.com/confluentinc/ksql/commit/30d49b3)), closes [#4157](https://github.com/confluentinc/ksql/issues/4157)
- add REGEXP_EXTRACT UDF ([#4728](https://github.com/confluentinc/ksql/pull/4728)) ([a25f0fb](https://github.com/confluentinc/ksql/commit/a25f0fb))
- add ARRAY_LENGTH UDF ([#4725](https://github.com/confluentinc/ksql/pull/4725)) ([31a9d9d](https://github.com/confluentinc/ksql/commit/31a9d9d))
- Implement latest_by_offset() UDAF ([#4782](https://github.com/confluentinc/ksql/pull/4782)) ([0c13bb0](https://github.com/confluentinc/ksql/commit/0c13bb0))
- add confluent-hub to ksqlDB docker image ([#4729](https://github.com/confluentinc/ksql/pull/4729)) ([b74867a](https://github.com/confluentinc/ksql/commit/b74867a))
- add the topic name to deserialization errors ([#4573](https://github.com/confluentinc/ksql/pull/4573)) ([0f7edf6](https://github.com/confluentinc/ksql/commit/0f7edf6))
- Add metrics for pull queries endpoint ([#4608](https://github.com/confluentinc/ksql/pull/4608)) ([23e3868](https://github.com/confluentinc/ksql/commit/23e3868))
- log groupby errors to processing logger ([#4575](https://github.com/confluentinc/ksql/pull/4575)) ([b503d25](https://github.com/confluentinc/ksql/commit/b503d25))
- Provide upper limit on number of push queries ([#4581](https://github.com/confluentinc/ksql/pull/4581)) ([2cd66c7](https://github.com/confluentinc/ksql/commit/2cd66c7))
- display errors in CLI in red text ([#4509](https://github.com/confluentinc/ksql/pull/4509)) ([56f9c9b](https://github.com/confluentinc/ksql/commit/56f9c9b))
- enhance `PRINT TOPIC`'s format detection ([#4551](https://github.com/confluentinc/ksql/pull/4551)) ([8b19bc6](https://github.com/confluentinc/ksql/commit/8b19bc6))

### Bug Fixes

- change default exception handling for timestamp extractors ([#4632](https://github.com/confluentinc/ksql/pull/4632)) ([1576af0](https://github.com/confluentinc/ksql/commit/1576af0))
- create schemas at topic creation ([#4717](https://github.com/confluentinc/ksql/pull/4717)) ([514025d](https://github.com/confluentinc/ksql/commit/514025d))
- don't display decimals in scientific notation in CLI ([#4723](https://github.com/confluentinc/ksql/pull/4723)) ([3626f42](https://github.com/confluentinc/ksql/commit/3626f42))
- stop logging about command topic creation on startup if exists (MINOR) ([#4709](https://github.com/confluentinc/ksql/pull/4709)) ([f4cec0a](https://github.com/confluentinc/ksql/commit/f4cec0a))
- added special handling for forwarded pull query request ([#4597](https://github.com/confluentinc/ksql/pull/4597)) ([ba4fe74](https://github.com/confluentinc/ksql/commit/ba4fe74))
- backport fixes from query close ([#4662](https://github.com/confluentinc/ksql/pull/4662)) ([8168002](https://github.com/confluentinc/ksql/commit/8168002))
- change configOverrides back to streamsProperties ([#4675](https://github.com/confluentinc/ksql/pull/4675)) ([ce74cf8](https://github.com/confluentinc/ksql/commit/ce74cf8))
- csas/ctas with timestamp column is used for output rowtime ([#4489](https://github.com/confluentinc/ksql/pull/4489)) ([ddddf92](https://github.com/confluentinc/ksql/commit/ddddf92))
- patch KafkaStreamsInternalTopicsAccessor as KS internals changed ([#4621](https://github.com/confluentinc/ksql/pull/4621)) ([eb07370](https://github.com/confluentinc/ksql/commit/eb07370))
- use HTTPS instead of HTTP to resolve dependencies in Maven archetype ([#4511](https://github.com/confluentinc/ksql/pull/4511)) ([f21823f](https://github.com/confluentinc/ksql/commit/f21823f))
- add deserializer for SqlType ([#4830](https://github.com/confluentinc/ksql/pull/4830)) ([eed9912](https://github.com/confluentinc/ksql/commit/eed9912))

## [0.7.1](https://github.com/confluentinc/ksql/releases/tag/v0.7.1-ksqldb) (2020-02-28)

### Features

- support custom column widths in cli ([#4616](https://github.com/confluentinc/ksql/pull/4616)) ([cb66e05](https://github.com/confluentinc/ksql/commit/cb66e05))

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

- support partial schemas ([#4625](https://github.com/confluentinc/ksql/pull/4625)) ([7cc19a0](https://github.com/confluentinc/ksql/commit/7cc19a0))

### Bug Fixes

- add functional-test dependencies to Docker module ([#4586](https://github.com/confluentinc/ksql/pull/4586)) ([04fcf8d](https://github.com/confluentinc/ksql/commit/04fcf8d))
- don't cleanup topics on engine close ([#4658](https://github.com/confluentinc/ksql/pull/4658)) ([ad66a81](https://github.com/confluentinc/ksql/commit/ad66a81))
- idempotent terminate that can handle hung streams ([#4643](https://github.com/confluentinc/ksql/pull/4643)) ([d96db14](https://github.com/confluentinc/ksql/commit/d96db14))

## [0.7.0](https://github.com/confluentinc/ksql/releases/tag/v0.7.0-ksqldb) (2020-02-11)

### Upgrading

Note that ksqlDB 0.7.0 has a number of breaking changes when compared with ksqlDB 0.6.0 (see the 'Breaking changes' section below for details). Please make sure to read and follow these [upgrade instructions](./docs-md/operate-and-deploy/installation/upgrading.md) if you are upgrading from a previous ksqlDB version.

### Features

- feat: primitive key support ([#4478](https://github.com/confluentinc/ksql/pull/4478)) ([ddf09d](https://github.com/confluentinc/ksql/commit/ddf09d))

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

- add a new default SchemaRegistryClient and remove default for SR url ([#4325](https://github.com/confluentinc/ksql/pull/4325)) ([e045f7c](https://github.com/confluentinc/ksql/commit/e045f7c))
- Adds lag reporting and API for use in lag aware routing as described in KLIP 12 ([#4392](https://github.com/confluentinc/ksql/pull/4392)) ([cb9ae29](https://github.com/confluentinc/ksql/commit/cb9ae29))
- better error message when transaction to command topic fails to initialize by timeout ([#4486](https://github.com/confluentinc/ksql/pull/4486)) ([a5fed3b](https://github.com/confluentinc/ksql/commit/a5fed3b))
- expression support in JOINs ([#4278](https://github.com/confluentinc/ksql/pull/4278)) ([2d0bfe8](https://github.com/confluentinc/ksql/commit/2d0bfe8))
- hide internal/system topics from SHOW TOPICS ([#4322](https://github.com/confluentinc/ksql/pull/4322)) ([075fed3](https://github.com/confluentinc/ksql/commit/075fed3))
- Implement pull query routing to standbys if active is down ([#4398](https://github.com/confluentinc/ksql/pull/4398)) ([ace23b1](https://github.com/confluentinc/ksql/commit/ace23b1))
- Implementation of heartbeat mechanism as part of KLIP-12 ([#4173](https://github.com/confluentinc/ksql/pull/4173)) ([37c1eaa](https://github.com/confluentinc/ksql/commit/37c1eaa))
- native map/array constructors ([#4232](https://github.com/confluentinc/ksql/pull/4232)) ([3ecfaad](https://github.com/confluentinc/ksql/commit/3ecfaad))
- support implicit casting in UDFs ([#4406](https://github.com/confluentinc/ksql/pull/4406)) ([6fc4f72](https://github.com/confluentinc/ksql/commit/6fc4f72))
- add COUNT_DISTINCT and allow generics in UDAFs ([#4150](https://github.com/confluentinc/ksql/pull/4150)) ([2d5e680](https://github.com/confluentinc/ksql/commit/2d5e680))
- Add Cube UDTF ([#3935](https://github.com/confluentinc/ksql/pull/3935)) ([6be8e7c](https://github.com/confluentinc/ksql/commit/6be8e7c))
- remove WindowStart() and WindowEnd() UDAFs ([#4459](https://github.com/confluentinc/ksql/pull/4459)) ([eda2e34](https://github.com/confluentinc/ksql/commit/eda2e34))
- ask for password if -p is not provided ([#4153](https://github.com/confluentinc/ksql/pull/4153)) ([7a83bbf](https://github.com/confluentinc/ksql/commit/7a83bbf))
- make (certain types of) server error messages configurable ([#4121](https://github.com/confluentinc/ksql/pull/4121)) ([cedf47e](https://github.com/confluentinc/ksql/commit/cedf47e))
- add source statement to SourceDescription ([#4134](https://github.com/confluentinc/ksql/pull/4134)) ([1146aa5](https://github.com/confluentinc/ksql/commit/1146aa5))
- add support for inline struct creation ([#4120](https://github.com/confluentinc/ksql/pull/4120)) ([6e558da](https://github.com/confluentinc/ksql/commit/6e558da))
- allow environment variables to configure embedded connect ([#4260](https://github.com/confluentinc/ksql/pull/4260)) ([e032ea9](https://github.com/confluentinc/ksql/commit/e032ea9))
- enable Kafla ACL authorization checks for Pull Queries ([#4187](https://github.com/confluentinc/ksql/pull/4187)) ([5ee1e9e](https://github.com/confluentinc/ksql/commit/5ee1e9e))
- implemention of KLIP-13 ([#4099](https://github.com/confluentinc/ksql/pull/4099)) ([b23dae9](https://github.com/confluentinc/ksql/commit/b23dae9))
- show properties now includes embedded connect properties and scope ([#4099](https://github.com/confluentinc/ksql/pull/4099)) ([ebac104](https://github.com/confluentinc/ksql/commit/ebac104))
- add connector status to LIST CONNECTORS ([#4077](https://github.com/confluentinc/ksql/pull/4077)) ([5ff94b6](https://github.com/confluentinc/ksql/commit/5ff94b6))
- add JMX metric for commandRunner status ([#4019](https://github.com/confluentinc/ksql/pull/4019)) ([55d75f2](https://github.com/confluentinc/ksql/commit/55d75f2))
- add support to terminate all running queries ([#3944](https://github.com/confluentinc/ksql/pull/3944)) ([abbce84](https://github.com/confluentinc/ksql/commit/abbce84))
- expose execution plans from the ksql engine API ([#3482](https://github.com/confluentinc/ksql/pull/3482)) ([067139c](https://github.com/confluentinc/ksql/commit/067139c))
- expression support for PARTITION BY ([#4032](https://github.com/confluentinc/ksql/pull/4032)) ([0f31f8e](https://github.com/confluentinc/ksql/commit/0f31f8e))
- remove unnecessary changelog for topics ([#3987](https://github.com/confluentinc/ksql/pull/3987)) ([6e0d00e](https://github.com/confluentinc/ksql/commit/6e0d00e))

### Performance Improvements

- Avoids logging INFO for rest-util requests, since it hurts pull query performance ([#4302](https://github.com/confluentinc/ksql/pull/4302)) ([50b4c1c](https://github.com/confluentinc/ksql/commit/50b4c1c))
- Improves pull query performance by making the default schema service a singleton ([#4216](https://github.com/confluentinc/ksql/pull/4216)) ([f991752](https://github.com/confluentinc/ksql/commit/f991752))

### Bug Fixes

- add ksql-test-runner deps to ksql package lib ([#4272](https://github.com/confluentinc/ksql/pull/4272)) ([6e28cc4](https://github.com/confluentinc/ksql/commit/6e28cc4))
- ConcurrentModificationException in ClusterStatusResource ([#4510](https://github.com/confluentinc/ksql/pull/4510)) ([c79cba9](https://github.com/confluentinc/ksql/commit/c79cba9))
- deadlock when closing transient push query ([#4297](https://github.com/confluentinc/ksql/pull/4297)) ([ac8fb63](https://github.com/confluentinc/ksql/commit/ac8fb63))
- delimiters reset across non-delimited types (reverts [#4366](https://github.com/confluentinc/ksql/issues/4366)) ([#4371](https://github.com/confluentinc/ksql/pull/4371)) ([5788729](https://github.com/confluentinc/ksql/commit/5788729))
- do not throw error if VALUE_DELIMITER is set on non-DELIMITED topic ([#4366](https://github.com/confluentinc/ksql/pull/4366)) ([2b59b8b](https://github.com/confluentinc/ksql/commit/2b59b8b))
- exception on shutdown of ksqlDB server ([#4483](https://github.com/confluentinc/ksql/pull/4483)) ([126e2cf](https://github.com/confluentinc/ksql/commit/126e2cf))
- fix compilation error due to `Format` refactoring ([#4465](https://github.com/confluentinc/ksql/pull/4465)) ([07a4dcd](https://github.com/confluentinc/ksql/commit/07a4dcd))
- fix NPE in CLI if not username supplied ([#4312](https://github.com/confluentinc/ksql/pull/4312)) ([0b6da0b](https://github.com/confluentinc/ksql/commit/0b6da0b))
- Fixes the single host lag reporting case ([#4494](https://github.com/confluentinc/ksql/pull/4494)) ([6b8bc2a](https://github.com/confluentinc/ksql/commit/6b8bc2a))
- floating point comparison was inexact ([#4372](https://github.com/confluentinc/ksql/pull/4372)) ([2a4ca47](https://github.com/confluentinc/ksql/commit/2a4ca47))
- Include functional tests jar in docker images ([#4274](https://github.com/confluentinc/ksql/pull/4274)) ([2559b2f](https://github.com/confluentinc/ksql/commit/2559b2f))
- include valid alternative UDF signatures in error message (MINOR) ([#4403](https://github.com/confluentinc/ksql/pull/4403)) ([f397ad8](https://github.com/confluentinc/ksql/commit/f397ad8))
- Make null key serialization/deserialization symmetrical ([#4351](https://github.com/confluentinc/ksql/pull/4351)) ([2a61acb](https://github.com/confluentinc/ksql/commit/2a61acb))
- partial push & persistent query support for window bounds columns ([#4401](https://github.com/confluentinc/ksql/pull/4401)) ([48aa6ec](https://github.com/confluentinc/ksql/commit/48aa6ec))
- print root cause in error message ([#4505](https://github.com/confluentinc/ksql/pull/4505)) ([6299410](https://github.com/confluentinc/ksql/commit/6299410))
- pull queries should work across nodes ([#4169](https://github.com/confluentinc/ksql/pull/4169)) ([#4271](https://github.com/confluentinc/ksql/issues/4271)) ([2369213](https://github.com/confluentinc/ksql/commit/2369213))
- remove deprecated Acl API ([#4373](https://github.com/confluentinc/ksql/pull/4373)) ([a2b69f7](https://github.com/confluentinc/ksql/commit/a2b69f7))
- remove duplicate comment about Schema Regitry URL from sample server properties ([#4346](https://github.com/confluentinc/ksql/pull/4346)) ([0d542c5](https://github.com/confluentinc/ksql/commit/0d542c5))
- rename stale to standby in KsqlConfig ([#4467](https://github.com/confluentinc/ksql/pull/4467)) ([f8bb986](https://github.com/confluentinc/ksql/commit/f8bb986))
- report window type and query status better from API ([#4313](https://github.com/confluentinc/ksql/pull/4313)) ([ca9368a](https://github.com/confluentinc/ksql/commit/ca9368a))
- reserve `WINDOWSTART` and `WINDOWEND` as system column names ([#4388](https://github.com/confluentinc/ksql/pull/4388)) ([ea0a0ac](https://github.com/confluentinc/ksql/commit/ea0a0ac))
- Sets timezone of RestQueryTranslationTest test to make it work in non UTC zones ([#4407](https://github.com/confluentinc/ksql/pull/4407)) ([50b25d5](https://github.com/confluentinc/ksql/commit/50b25d5))
- show queries now returns the correct Kafka Topic if the query string contains with clause ([#4430](https://github.com/confluentinc/ksql/pull/4430)) ([1b713cd](https://github.com/confluentinc/ksql/commit/1b713cd))
- support conversion of STRING to BIGINT for window bounds ([#4500](https://github.com/confluentinc/ksql/pull/4500)) ([9c3cbf8](https://github.com/confluentinc/ksql/commit/9c3cbf8))
- support WindowStart() and WindowEnd() in pull queries ([#4435](https://github.com/confluentinc/ksql/pull/4435)) ([8da2b63](https://github.com/confluentinc/ksql/commit/8da2b63))
- add logging during restore ([#4270](https://github.com/confluentinc/ksql/pull/4270)) ([4e32da6](https://github.com/confluentinc/ksql/commit/4e32da6))
- log4j properties files ([#4293](https://github.com/confluentinc/ksql/pull/4293)) ([5911faf](https://github.com/confluentinc/ksql/commit/5911faf))
- report clearer error message when AVG used with DELIMITED ([#4295](https://github.com/confluentinc/ksql/pull/4295)) ([307bf4d](https://github.com/confluentinc/ksql/commit/307bf4d))
- better error message on self-join ([#4248](https://github.com/confluentinc/ksql/pull/4248)) ([1281ab2](https://github.com/confluentinc/ksql/commit/1281ab2))
- change query id generation to work with planned commands ([#4149](https://github.com/confluentinc/ksql/pull/4149)) ([91c421a](https://github.com/confluentinc/ksql/commit/91c421a))
- CLI commands may be terminated with semicolon+whitespace (MINOR) ([#4234](https://github.com/confluentinc/ksql/pull/4234)) ([096b78f](https://github.com/confluentinc/ksql/commit/096b78f))
- decimals in structs should display as numeric ([#4165](https://github.com/confluentinc/ksql/pull/4165)) ([75b539e](https://github.com/confluentinc/ksql/commit/75b539e))
- don't load current qtt test case from legacy loader ([#4245](https://github.com/confluentinc/ksql/pull/4245)) ([9479fd6](https://github.com/confluentinc/ksql/commit/9479fd6))
- immutability in some more classes (MINOR) ([#4179](https://github.com/confluentinc/ksql/pull/4179)) ([cbd3bab](https://github.com/confluentinc/ksql/commit/cbd3bab))
- include path of field that causes JSON deserialization error ([#4249](https://github.com/confluentinc/ksql/pull/4249)) ([5cc718b](https://github.com/confluentinc/ksql/commit/5cc718b))
- reintroduce FetchFieldFromStruct as a public UDF ([#4185](https://github.com/confluentinc/ksql/pull/4185)) ([a50a665](https://github.com/confluentinc/ksql/commit/a50a665))
- show topics doesn't display topics with different casing ([#4159](https://github.com/confluentinc/ksql/pull/4159)) ([0ac8747](https://github.com/confluentinc/ksql/commit/0ac8747))
- untracked file after cloning on Windows ([#4122](https://github.com/confluentinc/ksql/pull/4122)) ([04de30e](https://github.com/confluentinc/ksql/commit/04de30e))
- array access is now 1-indexed instead of 0-indexed ([#4057](https://github.com/confluentinc/ksql/pull/4057)) ([f09f797](https://github.com/confluentinc/ksql/commit/f09f797))
- Explicitly disallow table functions with table sources, fixes [#4033](https://github.com/confluentinc/ksql/issues/4033) ([#4085](https://github.com/confluentinc/ksql/pull/4085)) ([60e20ef](https://github.com/confluentinc/ksql/commit/60e20ef))
- fix issues with multi-statement requests failing to validate ([#3952](https://github.com/confluentinc/ksql/pull/3952)) ([3e7169b](https://github.com/confluentinc/ksql/commit/3e7169b)), closes [#3363](https://github.com/confluentinc/ksql/issues/3363)
- NPE when starting StandaloneExecutor ([#4119](https://github.com/confluentinc/ksql/pull/4119)) ([c6c00b1](https://github.com/confluentinc/ksql/commit/c6c00b1))
- properly set key when partition by ROWKEY and join on non-ROWKEY ([#4090](https://github.com/confluentinc/ksql/pull/4090)) ([6c80941](https://github.com/confluentinc/ksql/commit/6c80941))
- remove mapValues that excluded ROWTIME and ROWKEY columns ([#4066](https://github.com/confluentinc/ksql/pull/4066)) ([a6982bd](https://github.com/confluentinc/ksql/commit/a6982bd)), closes [#4052](https://github.com/confluentinc/ksql/issues/4052)
- robin's requested message changes ([#4021](https://github.com/confluentinc/ksql/pull/4021)) ([422a2e3](https://github.com/confluentinc/ksql/commit/422a2e3))
- schema column order returned by websocket pull query ([#4012](https://github.com/confluentinc/ksql/pull/4012)) ([85fef09](https://github.com/confluentinc/ksql/commit/85fef09))
- some terminals dont work with JLine 3.11 ([#3931](https://github.com/confluentinc/ksql/pull/3931)) ([ad183ec](https://github.com/confluentinc/ksql/commit/ad183ec))
- the Abs, Ceil and Floor methods now return proper types ([#3948](https://github.com/confluentinc/ksql/pull/3948)) ([3d6e119](https://github.com/confluentinc/ksql/commit/3d6e119))
- UncaughtExceptionHandler not being set for Persistent Queries ([#4087](https://github.com/confluentinc/ksql/pull/4087)) ([e193a2a](https://github.com/confluentinc/ksql/commit/e193a2a))
- unify behavior for PARTITION BY and GROUP BY ([#3982](https://github.com/confluentinc/ksql/pull/3982)) ([67d3f8c](https://github.com/confluentinc/ksql/commit/67d3f8c))
- wrong source type in pull query error message ([#3885](https://github.com/confluentinc/ksql/pull/3885)) ([65523c7](https://github.com/confluentinc/ksql/commit/65523c7)), closes [#3523](https://github.com/confluentinc/ksql/issues/3523)

### BREAKING CHANGES

- existing queries that perform a PARTITION BY or GROUP BY on a single column of one of the above supported primitive key types will now set the key to the appropriate type, not a `STRING` as previously.
- The `WindowStart()` and `WindowEnd()` UDAFs have been removed from KSQL. Use the `WindowStart` and `WindowEnd` system columns to access the window bounds within the SELECT expression instead.
- the order of columns for internal topics has changed. The `DELIMITED` format can not handle this in a backwards compatible way. Hence this is a breaking change for any existing queries the use the `DELIMITED` format and have internal topics.
  This change has been made now for two reasons:
  1.  its a breaking change, making it much harder to do later.
  2.  The recent https://github.com/confluentinc/ksql/pull/4404 change introduced this same issue for pull queries. This current change corrects pull queries too.
- Any query of a windowed source that uses `ROWKEY` in the SELECT projection will see the contents of `ROWKEY` change from a formatted `STRING` containing the underlying key and the window bounds, to just the underlying key. Queries can access the window bounds using `WINDOWSTART` and `WINDOWEND`.
- Joins on windowed sources now include `WINDOWSTART` and `WINDOWEND` columns from both sides on a `SELECT *`.
- `WINDOWSTART` and `WINDOWEND` are now reserved system column names. Any query that previously used those names will need to be changed: for example, alias the columns to a different name.
  These column names are being reserved for use as system columns when dealing with streams and tables that have a windowed key.
- standalone literals that used to be doubles may now be
  interpreted as BigDecimal. In most scenarios, this won't affect any
  queries as the DECIMAL can auto-cast to DOUBLE; in the case were the
  literal stands alone, the output schema will be a DECIMAL instead of a
  DOUBLE. To specify a DOUBLE literal, use scientific notation (e.g.
  1.234E-5).
- The response from the RESTful API has changed for some commands with this commit: the `SourceDescription` type no longer has a `format` field. Instead it has `keyFormat` and `valueFormat` fields.
  Response now includes a `state` property for each query that indicates the state of the query.
  e.g.
  ```json
  {
    "queryString": "create table OUTPUT as select * from INPUT;",
    "sinks": ["OUTPUT"],
    "id": "CSAS_OUTPUT_0",
    "state": "Running"
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
- Any `KEY` column identified in the `WITH` clause must be of the same Sql type as `ROWKEY`.
  Users can provide the name of a value column that matches the key column, e.g.
  ```sql
  CREATE STREAM S (ID INT, NAME STRING) WITH (KEY='ID', ...);
  ```
  Before primitive keys was introduced all keys were treated as `STRING`. With primitive keys `ROWKEY` can be types other than `STRING`, e.g. `BIGINT`.
  It therefore follows that any `KEY` column identified in the `WITH` clause must have the same SQL type as the _actual_ key, i.e. `ROWKEY`.
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
- Some existing joins may now fail and the type of `ROWKEY` in the result schema of joins may have changed.
  When `ROWKEY` was always a `STRING` it was possible to join an `INTEGER` column with a `BIGINT` column. This is no longer the case. A `JOIN` requires the join columns to be of the same type. (See https://github.com/confluentinc/ksql/issues/4130 which tracks adding support for being able to `CAST` join criteria).
  Where joining on two `INT` columns would previously have resulted in a schema containing `ROWKEY STRING KEY`, it would not result in `ROWKEY INT KEY`.
- A `GROUP BY` on single expressions now changes the SQL type of `ROWKEY` in the output schema of the query to match the SQL type of the expression.
  For example, consider:
  ```sql
  CREATE STREAM INPUT (ROWKEY STRING KEY, ID INT) WITH (...);
  CREATE TABLE OUTPUT AS SELECT COUNT(*) AS COUNT FROM INPUT GROUP BY ID;
  ```
  Previously, the above would have resulted in an output schema of `ROWKEY STRING KEY, COUNT BIGINT`, where `ROWKEY` would have stored the string representation of the integer from the `ID` column.
  With this commit the output schema will be `ROWKEY INT KEY COUNT BIGINT`.
- Any`GROUP BY` expression that resolves to `NULL`, including because a UDF throws an exception, now results in the row being excluded from the result. Previously, as the key was a `STRING` a value of `"null"` could be used. With other primitive types this is not possible. As key columns must be non-null any exception is logged and the row is excluded.
- commands that were persisted with RUN SCRIPT will no
  longer be executable
- the ARRAYCONTAINS function now needs to be referenced
  as either JSON_ARRAY_CONTAINS or ARRAY_CONTAINS depending on the
  intended param types
- A `PARTITION BY` now changes the SQL type of `ROWKEY` in the output schema of a query.
  For example, consider:
  ```sql
  CREATE STREAM INPUT (ROWKEY STRING KEY, ID INT) WITH (...);
  CREATE STREAM OUTPUT AS SELECT ROWKEY AS NAME FROM INPUT PARTITION BY ID;
  ```
  Previously, the above would have resulted in an output schema of `ROWKEY STRING KEY, NAME STRING`, where `ROWKEY` would have stored the string representation of the integer from the `ID` column. With this commit the output schema will be `ROWKEY INT KEY, NAME STRING`.
- any queries that were using array index mechanism
  should change to use 1-base indexing instead of 0-base.
- The maxInterval parameter for ksql-datagen is now deprecated. Use msgRate instead.
- this change makes it so that PARTITION BY statements
  use the _source_ schema, not the value/projection schema, when selecting
  the value to partition by. This is consistent with GROUP BY, and
  standard SQL for GROUP by. Any statement that previously used PARTITION
  BY may need to be reworked. 1/2
- when querying with EMIT CHANGES and PARTITION BY, the
  PARTITION BY clause should now come before EMIT CHANGES. 2/2
- KSQL will now, by default, not create duplicate changelog for table sources.
  fixes: https://github.com/confluentinc/ksql/issues/3621
  Now that Kafka Steams has a `KTable.transformValues` we no longer need to create a table by first creating a stream, then doing a select/groupby/aggregate on it. Instead, we can just use `StreamBuilder.table`.
  This change makes the switch, removing the `StreamToTable` types and calls and replaces them with either `TableSource` or `WindowedTableSource`, copying the existing pattern for `StreamSource` and `WindowedStreamSource`.
  It also reinstates a change in `KsqlConfig` that ensures topology optimisations are on by default. This was the case for 5.4.x, but was inadvertently turned off.
  With the optimisation config turned on, and the new builder step used, KSQL no longer creates a changelog topic to back the tables state store. This is not needed as the source topic is itself the changelog. The change includes new tests in `table.json` to confirm the change log topic is not created by default and is created if the user turns off optimisations.
  This change also removes the line in the `TestExecutor` that explicitly sets topology optimisations to `all`. The test should _not_ of being doing tis. This may been why the bug turning off optimisations was not detected.
- this change removes the old method of generating query
  IDs based on their sequence of _successful_ execution. Instead all
  queries will use their offset in the command topic. Similarly, all DROP
  queries issued before 5.0 will no longer cascade query terminiation.
- `ALL` is now a reserved word and can not be used for identifiers without being quoted.
- abs, ceil and floor will now return types aligned with
  other databases systems (i.e. the same type as the input). Previously
  these udfs would always return Double.
- Statements in the command topic will be retried until they succeed. For example, if the source topic has been deleted for a create stream/table statement, the server may fail to start since command runner will be stuck processing the statement. This ensures that the same set of streams/tables are created when restarting the server. You can check to see if the command runner is stuck by:
  1. Looking in the server logs to see if a statement is being retried.
  2. The JMX metric `_confluent-ksql-<service-id>ksql-rest-app-command-runner` will be in an `ERROR` state

## [v0.6.0](https://github.com/confluentinc/ksql/releases/tag/v0.6.0-ksqldb) (2019-11-19)

### Features

- add config to disable pull queries when validation is required ([#3879](https://github.com/confluentinc/ksql/pull/3879)) ([ccc636d](https://github.com/confluentinc/ksql/commit/ccc636d)), closes [#3863](https://github.com/confluentinc/ksql/issues/3863)
- add configurable metrics reporters ([#3490](https://github.com/confluentinc/ksql/pull/3490)) ([378b8af](https://github.com/confluentinc/ksql/commit/378b8af))
- add flag to disable pull queries (MINOR) ([#3778](https://github.com/confluentinc/ksql/pull/3778)) ([04e206f](https://github.com/confluentinc/ksql/commit/04e206f))
- add health check endpoint ([#3501](https://github.com/confluentinc/ksql/pull/3501)) ([2308686](https://github.com/confluentinc/ksql/commit/2308686))
- add KsqlUncaughtExceptionHandler and new KsqlRestConfig for enabling it ([#3425](https://github.com/confluentinc/ksql/pull/3425)) ([d83c787](https://github.com/confluentinc/ksql/commit/d83c787))
- add request logging ([#3518](https://github.com/confluentinc/ksql/pull/3518)) ([c401ec0](https://github.com/confluentinc/ksql/commit/c401ec0))
- Add UDF invoker benchmark ([#3592](https://github.com/confluentinc/ksql/pull/3592)) ([83dfc24](https://github.com/confluentinc/ksql/commit/83dfc24))
- Added UDFs ENTRIES and GENERATE_SERIES ([#3724](https://github.com/confluentinc/ksql/pull/3724)) ([0a4558b](https://github.com/confluentinc/ksql/commit/0a4558b))
- build ks app from an execution plan visitor ([#3418](https://github.com/confluentinc/ksql/pull/3418)) ([b57d194](https://github.com/confluentinc/ksql/commit/b57d194))
- build materializations from the physical plan ([#3494](https://github.com/confluentinc/ksql/pull/3494)) ([f45d649](https://github.com/confluentinc/ksql/commit/f45d649))
- change /metadata REST path to /v1/metadata ([#3467](https://github.com/confluentinc/ksql/pull/3467)) ([ed94895](https://github.com/confluentinc/ksql/commit/ed94895))
- Config file for the no-response bot which closes issues which haven't been responded to ([#3765](https://github.com/confluentinc/ksql/pull/3765)) ([1dfdb68](https://github.com/confluentinc/ksql/commit/1dfdb68))
- drop legacy key field functionality ([#3764](https://github.com/confluentinc/ksql/pull/3764)) ([5369dc2](https://github.com/confluentinc/ksql/commit/5369dc2))
- expose query status through EXPLAIN ([#3570](https://github.com/confluentinc/ksql/pull/3570)) ([8ef82eb](https://github.com/confluentinc/ksql/commit/8ef82eb))
- expression support for insert values ([#3612](https://github.com/confluentinc/ksql/pull/3612)) ([37f9763](https://github.com/confluentinc/ksql/commit/37f9763))
- Implement complex expressions for table functions ([#3683](https://github.com/confluentinc/ksql/pull/3683)) ([200022b](https://github.com/confluentinc/ksql/commit/200022b))
- Implement describe and list functions for UDTFs ([#3716](https://github.com/confluentinc/ksql/pull/3716)) ([b0bbea4](https://github.com/confluentinc/ksql/commit/b0bbea4))
- Implement EXPLODE(ARRAY) for single table function in SELECT ([#3589](https://github.com/confluentinc/ksql/pull/3589)) ([8b52aa8](https://github.com/confluentinc/ksql/commit/8b52aa8))
- Implement schemaProvider for UDTFs ([#3690](https://github.com/confluentinc/ksql/pull/3690)) ([4e66825](https://github.com/confluentinc/ksql/commit/4e66825))
- Implement user defined table functions ([#3687](https://github.com/confluentinc/ksql/pull/3687)) ([e62bd46](https://github.com/confluentinc/ksql/commit/e62bd46))
- Makes timeout for owner lookup in StaticQueryExecutor and rebalancing in KsStateStore configurable ([#3856](https://github.com/confluentinc/ksql/pull/3856)) ([39245c6](https://github.com/confluentinc/ksql/commit/39245c6))
- pass auth header to connect client for RBAC integration ([#3492](https://github.com/confluentinc/ksql/pull/3492)) ([cef0ea3](https://github.com/confluentinc/ksql/commit/cef0ea3))
- serialize expressions ([#3721](https://github.com/confluentinc/ksql/pull/3721)) ([e1cd477](https://github.com/confluentinc/ksql/commit/e1cd477))
- Support multiple table functions in queries ([#3685](https://github.com/confluentinc/ksql/pull/3685)) ([44be5a2](https://github.com/confluentinc/ksql/commit/44be5a2))
- support numeric json serde for decimals ([#3588](https://github.com/confluentinc/ksql/pull/3588)) ([8621594](https://github.com/confluentinc/ksql/commit/8621594))
- support quoted identifiers in column names ([#3477](https://github.com/confluentinc/ksql/pull/3477)) ([be2bdcc](https://github.com/confluentinc/ksql/commit/be2bdcc))
- Transactional Produces to Command Topic ([#3660](https://github.com/confluentinc/ksql/pull/3660)) ([cba2877](https://github.com/confluentinc/ksql/commit/cba2877))
- **static:** allow logical schema to have fields in any order ([#3422](https://github.com/confluentinc/ksql/pull/3422)) ([d935af3](https://github.com/confluentinc/ksql/commit/d935af3))
- **static:** allow windowed queries without bounds on rowtime ([#3438](https://github.com/confluentinc/ksql/pull/3438)) ([6593ee3](https://github.com/confluentinc/ksql/commit/6593ee3))
- **static:** fail on ROWTIME in projection ([#3430](https://github.com/confluentinc/ksql/pull/3430)) ([2f27b68](https://github.com/confluentinc/ksql/commit/2f27b68))
- **static:** support for partial datetimes on `WindowStart` bounds ([#3435](https://github.com/confluentinc/ksql/pull/3435)) ([99f6e24](https://github.com/confluentinc/ksql/commit/99f6e24))
- **static:** support ROWKEY in the projection of static queries ([#3439](https://github.com/confluentinc/ksql/pull/3439)) ([9218766](https://github.com/confluentinc/ksql/commit/9218766))
- **static:** switch partial datetime parser to use UTC by default ([#3473](https://github.com/confluentinc/ksql/pull/3473)) ([81557e3](https://github.com/confluentinc/ksql/commit/81557e3))
- **static:** unordered table elements and meta columns serialization ([#3428](https://github.com/confluentinc/ksql/pull/3428)) ([3b23fd6](https://github.com/confluentinc/ksql/commit/3b23fd6))
- add KsqlRocksDBConfigSetter to bound memory and set num threads ([#3167](https://github.com/confluentinc/ksql/pull/3167)) ([cdcaa2d](https://github.com/confluentinc/ksql/commit/cdcaa2d))
- add logs/ to .gitignore (MINOR) ([#3353](https://github.com/confluentinc/ksql/pull/3353)) ([81272cf](https://github.com/confluentinc/ksql/commit/81272cf))
- add offest to QueuedCommand and flag to Command ([#3343](https://github.com/confluentinc/ksql/pull/3343)) ([fd112a4](https://github.com/confluentinc/ksql/commit/fd112a4))
- add option for datagen to produce indefinitely (MINOR) ([#3307](https://github.com/confluentinc/ksql/pull/3307)) ([6281738](https://github.com/confluentinc/ksql/commit/6281738))
- add REST /metadata path to display KSQL server information (replaces /info) ([#3313](https://github.com/confluentinc/ksql/pull/3313)) ([8be29b9](https://github.com/confluentinc/ksql/commit/8be29b9))
- add SHOW TYPES to list all custom types ([#3280](https://github.com/confluentinc/ksql/pull/3280)) ([13fde33](https://github.com/confluentinc/ksql/commit/13fde33))
- Add support for average aggregate function ([#3302](https://github.com/confluentinc/ksql/pull/3302)) ([6757d9f](https://github.com/confluentinc/ksql/commit/6757d9f))
- back out Connect auto-import feature ([#3386](https://github.com/confluentinc/ksql/pull/3386)) ([d4c748c](https://github.com/confluentinc/ksql/commit/d4c748c))
- build execution plan from structured package ([#3285](https://github.com/confluentinc/ksql/pull/3285)) ([0d0b1c3](https://github.com/confluentinc/ksql/commit/0d0b1c3))
- change the public API of schema provider method ([#3287](https://github.com/confluentinc/ksql/pull/3287)) ([1324285](https://github.com/confluentinc/ksql/commit/1324285))
- custom comparators for array, map and struct ([#3385](https://github.com/confluentinc/ksql/pull/3385)) ([fe63d21](https://github.com/confluentinc/ksql/commit/fe63d21))
- Implement ROUND() UDF ([#3404](https://github.com/confluentinc/ksql/pull/3404)) ([f9783a9](https://github.com/confluentinc/ksql/commit/f9783a9))
- Implement user defined delimiter for value format ([#3393](https://github.com/confluentinc/ksql/pull/3393)) ([b84d0aa](https://github.com/confluentinc/ksql/commit/b84d0aa))
- move aggregation to plan builder ([#3391](https://github.com/confluentinc/ksql/pull/3391)) ([3aaeb73](https://github.com/confluentinc/ksql/commit/3aaeb73))
- move filters to plan builder ([#3346](https://github.com/confluentinc/ksql/pull/3346)) ([d4d52f3](https://github.com/confluentinc/ksql/commit/d4d52f3))
- move groupBy into plan builders ([#3359](https://github.com/confluentinc/ksql/pull/3359)) ([730c913](https://github.com/confluentinc/ksql/commit/730c913))
- move joins to plan builder ([#3361](https://github.com/confluentinc/ksql/pull/3361)) ([e243c74](https://github.com/confluentinc/ksql/commit/e243c74))
- move selectKey impl to plan builder ([#3362](https://github.com/confluentinc/ksql/pull/3362)) ([f312fcc](https://github.com/confluentinc/ksql/commit/f312fcc))
- move setup of the sink to plan builders ([#3360](https://github.com/confluentinc/ksql/pull/3360)) ([bfbdc20](https://github.com/confluentinc/ksql/commit/bfbdc20))
- remove equalsIgnoreCase from all Name classes (MINOR) ([#3411](https://github.com/confluentinc/ksql/pull/3411)) ([b78619c](https://github.com/confluentinc/ksql/commit/b78619c))
- update query id generation to use command topic record offset ([#3354](https://github.com/confluentinc/ksql/pull/3354)) ([295314e](https://github.com/confluentinc/ksql/commit/295314e))
- **cli:** add the feature to turn of WRAP for CLI output ([#3341](https://github.com/confluentinc/ksql/pull/3341)) ([3814c71](https://github.com/confluentinc/ksql/commit/3814c71))
- **static:** add custom jackson JSON serde for handling LogicalSchema ([#3322](https://github.com/confluentinc/ksql/pull/3322)) ([c571508](https://github.com/confluentinc/ksql/commit/c571508))
- **static:** add forEach() to KsqlStruct (MINOR) ([#3320](https://github.com/confluentinc/ksql/pull/3320)) ([587b545](https://github.com/confluentinc/ksql/commit/587b545))
- **static:** initial syntax for static queries ([#3300](https://github.com/confluentinc/ksql/pull/3300)) ([8917e48](https://github.com/confluentinc/ksql/commit/8917e48))
- **static:** static select support ([#3369](https://github.com/confluentinc/ksql/pull/3369)) ([e4b3275](https://github.com/confluentinc/ksql/commit/e4b3275))
- move toTable kstreams calls to plan builder ([#3334](https://github.com/confluentinc/ksql/pull/3334)) ([06aa252](https://github.com/confluentinc/ksql/commit/06aa252))
- use coherent naming scheme for generated java code ([#3417](https://github.com/confluentinc/ksql/pull/3417)) ([06a2a57](https://github.com/confluentinc/ksql/commit/06a2a57))
- **static:** initial drop of static query functionality ([#3340](https://github.com/confluentinc/ksql/pull/3340)) ([54c5139](https://github.com/confluentinc/ksql/commit/54c5139))
- add ability to DROP custom types ([#3281](https://github.com/confluentinc/ksql/pull/3281)) ([32005ed](https://github.com/confluentinc/ksql/commit/32005ed))
- Add schema resolver method to UDF specification ([#3215](https://github.com/confluentinc/ksql/pull/3215)) ([08855ad](https://github.com/confluentinc/ksql/commit/08855ad))
- add support to register custom types with KSQL (`CREATE TYPE`) ([#3266](https://github.com/confluentinc/ksql/pull/3266)) ([08ffebf](https://github.com/confluentinc/ksql/commit/08ffebf))
- include error message in DESCRIBE CONNECTOR (MINOR) ([#3289](https://github.com/confluentinc/ksql/pull/3289)) ([458f1d8](https://github.com/confluentinc/ksql/commit/458f1d8))
- perform topic permission checks for KSQL service principal ([#3261](https://github.com/confluentinc/ksql/pull/3261)) ([ba1f613](https://github.com/confluentinc/ksql/commit/ba1f613))
- wire in the KS config needed for point queries (MINOR) ([#3251](https://github.com/confluentinc/ksql/pull/3251)) ([5152d06](https://github.com/confluentinc/ksql/commit/5152d06))
- add a new module ksql-execution for the execution plan interfaces ([#3125](https://github.com/confluentinc/ksql/pull/3125)) ([3251d25](https://github.com/confluentinc/ksql/commit/3251d25))
- add an initial set of execution steps ([#3214](https://github.com/confluentinc/ksql/pull/3214)) ([c860793](https://github.com/confluentinc/ksql/commit/c860793))
- add config for custom metrics tags (5.3.x) ([#2996](https://github.com/confluentinc/ksql/pull/2996)) ([76f5590](https://github.com/confluentinc/ksql/commit/76f5590))
- add config for enabling topic access validator ([#3079](https://github.com/confluentinc/ksql/pull/3079)) ([440e247](https://github.com/confluentinc/ksql/commit/440e247))
- add connect templates and simplify JDBC source (MINOR) ([#3231](https://github.com/confluentinc/ksql/pull/3231)) ([ba0fb99](https://github.com/confluentinc/ksql/commit/ba0fb99))
- add DESCRIBE functionality for connectors ([#3206](https://github.com/confluentinc/ksql/pull/3206)) ([a79adb4](https://github.com/confluentinc/ksql/commit/a79adb4))
- add DROP CONNECTOR functionality ([#3245](https://github.com/confluentinc/ksql/pull/3245)) ([103c958](https://github.com/confluentinc/ksql/commit/103c958))
- add extension for custom metrics (5.3.x) ([#2997](https://github.com/confluentinc/ksql/pull/2997)) ([94a8ae7](https://github.com/confluentinc/ksql/commit/94a8ae7))
- add Logarithm, Exponential and Sqrt functions ([#3091](https://github.com/confluentinc/ksql/pull/3091)) ([a4ca934](https://github.com/confluentinc/ksql/commit/a4ca934))
- add SHOW CONNECTORS functionality ([#3210](https://github.com/confluentinc/ksql/pull/3210)) ([0bf31eb](https://github.com/confluentinc/ksql/commit/0bf31eb))
- Add SHOW TOPICS EXTENDED ([#3183](https://github.com/confluentinc/ksql/pull/3183)) ([dd3eb5f](https://github.com/confluentinc/ksql/commit/dd3eb5f)), closes [#1268](https://github.com/confluentinc/ksql/issues/1268)
- add SIGN, REPLACE and INITCAP ([#3189](https://github.com/confluentinc/ksql/pull/3189)) ([ab67684](https://github.com/confluentinc/ksql/commit/ab67684))
- Add window size for time window tables ([#3102](https://github.com/confluentinc/ksql/pull/3102)) ([6ff07d5](https://github.com/confluentinc/ksql/commit/6ff07d5))
- allow for decimals to be used as input types for UDFs ([#3217](https://github.com/confluentinc/ksql/pull/3217)) ([4a2e4b9](https://github.com/confluentinc/ksql/commit/4a2e4b9))
- enhance datagen for use as a load generator ([#3230](https://github.com/confluentinc/ksql/pull/3230)) ([ddb970b](https://github.com/confluentinc/ksql/commit/ddb970b))
- Extend UdfLoader to allow loading specific classes with UDFs ([#3234](https://github.com/confluentinc/ksql/pull/3234)) ([99c79b3](https://github.com/confluentinc/ksql/commit/99c79b3))
- format error messages better (MINOR) ([#3233](https://github.com/confluentinc/ksql/pull/3233)) ([c727d50](https://github.com/confluentinc/ksql/commit/c727d50))
- improved error message and updated error code for PrintTopics command ([#3246](https://github.com/confluentinc/ksql/pull/3246)) ([4b94f22](https://github.com/confluentinc/ksql/commit/4b94f22))
- some robustness improvements for Connect integration ([#3227](https://github.com/confluentinc/ksql/pull/3227)) ([bc1a2f8](https://github.com/confluentinc/ksql/commit/bc1a2f8))
- spin up a connect worker embedded inside the KSQL JVM ([#3241](https://github.com/confluentinc/ksql/pull/3241)) ([4d7ef2a](https://github.com/confluentinc/ksql/commit/4d7ef2a))
- validate createTopic permissions on SandboxedKafkaTopicClient ([#3250](https://github.com/confluentinc/ksql/pull/3250)) ([0ea157b](https://github.com/confluentinc/ksql/commit/0ea157b))
- **data-gen:** support KAFKA format in DataGen ([#3120](https://github.com/confluentinc/ksql/pull/3120)) ([cb7abcc](https://github.com/confluentinc/ksql/commit/cb7abcc))
- **ksql-connect:** poll connect-configs and auto register sources ([#3178](https://github.com/confluentinc/ksql/pull/3178)) ([6dd21fd](https://github.com/confluentinc/ksql/commit/6dd21fd))
- wrap timestamps in ROWTIME expressions with STRINGTOTIMESTAMP ([#3160](https://github.com/confluentinc/ksql/pull/3160)) ([42acd78](https://github.com/confluentinc/ksql/commit/42acd78))
- **ksql-connect:** introduce ConnectClient for REST requests ([#3137](https://github.com/confluentinc/ksql/pull/3137)) ([15548ce](https://github.com/confluentinc/ksql/commit/15548ce))
- **ksql-connect:** introduce syntax for CREATE CONNECTOR (syntax only) ([#3139](https://github.com/confluentinc/ksql/pull/3139)) ([e823659](https://github.com/confluentinc/ksql/commit/e823659))
- **ksql-connect:** wiring for creating connectors ([#3149](https://github.com/confluentinc/ksql/pull/3149)) ([cd20d57](https://github.com/confluentinc/ksql/commit/cd20d57))
- **udfs:** generic support for UDFs ([#3054](https://github.com/confluentinc/ksql/pull/3054)) ([a381c48](https://github.com/confluentinc/ksql/commit/a381c48))
- **cli:** improve CLI transient queries with headers and spacing (partial fix for [#892](https://github.com/confluentinc/ksql/issues/892)) ([#3047](https://github.com/confluentinc/ksql/pull/3047)) ([050b72a](https://github.com/confluentinc/ksql/commit/050b72a))
- **serde:** kafka format ([#3065](https://github.com/confluentinc/ksql/pull/3065)) ([2b5c3d1](https://github.com/confluentinc/ksql/commit/2b5c3d1))
- add basic support for key syntax ([#3034](https://github.com/confluentinc/ksql/pull/3034)) ([ca6478a](https://github.com/confluentinc/ksql/commit/ca6478a))
- Add REST and Websocket authorization hooks and interface ([#3000](https://github.com/confluentinc/ksql/pull/3000)) ([39af991](https://github.com/confluentinc/ksql/commit/39af991))
- Arrays should be indexable from the end too ([#3004](https://github.com/confluentinc/ksql/pull/3004)) ([a166075](https://github.com/confluentinc/ksql/commit/a166075)), closes [#2974](https://github.com/confluentinc/ksql/issues/2974)
- decimal math with other numbers ([#3001](https://github.com/confluentinc/ksql/pull/3001)) ([14d2bb7](https://github.com/confluentinc/ksql/commit/14d2bb7))
- New UNIX_TIMESTAMP and UNIX_DATE datetime functions ([#2459](https://github.com/confluentinc/ksql/pull/2459)) ([39ce7f4](https://github.com/confluentinc/ksql/commit/39ce7f4))

### Performance Improvements

- do not spam the logs with config defs ([#3044](https://github.com/confluentinc/ksql/pull/3044)) ([94904a3](https://github.com/confluentinc/ksql/commit/94904a3))
- Only look up index of new key field once, not per row processed ([#3020](https://github.com/confluentinc/ksql/pull/3020)) ([fda1c7f](https://github.com/confluentinc/ksql/commit/fda1c7f))
- Remove parsing of integer literals ([#3019](https://github.com/confluentinc/ksql/pull/3019)) ([6195b76](https://github.com/confluentinc/ksql/commit/6195b76))

### Bug Fixes

- `/query` rest endpoint should return valid JSON ([#3819](https://github.com/confluentinc/ksql/pull/3819)) ([b278e83](https://github.com/confluentinc/ksql/commit/b278e83))
- address upstream change in KafkaAvroDeserializer ([#3372](https://github.com/confluentinc/ksql/pull/3372)) ([b32e6a9](https://github.com/confluentinc/ksql/commit/b32e6a9))
- address upstream change in KafkaAvroDeserializer (revert previous fix) ([#3437](https://github.com/confluentinc/ksql/pull/3437)) ([bed164b](https://github.com/confluentinc/ksql/commit/bed164b))
- allow streams topic prefixed configs ([#3691](https://github.com/confluentinc/ksql/pull/3691)) ([939c45a](https://github.com/confluentinc/ksql/commit/939c45a)), closes [#817](https://github.com/confluentinc/ksql/issues/817)
- apply filter before flat mapping in logical planner ([#3730](https://github.com/confluentinc/ksql/pull/3730)) ([f4bd083](https://github.com/confluentinc/ksql/commit/f4bd083))
- band-aid around RestQueryTranslationTest ([#3326](https://github.com/confluentinc/ksql/pull/3326)) ([677e03c](https://github.com/confluentinc/ksql/commit/677e03c))
- be more lax on validating config ([#3599](https://github.com/confluentinc/ksql/pull/3599)) ([3c80cf1](https://github.com/confluentinc/ksql/commit/3c80cf1)), closes [#2279](https://github.com/confluentinc/ksql/issues/2279)
- better error message if tz invalid in datetime string ([#3449](https://github.com/confluentinc/ksql/pull/3449)) ([e93c445](https://github.com/confluentinc/ksql/commit/e93c445))
- better error message when users enter old style syntax for query ([#3397](https://github.com/confluentinc/ksql/pull/3397)) ([f948ec0](https://github.com/confluentinc/ksql/commit/f948ec0))
- changes required for compatibility with KIP-479 ([#3466](https://github.com/confluentinc/ksql/pull/3466)) ([567f056](https://github.com/confluentinc/ksql/commit/567f056))
- Created new test for running topology generation manually from the IDE, and removed main() from TopologyFileGenerator ([#3609](https://github.com/confluentinc/ksql/pull/3609)) ([381e563](https://github.com/confluentinc/ksql/commit/381e563))
- do not allow inserts into tables with null key ([#3605](https://github.com/confluentinc/ksql/pull/3605)) ([7e326b7](https://github.com/confluentinc/ksql/commit/7e326b7)), closes [#3021](https://github.com/confluentinc/ksql/issues/3021)
- do not allow WITH clause to be created with invalid WINDOW_SIZE ([#3432](https://github.com/confluentinc/ksql/pull/3432)) ([96bfc11](https://github.com/confluentinc/ksql/commit/96bfc11))
- Don't throw NPE on null columns ([#3647](https://github.com/confluentinc/ksql/pull/3647)) ([6969768](https://github.com/confluentinc/ksql/commit/6969768)), closes [#3617](https://github.com/confluentinc/ksql/issues/3617)
- drop `TopicDescriptionFactory` class ([#3528](https://github.com/confluentinc/ksql/pull/3528)) ([5281c74](https://github.com/confluentinc/ksql/commit/5281c74))
- ensure default server config works with IP6 (fixes [#3309](https://github.com/confluentinc/ksql/issues/3309)) ([#3310](https://github.com/confluentinc/ksql/pull/3310)) ([92b03ec](https://github.com/confluentinc/ksql/commit/92b03ec))
- error message when UDAF has STRUCT type with no schema ([#3407](https://github.com/confluentinc/ksql/pull/3407)) ([49f456e](https://github.com/confluentinc/ksql/commit/49f456e))
- fix Avro schema validation ([#3499](https://github.com/confluentinc/ksql/pull/3499)) ([a59954d](https://github.com/confluentinc/ksql/commit/a59954d))
- fix broken map coersion test case ([#3694](https://github.com/confluentinc/ksql/pull/3694)) ([b5ea24c](https://github.com/confluentinc/ksql/commit/b5ea24c))
- fix NPE when printing records with empty value (MINOR) ([#3470](https://github.com/confluentinc/ksql/pull/3470)) ([47313ff](https://github.com/confluentinc/ksql/commit/47313ff))
- fix parsing issue with LIST property overrides ([#3601](https://github.com/confluentinc/ksql/pull/3601)) ([6459fa1](https://github.com/confluentinc/ksql/commit/6459fa1))
- fix test for KIP-307 final PR ([#3402](https://github.com/confluentinc/ksql/pull/3402)) ([d77db50](https://github.com/confluentinc/ksql/commit/d77db50))
- improve error message on query or print statement on /ksql ([#3337](https://github.com/confluentinc/ksql/pull/3337)) ([dae28eb](https://github.com/confluentinc/ksql/commit/dae28eb))
- improve print topic error message when topic does not exist ([#3464](https://github.com/confluentinc/ksql/pull/3464)) ([0fa4d24](https://github.com/confluentinc/ksql/commit/0fa4d24))
- include lower-case identifiers among those that need quotes ([#3723](https://github.com/confluentinc/ksql/pull/3723)) ([62c47bf](https://github.com/confluentinc/ksql/commit/62c47bf))
- make sure use of non threadsafe UdfIndex is synchronized ([#3486](https://github.com/confluentinc/ksql/pull/3486)) ([618aae8](https://github.com/confluentinc/ksql/commit/618aae8))
- pull queries available on `/query` rest & ws endpoint ([#3820](https://github.com/confluentinc/ksql/pull/3820)) ([9a47eaf](https://github.com/confluentinc/ksql/commit/9a47eaf)), closes [#3672](https://github.com/confluentinc/ksql/issues/3672) [#3495](https://github.com/confluentinc/ksql/issues/3495)
- quoted identifiers for source names ([#3695](https://github.com/confluentinc/ksql/pull/3695)) ([7d3cf92](https://github.com/confluentinc/ksql/commit/7d3cf92))
- race condition in KsStateStore ([#3474](https://github.com/confluentinc/ksql/pull/3474)) ([7336389](https://github.com/confluentinc/ksql/commit/7336389))
- Remove dependencies on test-jars from ksql-functional-tests jar ([#3421](https://github.com/confluentinc/ksql/pull/3421)) ([e09d6ad](https://github.com/confluentinc/ksql/commit/e09d6ad))
- Remove duplicate ksql-common dependency in ksql-streams pom ([#3703](https://github.com/confluentinc/ksql/pull/3703)) ([0620906](https://github.com/confluentinc/ksql/commit/0620906))
- Remove unnecessary arg coercion for UDFs ([#3595](https://github.com/confluentinc/ksql/pull/3595)) ([4c42530](https://github.com/confluentinc/ksql/commit/4c42530))
- Rename Delimiter:parse(char c) to Delimiter.of(char c) ([#3433](https://github.com/confluentinc/ksql/pull/3433)) ([8716c41](https://github.com/confluentinc/ksql/commit/8716c41))
- renamed method to avoid checkstyle error ([#3652](https://github.com/confluentinc/ksql/pull/3652)) ([a8a3588](https://github.com/confluentinc/ksql/commit/a8a3588))
- revert ipv6 address and update docs ([#3314](https://github.com/confluentinc/ksql/pull/3314)) ([0ff4a51](https://github.com/confluentinc/ksql/commit/0ff4a51))
- Revert named stores in expected topologies, disable naming stores from StreamJoined, re-enable join tests. ([#3550](https://github.com/confluentinc/ksql/pull/3550)) ([0b8ccc1](https://github.com/confluentinc/ksql/commit/0b8ccc1)), closes [#3364](https://github.com/confluentinc/ksql/issues/3364)
- should be able to parse empty STRUCT schema (MINOR) ([#3318](https://github.com/confluentinc/ksql/pull/3318)) ([a6549e1](https://github.com/confluentinc/ksql/commit/a6549e1))
- Some renaming around KsqlFunction etc ([#3747](https://github.com/confluentinc/ksql/pull/3747)) ([b30d965](https://github.com/confluentinc/ksql/commit/b30d965))
- standardize KSQL up-casting ([#3516](https://github.com/confluentinc/ksql/pull/3516)) ([7fe8772](https://github.com/confluentinc/ksql/commit/7fe8772))
- support NULL return values from CASE statements ([#3531](https://github.com/confluentinc/ksql/pull/3531)) ([eb9e41b](https://github.com/confluentinc/ksql/commit/eb9e41b))
- support UDAFs with different intermediate schema ([#3412](https://github.com/confluentinc/ksql/pull/3412)) ([70e10e9](https://github.com/confluentinc/ksql/commit/70e10e9))
- switch AdminClient to be sandbox proxy ([#3351](https://github.com/confluentinc/ksql/pull/3351)) ([6747d5c](https://github.com/confluentinc/ksql/commit/6747d5c))
- typo in static query WHERE clause example ([#3423](https://github.com/confluentinc/ksql/pull/3423)) ([7ad3248](https://github.com/confluentinc/ksql/commit/7ad3248))
- Update repartition processor names for KAFKA-9098 ([#3802](https://github.com/confluentinc/ksql/pull/3802)) ([2b86cd8](https://github.com/confluentinc/ksql/commit/2b86cd8))
- Use the correct Immutable interface ([#3488](https://github.com/confluentinc/ksql/pull/3488)) ([a1096bf](https://github.com/confluentinc/ksql/commit/a1096bf))
- **3356:** struct rewritter missed EXPLAIN ([#3398](https://github.com/confluentinc/ksql/pull/3398)) ([daf974b](https://github.com/confluentinc/ksql/commit/daf974b))
- **3441:** stabilize the StaticQueryFunctionalTest ([#3442](https://github.com/confluentinc/ksql/pull/3442)) ([44ae3a0](https://github.com/confluentinc/ksql/commit/44ae3a0)), closes [#3441](https://github.com/confluentinc/ksql/issues/3441)
- **3524:** improve pull query error message ([#3540](https://github.com/confluentinc/ksql/pull/3540)) ([2be8385](https://github.com/confluentinc/ksql/commit/2be8385)), closes [#3524](https://github.com/confluentinc/ksql/issues/3524)
- **3525:** sET should only affect statements after it ([#3529](https://github.com/confluentinc/ksql/pull/3529)) ([5315f1e](https://github.com/confluentinc/ksql/commit/5315f1e)), closes [#3525](https://github.com/confluentinc/ksql/issues/3525)
- address deprecation of getAdminClient ([#3276](https://github.com/confluentinc/ksql/pull/3276)) ([6a50fca](https://github.com/confluentinc/ksql/commit/6a50fca))
- error message with DROP DELETE TOPIC is invalid ([#3279](https://github.com/confluentinc/ksql/pull/3279)) ([4284b8c](https://github.com/confluentinc/ksql/commit/4284b8c))
- find bugs issue in KafkaTopicClientImpl ([#3268](https://github.com/confluentinc/ksql/pull/3268)) ([70e880f](https://github.com/confluentinc/ksql/commit/70e880f))
- fixed how wrapped KsqlTopicAuthorizationException error messages are displayed ([#3258](https://github.com/confluentinc/ksql/pull/3258)) ([63672ae](https://github.com/confluentinc/ksql/commit/63672ae))
- improve escaping of identifiers ([#3295](https://github.com/confluentinc/ksql/pull/3295)) ([04435d7](https://github.com/confluentinc/ksql/commit/04435d7))
- respect reserved words in more clauses for SqlFormatter (MINOR) ([#3284](https://github.com/confluentinc/ksql/pull/3284)) ([6974a80](https://github.com/confluentinc/ksql/commit/6974a80))
- schema converters should handle List and Map impl ([#3290](https://github.com/confluentinc/ksql/pull/3290)) ([af779dc](https://github.com/confluentinc/ksql/commit/af779dc))
- `COLLECT_LIST` can now be applied to tables ([#3104](https://github.com/confluentinc/ksql/pull/3104)) ([c239785](https://github.com/confluentinc/ksql/commit/c239785))
- add ksql-functional-tests to the ksql package ([#3111](https://github.com/confluentinc/ksql/pull/3111)) ([9548135](https://github.com/confluentinc/ksql/commit/9548135))
- authorization filter is logging incorrect username ([#3138](https://github.com/confluentinc/ksql/pull/3138)) ([b15c6d0](https://github.com/confluentinc/ksql/commit/b15c6d0))
- broken build due to bad import statements ([#3204](https://github.com/confluentinc/ksql/pull/3204)) ([8ec4c2b](https://github.com/confluentinc/ksql/commit/8ec4c2b))
- check for other sources using a topic before deleting ([#3070](https://github.com/confluentinc/ksql/pull/3070)) ([b3fa315](https://github.com/confluentinc/ksql/commit/b3fa315))
- default timestamp extractor override is not working ([#3176](https://github.com/confluentinc/ksql/pull/3176)) ([d1db07b](https://github.com/confluentinc/ksql/commit/d1db07b))
- drop succeeds even if missing topic or schema ([#3131](https://github.com/confluentinc/ksql/pull/3131)) ([ba03d6f](https://github.com/confluentinc/ksql/commit/ba03d6f))
- dummy placeholder class in ksql-execution ([#3142](https://github.com/confluentinc/ksql/pull/3142)) ([c9f1cbb](https://github.com/confluentinc/ksql/commit/c9f1cbb))
- expose user/password command line options ([#3129](https://github.com/confluentinc/ksql/pull/3129)) ([1fd70fa](https://github.com/confluentinc/ksql/commit/1fd70fa))
- filter null entries before creating KafkaConfigStore ([#3147](https://github.com/confluentinc/ksql/pull/3147)) ([2852af1](https://github.com/confluentinc/ksql/commit/2852af1))
- fix auth error message with insert values command ([#3257](https://github.com/confluentinc/ksql/pull/3257)) ([abe410a](https://github.com/confluentinc/ksql/commit/abe410a))
- Implement new KIP-455 AdminClient AlterPartitionReassignments and tPartitionReassignments APIs ([#3218](https://github.com/confluentinc/ksql/pull/3218)) ([d951026](https://github.com/confluentinc/ksql/commit/d951026))
- incorrect SR authorization message is displayed ([#3186](https://github.com/confluentinc/ksql/pull/3186)) ([b3b6c82](https://github.com/confluentinc/ksql/commit/b3b6c82))
- logicalSchema toString() to include key fields (MINOR) ([#3123](https://github.com/confluentinc/ksql/pull/3123)) ([0984529](https://github.com/confluentinc/ksql/commit/0984529))
- Remove delete.topic.enable check ([#3089](https://github.com/confluentinc/ksql/pull/3089)) ([71ec1c0](https://github.com/confluentinc/ksql/commit/71ec1c0))
- remove non-standard JavaFx usage of Pair ([#3145](https://github.com/confluentinc/ksql/pull/3145)) ([3508847](https://github.com/confluentinc/ksql/commit/3508847))
- replace nashorn @Immutable with errorprone for JDK12 (MINOR) ([#3239](https://github.com/confluentinc/ksql/pull/3239)) ([0c47a34](https://github.com/confluentinc/ksql/commit/0c47a34))
- request / on makeRootRequest instead of /info ([#3197](https://github.com/confluentinc/ksql/pull/3197)) ([7935488](https://github.com/confluentinc/ksql/commit/7935488))
- the QTTs now run through SqlFormatter & other formatting fixes ([#3222](https://github.com/confluentinc/ksql/pull/3222)) ([79da68c](https://github.com/confluentinc/ksql/commit/79da68c))
- use errorprone Immutable annotation instead of nashorn version ([#3150](https://github.com/confluentinc/ksql/pull/3150)) ([e7f5e17](https://github.com/confluentinc/ksql/commit/e7f5e17))
- `DESCRIBE` now works for sources with decimal types ([#3083](https://github.com/confluentinc/ksql/pull/3083)) ([0eaa101](https://github.com/confluentinc/ksql/commit/0eaa101))
- don't log out to stderr on parser errors ([#3052](https://github.com/confluentinc/ksql/pull/3052)) ([29dea47](https://github.com/confluentinc/ksql/commit/29dea47))
- drop describe topic functionality (MINOR) ([#3072](https://github.com/confluentinc/ksql/pull/3072)) ([1290b82](https://github.com/confluentinc/ksql/commit/1290b82))
- ensure topology generator test runs in build ([#3067](https://github.com/confluentinc/ksql/pull/3067)) ([3168150](https://github.com/confluentinc/ksql/commit/3168150))
- misplaced commas when formatting CTAS statements ([#3058](https://github.com/confluentinc/ksql/pull/3058)) ([c05615d](https://github.com/confluentinc/ksql/commit/c05615d))
- remove any rowtime or rowkey columns from query schema (MINOR) (Fixes 3039) ([#3043](https://github.com/confluentinc/ksql/pull/3043)) ([0346933](https://github.com/confluentinc/ksql/commit/0346933))
- remove last of registered topics stuff from api / cli (MINOR) ([#3068](https://github.com/confluentinc/ksql/pull/3068)) ([24d874c](https://github.com/confluentinc/ksql/commit/24d874c))
- sqlformatter to correctly handle describe ([#3074](https://github.com/confluentinc/ksql/pull/3074)) ([8de57bd](https://github.com/confluentinc/ksql/commit/8de57bd))

### BREAKING CHANGES

- Introduced [`EMIT CHANGES`](https://docs.ksqldb.io/en/latest/operate-and-deploy/ksql-vs-ksqldb/#syntax) syntax to differentiate [push queries](https://docs.ksqldb.io/en/latest/concepts/queries/push/) from new [pull queries](https://docs.ksqldb.io/en/latest/concepts/queries/pull/). Persistent push queries do not yet require an `EMIT CHANGES` clause, but transient push queries do.
- the response from the RESTful API for push queries has changed: it is now a valid JSON document containing a JSON array, where each element is JSON object containing either a row of data, an error message, or a final message. The `terminal` field has been removed.
- the response from the RESTful API for push queries has changed: it now returns a line with the schema and query id in a `header` field and null fields are not included in the payload.
  The CLI is backwards compatible with older versions of the server, though it won't output column headings from older versions.
- If users are relying on the previous behaviour of uppercasing topic names, this change breaks that
- If no value is passed for the KSQL datagen option `iterations`, datagen will now produce indefinitely, rather than terminating after a default of 1,000,000 rows.
- Previously CLI startup permissions were based on whether the user has access to /info, but now it's based on whether the user has access to /. This change implies that if a user has permissions to / but not /info, they now have access to the CLI whereas previously they did not.
- "SHOW TOPICS" no longer includes the "Consumers" and
  "ConsumerGroups" columns. You can use "SHOW TOPICS EXTENDED" to get the
  output previous emitted from "SHOW TOPICS". See below for examples.
  This change splits "SHOW TOPICS" into two commands:
  1. "SHOW TOPICS EXTENDED", which shows what was previously shown by
     "SHOW TOPICS". Sample output:
     `ksql> show topics extended; Kafka Topic | Partitions | Partition Replicas | Consumers | ConsumerGroups -------------------------------------------------------------------------------------------------------------------------------------------------------------- _confluent-command | 1 | 1 | 1 | 1 _confluent-controlcenter-5-3-0-1-actual-group-consumption-rekey | 1 | 1 | 1 | 1`
  2. "SHOW TOPICS", which now no longer queries consumer groups and their
     active consumers. Sample output:
     `ksql> show topics; Kafka Topic | Partitions | Partition Replicas --------------------------------------------------------------------------------------------------------------------------------- _confluent-command | 1 | 1 _confluent-controlcenter-5-3-0-1-actual-group-consumption-rekey | 1 | 1`

## Earlier releases

Release notes for KSQL releases prior to ksqlDB v0.6.0 can be found at
[docs/changelog.rst](https://github.com/confluentinc/ksql/blob/5.4.1-post/docs/changelog.rst).
