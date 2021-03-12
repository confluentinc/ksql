## [0.17.0](https://github.com/confluentinc/ksql/releases/tag/v0.17.0-ksqldb) (2021-03-12)

### Features

* Adds max concurrent pull queries to limit effects of table scans ([#7188](https://github.com/confluentinc/ksql/pull/7188)) ([55f5403](https://github.com/confluentinc/ksql/commit/55f5403eab223ca727ca5a6d4efb10b06b1dd97b))
* CLI should fail for unsupported server version ([#7097](https://github.com/confluentinc/ksql/pull/7097)) ([a0745b9](https://github.com/confluentinc/ksql/commit/a0745b9e1a855be5be3c4cf5c2ffc7e384d29105))
* **migrations:** enable insert values and create connector for migrations tool ([#7161](https://github.com/confluentinc/ksql/pull/7161)) ([2c614cd](https://github.com/confluentinc/ksql/commit/2c614cd80aeae81c71107de16fbf2649ce222b1c))
* **migrations:** enable set/unset statements for migration tool ([#7190](https://github.com/confluentinc/ksql/pull/7190)) ([4151bae](https://github.com/confluentinc/ksql/commit/4151bae93632b9b2ed425db093051d4b58bffa7d))
* add filter lambda udf ([#7075](https://github.com/confluentinc/ksql/pull/7075)) ([d6529a3](https://github.com/confluentinc/ksql/commit/d6529a32d4d280a0b8c66c56228dedfc45a2bf03))
* add lambda feature flag ([#7148](https://github.com/confluentinc/ksql/pull/7148)) ([c8f745e](https://github.com/confluentinc/ksql/commit/c8f745e8ac8f034661234ec7334009df4b50aac1))
* Add timestamp arithmetic functionality ([#6901](https://github.com/confluentinc/ksql/pull/6901)) ([e2c06dc](https://github.com/confluentinc/ksql/commit/e2c06dc47c594215bc956fe29c5a1619508012af))
* add type checking and code gen for lambdas ([#6966](https://github.com/confluentinc/ksql/pull/6966)) ([d09c99e](https://github.com/confluentinc/ksql/commit/d09c99edb29485182e5b18f0612eae338daa41e1))
* added script to export antlr tokens to use in frontend editor ([#7118](https://github.com/confluentinc/ksql/pull/7118)) ([7e1c180](https://github.com/confluentinc/ksql/commit/7e1c1808e535c43c9d5991e24bb8490909b46c13))
* Adds an expression interpreter to be used for pull queries ([#7006](https://github.com/confluentinc/ksql/pull/7006)) ([5d2cd83](https://github.com/confluentinc/ksql/commit/5d2cd8398ba5ae09abf0a1edb47113028aa8e0b7))
* Adds Lambda functionality to the interpreter ([#7152](https://github.com/confluentinc/ksql/pull/7152)) ([643816f](https://github.com/confluentinc/ksql/commit/643816f066bb69610b972c2afa4820d39c5f8670))
* Allows lots of table scans cases where keys cannot easily be extracted ([#7155](https://github.com/confluentinc/ksql/pull/7155)) ([71becea](https://github.com/confluentinc/ksql/commit/71becea4a1fe193004d0e480df70ac30e0a8bc0a))
* Introduce materialization support for tables using CTAS to make tables queryable ([#7085](https://github.com/confluentinc/ksql/pull/7085)) ([65d0df1](https://github.com/confluentinc/ksql/commit/65d0df1c49cd38a208cf16ee05af19559e688787))
* limit the number of active push queries everywhere using "ksql.max.push.queries" config ([#7109](https://github.com/confluentinc/ksql/pull/7109)) ([906f4c5](https://github.com/confluentinc/ksql/commit/906f4c5dee491cfe1366e70b1f06bd45b280f51c))
* **migrations:** add ability to apply specific migration version ([#7137](https://github.com/confluentinc/ksql/pull/7137)) ([608cb5e](https://github.com/confluentinc/ksql/commit/608cb5eabb13cfc65bd6388993a5912da5f6b764))
* **migrations:** apply command ([#7099](https://github.com/confluentinc/ksql/pull/7099)) ([a75c355](https://github.com/confluentinc/ksql/commit/a75c355943ff819aaecc6c73702d138efcf6c089))
* **migrations:** implement `clean` command ([#7153](https://github.com/confluentinc/ksql/pull/7153)) ([2e546b6](https://github.com/confluentinc/ksql/commit/2e546b625d7beaf83645f742839cff44d6e1bc62))
* **migrations:** implement `create` command ([#7133](https://github.com/confluentinc/ksql/pull/7133)) ([c91e23c](https://github.com/confluentinc/ksql/commit/c91e23c0f99b71912cecc00149d07b367d5c81cd))
* **migrations:** implement `info` command ([#7145](https://github.com/confluentinc/ksql/pull/7145)) ([956e799](https://github.com/confluentinc/ksql/commit/956e799e70cf23e7450302888609bd35d2b56823))
* classify authorization exception as user error ([#7061](https://github.com/confluentinc/ksql/pull/7061)) ([a74b77c](https://github.com/confluentinc/ksql/commit/a74b77c15e30319bf98b3d686cf127e97ed44154))
* implement correct logic for nested lambdas and more complex lambda expressions ([#7056](https://github.com/confluentinc/ksql/pull/7056)) ([1a042cd](https://github.com/confluentinc/ksql/commit/1a042cd2bb9babc52a7aa071638aa408a1e47b73))
* introduce transform and reduce invocation function for lambdas ([#6994](https://github.com/confluentinc/ksql/pull/6994)) ([563ff9b](https://github.com/confluentinc/ksql/commit/563ff9b91a9095045c1597a7008359439a8a217c))
* **migrations:** implement `validate` command ([#7087](https://github.com/confluentinc/ksql/pull/7087)) ([d4cf400](https://github.com/confluentinc/ksql/commit/d4cf400cb77e7a9dd0d7810e7de53562197f32fa))



### Bug Fixes

* check for null inputs for various timestamp functions ([#7180](https://github.com/confluentinc/ksql/pull/7180)) ([42496c1](https://github.com/confluentinc/ksql/commit/42496c17b7bb4d2122a62e32d91b1ff321d71433))
* fix the cache max bytes buffering check ([#7181](https://github.com/confluentinc/ksql/pull/7181)) ([f383800](https://github.com/confluentinc/ksql/commit/f3838002fc4c8c085a3f62bfd34073e232416de4))
* get all available restore commands even on poll timeout ([#6985](https://github.com/confluentinc/ksql/pull/6985)) ([28a7ba9](https://github.com/confluentinc/ksql/commit/28a7ba9a3d05a821eea702dcfaff046463a5394c))
* incorporate lambdas into CoercionUtil and change LambdaVariable from Literal to Expression ([#7178](https://github.com/confluentinc/ksql/pull/7178)) ([c882303](https://github.com/confluentinc/ksql/commit/c882303f13de5afb6b3e023b3f21ddf59aa199ef))
* ksql.service.id should not be usable as a query parameter ([#7192](https://github.com/confluentinc/ksql/pull/7192)) ([cc5cd81](https://github.com/confluentinc/ksql/commit/cc5cd81cadc7fb8fb7cbf34c9e98a3a567df746e))
* Make pull query metrics apply only to pull and not also push ([#6944](https://github.com/confluentinc/ksql/pull/6944)) ([1db18b3](https://github.com/confluentinc/ksql/commit/1db18b3e8b161ae7315db67917bb1f5d89da8c2a))
* prevent IOB when printing topics with a key/value with an empty string ([#7162](https://github.com/confluentinc/ksql/pull/7162)) ([177d0db](https://github.com/confluentinc/ksql/commit/177d0dbff72adcded82f9da9f9637b23fc6ffa3c))
* Pull Queries: Avoids KsqlConfig copy with overrides since this is very inefficient ([#7193](https://github.com/confluentinc/ksql/pull/7193)) ([b36a3ce](https://github.com/confluentinc/ksql/commit/b36a3ce7d02dbeec7740d32801d443c4d7bca1e2))
* quick fix for appender ([#7078](https://github.com/confluentinc/ksql/pull/7078)) ([8bc16b3](https://github.com/confluentinc/ksql/commit/8bc16b3f8b6d6a785f65c73c510180db443ef4aa))
* return null on null input for timestamp math functions ([#7159](https://github.com/confluentinc/ksql/pull/7159)) ([1675221](https://github.com/confluentinc/ksql/commit/1675221e2b9342070a6210ffe33bf6a6eac1ad14))
* script used in kci-170 is now exporting an array instead of string ([#7134](https://github.com/confluentinc/ksql/pull/7134)) ([986cbe3](https://github.com/confluentinc/ksql/commit/986cbe38238f05af11eb841801eb715088328496))
* Table Scan: Filters out StreamsMetadata for wrong subtopology ([#7086](https://github.com/confluentinc/ksql/pull/7086)) ([9614720](https://github.com/confluentinc/ksql/commit/9614720fa423c3bebe78c13a2e1f5bf035be7cc3))
* incompatible schema causing KsqlEngineTest.shouldNotFailIfAvroSchemaEvolvable failure ([#7052](https://github.com/confluentinc/ksql/pull/7052)) ([8c9e928](https://github.com/confluentinc/ksql/commit/8c9e9280bb1f76f8d4ef88bae7c3e33426ff3d48))



### Reverts

* Revert "Bump Confluent to 7.0.0-0, Kafka to 7.0.0-0" ([60ebe90](https://github.com/confluentinc/ksql/commit/60ebe90d2aa8fecc66e80239aacf0a49a81d8a5d))



