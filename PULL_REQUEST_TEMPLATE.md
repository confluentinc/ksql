### Description 
_What behavior do you want to change, why, how does your patch achieve the changes?_

### Testing done 
_Describe the testing strategy. Unit and integration tests are expected for any behavior changes._

### Reviewer checklist
- [ ] Ensure docs are updated if necessary. (eg. if a user visible feature is being added or changed).
- [ ] Ensure relevant issues are linked (description should include text like "Fixes #<issue number>")
- [ ] Do these changes have compatibility implications for rollback? If so, ensure that the ksql [command version](https://github.com/confluentinc/ksql/blob/master/ksqldb-rest-app/src/main/java/io/confluent/ksql/rest/server/computation/Command.java#L41) is bumped.
