# KSQL Improvement Proposals

This is a place to collect improvement proposals for KSQL from the community. Please follow the 
[KLIP Template](klip-template.md) when creating a new KLIP.

We have a two-part process for submitting KLIPs:

## Proposal Intent

This serves as a locking mechanism to make sure that 
- each KLIP has a unique number
- two people are not working on the same KLIP at once

To submit a proposal intent, create a pull request with the following information:
1. title is `docs: intent for KLIP-<number>: <title-of-klip>`
1. update the number for the next KLIP on this readme
1. add a line in the table below for your new KLIP with status `Proposal`

You can expect this PR to be merged swiftly.

## Design Discussion

This is the guts of our improvement proposal process:

1. Create a new branch off of master on your fork
1. Copy the [`klip-template.md`](klip-template.md) file and rename it with your KLIP number and 
   title (e.g. `klip-1-improve-udf-interfaces.md`)
1. Fill in the template with details from your KLIP
1. Submit a Pull Request from your branch to KSQL:
    1. make sure the title is `docs: KLIP-<number>: <title>`
    1. update the table entry below from the Proposal Intent step with a link to your KLIP
1. Share a link to the PR in the `#ksql` channel on the [confluent community slack](https://slackpass.io/confluentcommunity).
1. The design discussion will happen on the pull request
1. The KLIP is approved and merged if at least two people with write access approve the change

# KLIP Directory

The progression of statuses should be: Proposal, Discussion, Approved, Merged

Next KLIP number: **7**

| KLIP                                                                               | Status         | Release |
|------------------------------------------------------------------------------------|:--------------:| ------: |
| [KLIP-X: Template](klip-template.md)                                               | -              | N/A     |
| [KLIP-1: Improve UDF Interfaces](klip-1-improve-udf-interfaces.md)                 | Approved       | N/A     |
| [KLIP-2: Insert Into Semantics](klip-2-produce-data.md)                            | Merged         | 5.3     |
| [KLIP-3: Serialization of single Fields](klip-3-serialization-of-single-fields.md) | Approved       | N/A     |
| [KLIP-4: Custom Type Registry](klip-4-custom-types.md)                             | Approved       | N/A     |
| [KLIP-6: Execution Plans](klip-6-execution-plans.md)                               | Approved       | N/A     |
