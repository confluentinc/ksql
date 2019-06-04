# KSQL Improvement proposals

This is a place to collect improvement proposals for KSQL from the community. Please follow the [KLIP Template](klip-template.md) when creating a new KLIP.

Here is the process for submitting a KLIP and getting it approved.

1. Create a new branch off of master on your fork.
1. Copy the `klip-template.md` file and rename it with a klip number and title. For example `klip-0-add-protobuf-support.md`.
1. Fill in the details in KLIP file you created above.
1. Submit a PR from your branch to KSQL. Label it `design-proposal`, and share a link to the PR in the `#ksql` channel on the [confluent community slack](https://slackpass.io/confluentcommunity).
1. Raise a second PR, from a second branch, which has only a change to this page, adding your KLIP to the list below. Set
the status of your KLIP in the table to `[In Discussion](https://github.com/confluentinc/ksql/pull/your_pr_number_here)`, so that the status links to your PR.
1. The design discussion will happen on the PR.
1. The KLIP is approved if at least two people with write access to the KSQL repo +1 the PR.

# KsqL Improvement Proposals (aka KLIPs)

Next KLIP number: 4.

| KLIP                                                                               | Status         | Release |
|------------------------------------------------------------------------------------|:--------------:| ------: |
| [KLIP-X: Template](klip-template.md)                                               | -              | N/A     |
| [KLIP-1: Improve UDF Interfaces](klip-1-improve-udf-interfaces.md)                 | Accepted       | N/A     |
| [KLIP-2: Insert Into Semantics](klip-2-produce-data.md)                            | Accepted       | N/A     |
| [KLIP-3: Serialization of single Fields](klip-3-serialization-of-single-fields.md) | Accepted       | N/A     |



