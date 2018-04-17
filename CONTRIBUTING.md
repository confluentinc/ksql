# Contributing

Thanks for helping us to make KSQL even better!

If you have any questions about how to contribute, either [create a GH issue](https://github.com/confluentinc/ksql/issues) or ask your question in the #ksql channel in our public [Confluent Community Slack](https://slackpass.io/confluentcommunity) (account registration is free and self-service).


## Developing KSQL

### Building and running KSQL locally

To build and run KSQL locally, run the following commands:

```shell
$ mvn clean package -DskipTests
$ ./bin/ksql-server-start -daemon config/ksql-server.properties
$ ./bin/ksql
```

This will start the KSQL server in the background and the KSQL CLI in the
foreground.

If you would rather have the KSQL server logs spool to the console, then
drop the `-daemon` switch, and start the CLI in a second console.


### Testing changes locally

To build and test changes locally, run the following commands:

```shell
$ mvn clean install checkstyle:check integration-test
```


## How to Contribute

### Reporting Bugs and Issues

Report bugs and issues by creating a [new GitHub issue](https://github.com/confluentinc/ksql/issues).
Prior to creating an issue, please search through existing issues so that you are not creating duplicate ones.


### Guidelines for Contributing Code, Examples, Documentation

When submitting a pull request (PR), use the following guidelines:

* Make sure your code respects existing formatting conventions. In general, follow the same coding style as the code that you are modifying.
* Add/update documentation appropriately for the change you are making.
* If you are introducing a new feature you may want to first submit your idea by creating a [new GitHub issue](https://github.com/confluentinc/ksql/issues) to solicit feedback.
* Non-trivial features should include unit tests covering the new functionality.
* Bug fixes should include a unit test or integration test reproducing the issue.
* Try to keep pull requests short and submit separate ones for unrelated features, but feel free to combine simple bugfixes/tests into one pull request.
* Keep the number of commits small and combine commits for related changes.
* Each commit should compile on its own and ideally pass tests.
* Keep formatting changes in separate commits to make code reviews easier and distinguish them from actual code changes.


### GitHub Workflow

1. Fork the `confluentinc/ksql` repository into your GitHub account: https://github.com/confluentinc/ksql/fork.

2. Clone your fork of the GitHub repository, replacing `<username>` with your GitHub username.

   Use ssh (recommended):

   ```bash
   git clone git@github.com:<username>/ksql.git
   ```

   Or https:

   ```bash
   git clone https://github.com/<username>/ksql.git
   ```

3. Add a remote to keep up with upstream changes.

   ```bash
   git remote add upstream https://github.com/confluentinc/ksql.git
   ```

   If you already have a copy, fetch upstream changes.

   ```bash
   git fetch upstream
   ```

4. Create a feature branch to work in.

   ```bash
   git checkout -b feature-xxx remotes/upstream/master
   ```

5. Work in your feature branch.

   ```bash
   git commit -a
   ```

6. Periodically rebase your changes

   ```bash
   git pull --rebase
   ```

7. When done, combine ("squash") related commits into a single one

   ```bash
   git rebase -i upstream/master
   ```

   This will open your editor and allow you to re-order commits and merge them:
   - Re-order the lines to change commit order (to the extent possible without creating conflicts)
   - Prefix commits using `s` (squash) or `f` (fixup) to merge extraneous commits.

8. Submit a pull-request

   ```bash
   git push origin feature-xxx
   ```

   Go to your fork main page

   ```bash
   https://github.com/<username>/ksql
   ```

   If you recently pushed your changes GitHub will automatically pop up a `Compare & pull request` button for any branches you recently pushed to. If you click that button it will automatically offer you to submit your pull-request to the `confluentinc/ksql` repository.

   - Give your pull-request a meaningful title.
   - In the description, explain your changes and the problem they are solving.

9. Addressing code review comments

   Repeat steps 5. through 7. to address any code review comments and rebase your changes if necessary.

   Push your updated changes to update the pull request

   ```bash
   git push origin [--force] feature-xxx
   ```

   `--force` may be necessary to overwrite your existing pull request in case your
  commit history was changed when performing the rebase.

   Note: Be careful when using `--force` since you may lose data if you are not careful.

   ```bash
   git push origin --force feature-xxx
   ```

