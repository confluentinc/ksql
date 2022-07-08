# Auto-generate changelog for ksqlDB releases

The ksqlDB repo uses conventional commits in order to assist with auto-generation of changelogs.

## One-time setup

Install the [tool](https://github.com/conventional-changelog/conventional-changelog/tree/master/packages/conventional-changelog-cli) we use to for generating changelogs:
```
npm install -g conventional-changelog-cli
```

## Generate changelog

Assuming you've completed the one-time setup above, steps for generating a changelog are as follows. Make sure that you pull from master first and that your predecessor has added their release to the changelog already.

Suppose you're interested in generating a changelog of all changes in ksqlDB 0.8.0 that are new as of ksqlDB 0.7.0. Refer to these versions as `NEW_VERSION` and `OLD_VERSION`, respectively. The same process applies for other new/old ksqlDB version combinations as well (e.g., 0.8.1 and 0.8.0, or 0.8.0 and 0.7.1).

These steps assume you're in the root of the `ksql` directory.

### List commits on `NEW_VERSION`

Start by listing all commits from the `NEW_VERSION`, which requires checking out the relevant branch/tag/commit. If the release tag exists:
```
git fetch --all --tags --prune
git checkout tags/v0.8.0-ksqldb
```
If the tag hasn't been created yet but a branch has been cut (e.g., if a release is in progress):
```
git checkout 0.8.x-ksqldb
git fetch upstream
git merge upstream/0.8.x-ksqldb
```
If the branch hasn't been cut yet (which could also be the case for a release currently in progress):
```
git checkout <GIT_SHA>
```

Once on the relevant branch/tag/commit, fetch the commits and save them to a file:
```
conventional-changelog -p angular -i new_commits.txt -s -r 0
```

### List commits on `OLD_VERSION`

Next, repeat the process to list commits from the `OLD_VERSION`. A tag for this release should already exist, so check out the tag:
 
**note: if a previous release is on-going there may not be a tag yet. If this is the case, wait for the previous release to create
their tag or continue with these instructions using the previous release's branch instead of the tag. If you use the previous release's branch,
you will have to manually edit the .txt file to have the right version in the header. For example, if you're working on 0.17, where the header
has `[]` you would want to change it to `[0.17.0]` for `new_commits.txt` and `[0.16.0]` for `old_commits.txt`
```
git fetch --all --tags --prune
git checkout tags/v0.7.0-ksqldb
```
and generate the list of commits:
```
conventional-changelog -p angular -i old_commits.txt -s -r 0
```

### Run the script

Now you're ready to run the script that generates a changelog based on the difference between the two lists of commits from above. In other words, the changelog consists of new commits on `NEW_VERSION` not present on `OLD_VERSION`.

Switch back to master to ensure you have the latest version of the script:
```
git checkout master
```
and run
```
python3 scripts/changelog/process_commit_files.py --release_version 0.8.0 --previous_version 0.7.0 --output_file CHANGELOG_additions.md
```

You should now see a file called `CHANGELOG_additions.md` with the desired changes, which can copied (directly) into the main `CHANGELOG.md`.

When opening a PR to introduce the changelog for a new release, don't forget to also add a section for the new release in
https://github.com/confluentinc/ksql/blob/master/docs/operate-and-deploy/changelog.md.
