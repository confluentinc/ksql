import argparse
import datetime
import re

from collections import defaultdict, OrderedDict


CONFLUENT_USERNAME = 'confluentinc'
COMMIT_TYPE_ORDER = ['Features', 'Performance Improvements', 'Bug Fixes']

##################
#### LOGGING #####
##################

VERBOSE = False
def log_info(msg):
	if VERBOSE:
		print('INFO: ' + msg)
def log_warn(msg):
	print('WARN: ' + msg)

##################
###### READ ######
##################

def parse_changelog(filename, release=None):
	'''
	Parses a list of commits and into a dict mapping commit type to commits of that type.
	Also performs basic processing on the returned commits (e.g., replacing fork name with Confluent user).
	Note that blank lines in multiline commit messages are removed as a byproduct of processing.

			Parameters:
					filename (str): file containing list of commits
					release (str): if provided, method will validate that the first header corresponds to the expected release

			Returns:
					commits (dict): maps commit type (str) to dict of commits of that type, where keys are shortened commits 
									(strings, truncated at the first open parenthesis) and values are the full commits (strings).
									Note that the commit type is the final line that will be written, e.g., "### Features" rather 
									than simply "Features"
	'''
	log_info('parsing changelog file %s' % filename)
	return _reformat_and_remove_duplicates(_parse_changelog_with_duplicates(filename, release))

def _parse_changelog_with_duplicates(filename, release=None):
	'''
	Parses a list of commits and into a dict mapping commit type to commits of that type.
	Also performs basic processing on the returned commits (e.g., replacing fork name with Confluent user).
	Note that blank lines in multiline commit messages are removed as a byproduct of processing.

			Parameters:
					filename (str): file containing list of commits
					release (str): if provided, method will validate that the first header corresponds to the expected release

			Returns:
					commits (dict): maps commit type (str) to list of commits (strings) of that type.
									Note that the commit type is the final line that will be written, e.g., "### Features" rather 
									than simply "Features"
	'''
	with open(filename, 'r') as f:
		found_header = True if release is None else False
		fork_name = None
		current_type = None
		commits = defaultdict(list)

		## process input
		for line in f:

			line = line.strip()

			## skip empty lines
			if not line:
				continue

			## verify header
			if _is_header(line):
				if fork_name is None:
					fork_name = _get_fork_name(line)

				if found_header:
					continue
				if _get_version(line).startswith(release):
					found_header = True
				continue
			assert found_header, 'could not find header for expected release'
			assert fork_name is not None, 'no header from which to fetch fork'

			if _is_commit_type(line):
				current_type = line
				continue

			if _is_commit(line):
				assert current_type is not None, 'no commit type found before first commit'
				line = _process_commit_line(fork_name, line, True, _is_breaking_change(current_type))
				commits[current_type].append(line)
				continue

			## multi-line commit. NOTE: this process loses blank lines in multi-line commit messages.
			assert current_type is not None, 'no commit type for active commit'
			assert len(commits[current_type]) != 0, 'no active commit found'
			line = _process_commit_line(fork_name, line, False, _is_breaking_change(current_type))
			commits[current_type][-1] += line

		return commits

def _is_header(line):
	return line[0] == '#' and line[0:3] != '###'

def _is_commit_type(line):
	return line[0:3] == '###'

def _is_commit(line):
	return line[0] == '*'

def _is_breaking_change(type):
	return 'BREAKING CHANGES' in type

def _get_version(header):
	'''
	Returns release version from the header.

			Parameters:
					header (str): A header line from the raw commits file, e.g., "## [5.3.1-rc190904141854](https://github.com/vcrfxia/ksql/compare/v5.3.1...v5.3.1-rc190904141854) (2019-09-04)"

			Returns:
					release_version (str): Release version extracted from the header, e.g., "5.3.1-rc190904141854" in the example above.
	'''
	left_bracket = header.index('[')
	right_bracket = header.index(']')
	assert left_bracket < right_bracket, 'malformed header'
	return header[left_bracket + 1:right_bracket]

def _get_fork_name(header):
	'''
	Returns fork name from the header.

			Parameters:
					header (str): A header line from the raw commits file, e.g., "# [](https://github.com/vcrfxia/ksql/compare/v5.5.0-rc200418032830...v) (2020-05-04)"

			Returns:
					fork_name (str): Repo fork name extracted from the header, e.g., "vcrfxia" in the example above.
	'''
	start_ind = header.index('https://github.com/')
	end_ind = start_ind + header[start_ind:].index('/ksql/compare/')
	return header[start_ind + len('https://github.com/') : end_ind]

##################
#### PROCESS #####
##################

def _reformat_and_remove_duplicates(commits_by_type):
	'''
	Removes duplicate commits from the supplied dictionary of commits. Duplicates are determined based on prefix until the first
	open parenthesis in the commit message, if present, else the entire commit message is used for comparison.

			Parameters:
					commits_by_type (dict): dictionary of commits where keys are commit type (str) and
											values are lists of commits (strings) of that type.

			Returns:
					unique_commits_by_type (dict): maps commit type (str) to dict of commits of that type, where keys are shortened commits 
												   (strings, truncated at the first open parenthesis) and values are the full commits (strings).
	'''
	unique_commits_by_type = {}
	for commit_type, commits in commits_by_type.items():
		unique_messages = OrderedDict() ## shortened message -> full message
		for commit in commits:
			pull_request_ind = commit.find('(')
			shortened_msg = commit if pull_request_ind == -1 else commit[:pull_request_ind]
			if shortened_msg in unique_messages:
				log_info('found duplicate message:\n\tfirst:\n\t' + unique_messages[shortened_msg] + '\n\tsecond:\n\t' + commit + '\n\tdropping duplicate')
			else:
				unique_messages[shortened_msg] = commit
		unique_commits_by_type[commit_type] = unique_messages
	return unique_commits_by_type

def _process_commit_line(fork_name, line, is_first, is_breaking_change):
	'''
	Performs basic processing on a commit line, including updating the repo fork name to be that of the main Confluent repo,
	and correcting pull request links to point to pull requests rather than issues.

			Parameters:
					fork_name (str): repo fork name to be replaced.
					line (str): the commit line to process.
					is_first (bool): whether the line is the first line of a commit. Pull request links will only be updated if so.
					is_breaking_change (bool): whether the line is part of a breaking change message. Pull request links will only be updated if not the case.

			Returns:
					processed_line (str): commit line after processing.
	'''
	line = line.replace(fork_name, CONFLUENT_USERNAME)

	if is_first and not is_breaking_change:
		## update pull request link to correctly link to pull request rather than issue
		spans = [m.span() for m in re.finditer('\(\[#[0-9]+\]\(https://github.com/confluentinc/ksql/issues/[0-9]+\)\)', line)]
		if len(spans) > 0:
			span = spans[0]

			start_i = span[0] + line[span[0]:span[1]].index('issues')
			line = line[:start_i] + 'pull' + line[start_i + len('issues'):]
		else:
			log_info('first line of commit message does not include pull request link. line:\n%s\n' % line)

	return line + '\n'

##################
###### DIFF ######
##################

def get_diff(all_commits_by_type, old_commits_by_type):
	'''
	Returns new commits, i.e., those in <all_commits_by_type> but not <old_commits_by_type>.

			Parameters:
					all_commits_by_type (dict): dictionary of commits where keys are commit type (str) and
												values are an (ordered) dict of commits of that type, where keys are shortened commits 
												(strings, for purposes of deduplication) and values are the full commits (strings).
					old_commits_by_type (dict): same format as above.

			Returns:
					new_commits_by_type (dict): commits present in <all_commits_by_type> but not <old_commits_by_type>.
												Format is a dictionary of commits where keys are commit type (str) and
												values are lists of commits (strings) of that type.
	'''
	diff = {}
	for commit_type, all_commits in all_commits_by_type.items():
		commits = _get_diff_commits(all_commits, old_commits_by_type[commit_type] if commit_type in old_commits_by_type else [])
		if len(commits) > 0:
			diff[commit_type] = commits
	return diff

def _get_diff_commits(all_commits, old_commits):
	'''
	Returns new commits, i.e., those in <all_commits> but not <old_commits>.
	This method de-dups based on the shortened commits (dict keys) in order to avoid duplicates caused
	by commits being cherry-picked to different branches, which causes the full commit message to differ
	as the commit SHAs differ in this case. 

			Parameters:
					all_commits (dict): dictionary of commits where keys are shortened commits (strings, for 
										purposes of deduplication) and values are the full commits (strings).
					old_commits (dict): same format as above.

			Returns:
					new_commits (list): commits present in <all_commits> but not <old_commits>.
										Format is a list of commits (strings).
	'''

	## iterate rather than using set logic in order to preserve order
	new_commits = [commit for k, commit in all_commits.items() if k not in old_commits]

	## sanity check that all_commits is a superset of old_commits
	missing_commits = [commit for k, commit in old_commits.items() if k not in all_commits]
	if len(missing_commits) > 0:
		msg = 'commits present in old commits file but not new commits file\n'
		for commit in missing_commits:
			msg += "\t" + commit + "\n"
		log_warn(msg)

	return new_commits

##################
###### WRITE #####
##################

def write_output(filename, release_version, commits_by_type):
	'''
	Formats and writes commits to the specific output file. Sections for the different commit types
	will appear first in the order specified by <COMMIT_TYPE_ORDER>, followed by any other remaining sections.

			Parameters:
					filename (str): output file name.
					release_version (str): release version which will appear in output header
					commits_by_type (dict): dictionary of commits where keys are commit type (str) and
											values are lists of commits (strings) of that type.
	'''
	header = _generate_header(release_version)
	with open(filename, 'w') as f:
		f.write(header + '\n\n')

		for commit_type in COMMIT_TYPE_ORDER:
			did_write = _write_and_remove_if_present(f, commit_type, commits_by_type)

		## write remaining type
		for commit_type, commits in commits_by_type.items():
			_write_commits_for_type(f, commit_type, commits)

def _write_and_remove_if_present(f, commit_type, commits_by_type):
	commit_type = _find_commit_type(commit_type, commits_by_type)
	if commit_type is not None:
		_write_commits_for_type(f, commit_type, commits_by_type[commit_type])
		del commits_by_type[commit_type]

def _write_commits_for_type(f, commit_type, commits):
	f.write(commit_type + '\n\n')
	for commit in commits:
		f.write(commit)
	f.write('\n\n\n')

def _find_commit_type(commit_type, commits_by_type):
	'''
	Finds a key in <commits_by_type> dict corresponding to the specified <commit_type>, if present.
	A key is considered a match if the specified <commit_type> is a substring of the key.

			Parameters:
					commit_type (str): commit type to search for.
					commits_by_type (dict): dictionary of commits where keys are commit type (str) and
											values are lists of commits (strings) of that type.

			Returns:
					full_commit_type (str): key in <commits_by_type> corresponding to commit_type, if found; else, None.
	'''
	for ct in commits_by_type:
		if commit_type in ct:
			return ct
	return None

def _generate_header(release):
	'''
	Generate header for output changelog section.

			Parameters:
					release (str): new releae version, to be included in header. ex: "0.8.1"

			Returns:
					header (str): header for output changelog. ex: "## [0.8.1](https://github.com/confluentinc/ksql/releases/tag/v0.8.1-ksqldb) (2020-03-30)"
	'''
	date = datetime.datetime.now().strftime("%Y-%m-%d")
	header = "## [%s](https://github.com/%s/ksql/releases/tag/v%s-ksqldb) (%s)" % (release, CONFLUENT_USERNAME, release, date)
	return header

##################
### PARSE ARGS ###
##################

def parse_args():
	parser = argparse.ArgumentParser('Auto-generate ksqlDB changelog.')

	parser.add_argument('--release_version', type=str, required=True, help='ksqlDB version for which a changelog should be generated, e.g., "0.9.0". Will appear at the top of the generated changelog.')
	parser.add_argument('--previous_version', type=str, required=True, help='previous ksqlDB version, e.g., "0.8.1". Must appear in the first subheader of <old_commits_file>.')
	parser.add_argument('--new_commits_file', type=str, default='new_commits.txt', help='file containing commits for the (new) release_version. default: "new_commits.txt"')
	parser.add_argument('--old_commits_file', type=str, default='old_commits.txt', help='file containing commits for the previous_version. default: "old_commits.txt"')
	parser.add_argument('--output_file', type=str, default='CHANGELOG.md', help='output file name. default: "CHANGELOG.md"')

	parser.add_argument('--verbose', action='store_true', help='verbose logging')

	args = parser.parse_args()
	VERBOSE = args.verbose
	return args

if __name__ == '__main__':
	args = parse_args()

	all_commits = parse_changelog(args.new_commits_file)
	old_commits = parse_changelog(args.old_commits_file, args.previous_version)

	new_commits = get_diff(all_commits, old_commits)

	write_output(args.output_file, args.release_version, new_commits)

