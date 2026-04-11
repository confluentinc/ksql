# ksqlDB Documentation (deprecated)

As of 29 October 2024, this documentation has been retired.

Redirects have been implemented to the main Confluent documentation site at https://docs.confluent.io/platform/current/ksqldb. Going forward, all ksqlDB documentation updates will occur at this location.

Source content for ksqlDB documentation
=======================================

The `docs` directory in the [ksqlDB repo](https://github.com/confluentinc/ksql)
contains the markdown files and other source content for the
[ksqlDB docs](https://docs.ksqldb.io).

Contribute to ksqlDB docs
=========================

You can help to improve the ksqlDB documentation by contributing to this repo:

- Open a [GitHub issue](https://github.com/confluentinc/ksql/issues) and give it
  the `documentation` label.
- Submit a [pull request](https://github.com/confluentinc/ksql/pulls) with your
  proposed documentation changes.
- Get started with
  [writing and formatting on GitHub](https://help.github.com/en/github/writing-on-github/getting-started-with-writing-and-formatting-on-github).

Build docs locally
==================

It's easy to build the documentation on your local machine, so you can preview
as you write.

Prerequisites
-------------

To build the docs on your local machine: 

- Install Python 3.7 or higher: [Properly Installing Python](https://docs.python-guide.org/starting/installation/)
- Install Pip: [Installation](https://pip.pypa.io/en/stable/installing/)
- Clone the [ksqlDB repo](https://github.com/confluentinc/ksql).

Install MkDocs
--------------

The ksqlDB documentation uses the [MkDocs](https://www.mkdocs.org/) static
site generator to build the docs site.

With Python and `pip` installed, use the following command to install `mkdocs`.

```bash
pip install mkdocs
```

For more information, see [Installation](https://www.mkdocs.org/#installation).

Install MkDocs plugins and extensions
-------------------------------------

The ksqlDB documentation build uses these plugins and extensions:

- **mdx_gh_links:** shorthand links to GitHub
- **mkdocs-macros-plugin:** variables and macros  
- **mkdocs-git-revision-date-plugin:** page last updated in GitHub
- **pymdown-extensions:** adds features to the standard Python Markdown library
- **mkdocs-material:** docs site theme
- **mkdocs-redirects:** redirects for moved content
- **mdx-truly-sane-lists:** improved list formatting 

These dependencies and the required versions are listed in the `requirements.txt` 
file in the `docs` directory.

Install the plugins and extensions by using the `pip` installer:

```bash
pip install -r docs/requirements.txt
```

Build the docs
--------------

In your local clone of the ksqlDB repo, build the docs and start a server to enable viewing them:

```bash
mkdocs serve
```

Open a web browser to `http://127.0.0.1:8000` to view the docs. Keep the server
running, and it will detect file changes as you edit and automatically rebuild
when you save a file.