# ksqlDB Documentation

Source content for ksqlDB documentation
=======================================

The `docs-md` directory in the [KSQL repo](https://github.com/confluentinc/ksql)
contains the markdown files and other source content for the
[ksqlDB docs](https://docs.ksqldb.io).

Contribute to ksqlDB docs
=========================

You can help to improve the ksqlDB documentation by contributing to this repo:

- Open a [GitHub issue](https://github.com/confluentinc/ksql/issues) and give it
  the `documentation` label.
- Submit a [pull request](https://github.com/confluentinc/ksql/pulls) with your
  proposed documentation changes.

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

Build the docs
--------------

In your local clone of the ksqlDB repo, navigate to the `docs-md` directory.

```bash
cd docs-md
```

Build the docs and start a server to enable viewing them.

```bash
mkdocs serve
```

Open a web browser to `http://127.0.0.1:8000` to view the docs. Keep the server
running, and it will detect file changes as you edit and automatically rebuild
when you save a file.

Page last revised on: {{ git_revision_date }}
