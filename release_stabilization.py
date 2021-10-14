
class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf

    """This is a callback to Confluent's cloud release tooling,
    and allows us to have consistent versioning"""
    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'
