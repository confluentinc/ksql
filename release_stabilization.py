
class Callbacks:
    def __init__(self, working_dir, leaf, dry_run):
        self.leaf = leaf

    def version_as_leaf(self):
        return self.leaf == 'cc-docker-ksql'
