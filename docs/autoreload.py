#!/usr/bin/env python
from livereload import Server, shell
import os

server = Server()
remake = shell('make html')
for root, dirs, files in os.walk('.'):
    for fname in files:
        if fname.endswith('.rst'): server.watch(os.path.join(root,fname), remake)
server.serve(root='_build/html/')
