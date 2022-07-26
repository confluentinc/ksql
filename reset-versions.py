# This script is intended for use with Packaging (see debian/Makefile),
# to facilitate resetting parent and dependency versions to the
# currently-being-built version number. We cannot rely on the normal
# Maven version plugin because it must be able to resolve the existing
# pom versions, but during packaging, those versions may not be available.

import re
import sys

path = "pom.xml"
new_version = sys.argv[1]

if not new_version:
    raise Exception("Expected the new version to set as an argument")

print("Setting parent and dependencies to {0}".format(new_version))

VERSION_PATTERN = r'(\d+\.\d+\.\d+-\d+)'

POM_PARENT_PATTERN = re.compile(r'.*<parent>.*<groupId>(?:io.confluent|org.apache.kafka)\S*</groupId>.*'
                                r'<version>{0}</version>.*</parent>.*'.format(VERSION_PATTERN), re.DOTALL)

POM_DEPENDENCY_PATTERN = re.compile(r'<dependency>\s*?<groupId>io.confluent\S*?</groupId>\s*?'
                                    r'<artifactId>\S*?</artifactId>\s*?'
                                    r'<version>{0}</version>.*?</dependency>'.format(VERSION_PATTERN), re.DOTALL)


def update_version(match):
    (ms, me), (gs, ge) = match.span(0), match.span(1)  # Indices: m = match, g = group, s = start, e = end
    return match.string[ms:gs] + new_version + match.string[ge:me]


def replace_pattern(pattern, input):
    test_match = pattern.search(input)
    if not test_match:
        raise Exception("Did not find the version we were looking for in {0}: {1}".format(path, pattern))
    groups = test_match.groups()
    return re.sub(pattern, update_version, input)


with open(path) as fh:
    data = fh.read()

parent_replaced = replace_pattern(POM_PARENT_PATTERN, data)
deps_replaced = replace_pattern(POM_DEPENDENCY_PATTERN, parent_replaced)

if deps_replaced != data:
    with open(path, "w") as fh:
        fh.write(deps_replaced)
    print("done!")
else:
    print("nothing to do!")
