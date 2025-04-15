# This file is copied from the bazel github repository https://github.com/bazelbuild/bazel/blob/master/tools/compliance
# as it was recently added in latest bazel version but we are currently using old 5.4.0 because of pending
# bazelmod migration for rules_scala package
# *** DO NOT MODIFY THIS FILE UNLESS WE REALLY NEED TO ***

"""Filtered rule kinds for aspect inspection.

The format of this dictionary is:
  rule_name: [attr, attr, ...]

Filters for rules that are not part of the Bazel distribution should be added
to this file.

Attributes are either the explicit list of attributes to filter, or '_*' which
would ignore all attributes prefixed with a _.
"""

# Rule kinds with attributes the aspect currently needs to ignore
user_aspect_filters = {
}
