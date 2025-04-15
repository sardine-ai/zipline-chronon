# This file is copied from the bazel github repository https://github.com/bazelbuild/bazel/blob/master/tools/compliance
# as it was recently added in latest bazel version but we are currently using old 5.4.0 because of pending
# bazelmod migration for rules_scala package
# *** DO NOT MODIFY THIS FILE UNLESS WE REALLY NEED TO ***

"""Smoke test for packages_used."""

import json
import os
import unittest


def read_data_file(basename):
    path = os.path.join(
        os.getenv("TEST_SRCDIR"),
        os.getenv("TEST_WORKSPACE"),
        "tools/compliance",
        basename
    )
    with open(path, "rt", encoding="utf-8") as f:
        return f.read()


class PackagesUsedTest(unittest.TestCase):

    def test_found_key_licenses(self):
        raw_json = read_data_file("bazel_packages.json")
        content = json.loads(raw_json)
        found_top_level_license = False
        found_zlib = False
        for l in content["licenses"]:
            print(l["label"])  # for debugging the test
            if l["label"] == "//:license":
                found_top_level_license = True
            if l["label"] == "//third_party/bazel:license":
                found_top_level_license = True
            if l["label"] == "//third_party/zlib:license":
                found_zlib = True
        self.assertTrue(found_top_level_license)
        self.assertTrue(found_zlib)

    def test_found_remote_packages(self):
        if os.getenv("TEST_WORKSPACE") != "bazel":
            return
        raw_json = read_data_file("bazel_packages.json")
        content = json.loads(raw_json)
        self.assertIn(
            "@@remoteapis+//:build_bazel_remote_execution_v2_remote_execution_proto",
            content["packages"],
        )


if __name__ == "__main__":
    unittest.main()