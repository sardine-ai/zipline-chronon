#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import pytest
from ai.chronon.repo.spark_catalog_confs import (
    Configuration,
    GlueConfiguration,
    BigQueryConfiguration,
    OpenCatalogConfiguration,
)


class TestConfiguration:
    def test_init_empty(self):
        conf = Configuration()
        assert len(conf) == 0
        assert conf._required == []

    def test_init_with_dict(self):
        conf = Configuration({"key1": "value1", "key2": "value2"})
        assert conf["key1"] == "value1"
        assert conf["key2"] == "value2"
        assert len(conf) == 2

    def test_init_with_required(self):
        conf = Configuration({"key1": "value1"}, required=["param1", "param2"])
        assert conf._required == ["param1", "param2"]

    def test_resolve_no_substitution(self):
        result = Configuration.resolve(
            {"key1": "value1", "key2": "value2"}
        )
        assert result == {"key1": "value1", "key2": "value2"}

    def test_resolve_with_substitution(self):
        result = Configuration.resolve(
            {"key1": "value-{param1}", "key2": "prefix-{param2}-suffix"},
            param1="foo",
            param2="bar"
        )
        assert result == {"key1": "value-foo", "key2": "prefix-bar-suffix"}

    def test_resolve_multiple_substitutions_same_key(self):
        result = Configuration.resolve(
            {"key1": "{param1}-{param2}-{param1}"},
            param1="x",
            param2="y"
        )
        assert result == {"key1": "x-y-x"}

    def test_call_with_no_required(self):
        conf = Configuration({"key1": "value1", "key2": "value2"})
        result = conf()
        assert isinstance(result, Configuration)
        assert result["key1"] == "value1"
        assert result["key2"] == "value2"

    def test_call_with_required_values_provided(self):
        conf = Configuration(
            {"key1": "value-{param1}", "key2": "value-{param2}"},
            required=["param1", "param2"]
        )
        result = conf(required_values={"param1": "foo", "param2": "bar"})
        assert result["key1"] == "value-foo"
        assert result["key2"] == "value-bar"

    def test_call_with_missing_required_values(self):
        conf = Configuration(
            {"key1": "value-{param1}"},
            required=["param1", "param2"]
        )
        with pytest.raises(ValueError, match="Missing required values: \\['param2'\\]"):
            conf(required_values={"param1": "foo"})

    def test_call_with_all_missing_required_values(self):
        conf = Configuration(
            {"key1": "value"},
            required=["param1", "param2"]
        )
        with pytest.raises(ValueError, match="Missing required values: \\['param1', 'param2'\\]"):
            conf()

    def test_call_with_extra_values(self):
        conf = Configuration(
            {"key1": "value-{param1}"},
            required=["param1"]
        )
        result = conf(required_values={"param1": "foo", "param2": "bar"})
        assert result["key1"] == "value-foo"

    def test_call_none_required_values(self):
        conf = Configuration({"key1": "value1"}, required=["param1"])
        with pytest.raises(ValueError, match="Missing required values: \\['param1'\\]"):
            conf(None)

    def test_configuration_with_template_substitution(self):
        # Test that Configuration can handle templates that reference required values
        conf = Configuration(
            {
                "base.url": "{host}:{port}",
                "base.path": "{host}/data/{environment}",
                "base.connection": "jdbc:{host}:{port}/{database}"
            },
            required=["host", "port", "environment", "database"]
        )
        result = conf(required_values={
            "host": "localhost",
            "port": "5432",
            "environment": "prod",
            "database": "mydb"
        })
        assert result["base.url"] == "localhost:5432"
        assert result["base.path"] == "localhost/data/prod"
        assert result["base.connection"] == "jdbc:localhost:5432/mydb"

