
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

import os

import pytest


@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def teams_json(rootdir):
    return os.path.join(rootdir, 'sample/teams.json')


@pytest.fixture
def repo(rootdir):
    return os.path.join(rootdir, 'sample/')


@pytest.fixture
def test_online_group_by(repo):
    return os.path.join(repo, 'production/group_bys/sample_team/event_sample_group_by.v1')


@pytest.fixture
def sleepless():
    def justpass(seconds):
        pass
    return justpass
