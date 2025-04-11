"""
Tests for the parse_teams module.
"""

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

from ai.chronon.api.ttypes import GroupBy, Join, JoinPart, LabelParts, MetaData, Team
from ai.chronon.cli.compile import parse_teams


def test_update_metadata_with_existing_output_namespace():
    """Test that update_metadata doesn't override existing outputNamespace."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }
    
    # Test with existing outputNamespace
    existing_namespace = "existing_namespace"
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
        outputNamespace=existing_namespace
    ))
    
    # Call the function
    parse_teams.update_metadata(obj, team_dict)
    
    # Verify outputNamespace wasn't changed
    assert obj.metaData.outputNamespace == existing_namespace


def test_update_metadata_without_existing_output_namespace():
    """Test that update_metadata sets outputNamespace when not already set."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }
    
    # Test without existing outputNamespace
    obj = GroupBy(metaData=MetaData(
        team=team_name,
        name="test.group_by.name",
    ))
    
    # Call the function
    parse_teams.update_metadata(obj, team_dict)
    
    # Verify outputNamespace was set from team
    assert obj.metaData.outputNamespace == "team_namespace"


def test_update_metadata_preserves_join_part_namespace():
    """Test that update_metadata preserves outputNamespace in join parts."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }
    
    # Create a join with join parts that have existing outputNamespace
    join_part_gb = GroupBy(metaData=MetaData(outputNamespace="existing_jp_namespace"))
    join_part = JoinPart(groupBy=join_part_gb)
    
    # Create a join with label parts that have existing outputNamespace
    label_part_gb = GroupBy(metaData=MetaData(outputNamespace="existing_label_namespace"))
    label_parts = LabelParts(labels=[JoinPart(groupBy=label_part_gb)])
    
    # Create the join object
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part],
        labelParts=label_parts
    )
    
    # Call the function
    parse_teams.update_metadata(join, team_dict)
    
    # Verify outputNamespace values were preserved
    assert join.metaData.outputNamespace == "join_namespace"
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "existing_jp_namespace"
    assert join.labelParts.labels[0].groupBy.metaData.outputNamespace == "existing_label_namespace"


def test_update_metadata_sets_missing_join_part_namespace():
    """Test that update_metadata sets outputNamespace for join parts when not set."""
    # Setup
    team_name = "test_team"
    team_dict = {
        "default": Team(outputNamespace="default_namespace"),
        team_name: Team(outputNamespace="team_namespace"),
    }
    
    # Create a join with join parts that don't have outputNamespace
    join_part_gb = GroupBy(metaData=MetaData())
    join_part = JoinPart(groupBy=join_part_gb)
    
    # Create a join with label parts that don't have outputNamespace
    label_part_gb = GroupBy(metaData=MetaData())
    label_parts = LabelParts(labels=[JoinPart(groupBy=label_part_gb)])
    
    # Create the join object
    join = Join(
        metaData=MetaData(
            team=team_name,
            name="test.join.name",
            outputNamespace="join_namespace"
        ),
        joinParts=[join_part],
        labelParts=label_parts
    )
    
    # Call the function
    parse_teams.update_metadata(join, team_dict)
    
    # Verify outputNamespace values were set correctly
    assert join.metaData.outputNamespace == "join_namespace"
    assert join.joinParts[0].groupBy.metaData.outputNamespace == "join_namespace"
    assert join.labelParts.labels[0].groupBy.metaData.outputNamespace == "join_namespace"