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
from unittest.mock import patch

from click.testing import CliRunner
from rich.text import Text

from ai.chronon.repo.hub_runner import hub


def _plain(text: str) -> str:
    """Strip ANSI escape sequences using Rich's own parser."""
    return Text.from_ansi(text).plain

class TestHubRunner:
    """Test cases for hub_runner backfill command."""

    def _run_and_print(self, runner, command, args):
        """Helper method to run command and print output."""
        result = runner.invoke(command, args)

        # Print stdout
        if result.output:
            print(f"\n=== STDOUT ===\n{result.output}")

        # Print stderr if separate
        if hasattr(result, 'stderr') and result.stderr:
            print(f"\n=== STDERR ===\n{result.stderr}")

        # Print exception if any
        if result.exception:
            print(f"\n=== EXCEPTION ===\n{result.exception}")
            import traceback
            traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__)

        return result

    def test_hub_runner(self):
        """Test that hub command group can be invoked."""
        runner = CliRunner()
        result = self._run_and_print(runner, hub, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_backfill_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'backfill',
            online_join_conf,
            '--repo', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "backfill"
        assert json_payload['start'] == "2024-01-15"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_adhoc_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            online_join_conf,
            '--repo', canary,
            '--no-use-auth',
            '--start-ds', '2024-01-15',
            '--end-ds', '2024-02-15',
        ])

        # start-ds is not supported
        assert result.exit_code != 0
        result = self._run_and_print(runner, hub, [
            'run-adhoc',
            online_join_conf,
            '--repo', canary,
            '--no-use-auth',
            '--end-ds', '2024-02-15',
        ])
        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/workflow/v2/start")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['mode'] == "deploy"
        assert json_payload['end'] == "2024-02-15"
        assert json_payload['branch'] == "test-branch"

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    def test_schedule_end_to_end_post_request(
        self,
        mock_get_current_branch,
        mock_post,
        canary,
        online_join_conf,
    ):
        """Test end-to-end that the actual POST request contains the right date parameters."""
        # Mock get_current_branch to return a test branch name
        mock_get_current_branch.return_value = "test-branch"

        # Run backfill command
        runner = CliRunner(catch_exceptions=False)
        result = self._run_and_print(runner, hub, [
            'schedule',
            online_join_conf,
            '--repo', canary,
            '--no-use-auth',
        ])

        assert result.exit_code == 0

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called()
        call_args = mock_post.call_args

        # Check URL
        assert call_args[0][0].endswith("/schedule/v2/schedules")

        # Check JSON payload
        json_payload = call_args[1]['json']
        assert json_payload['confName'] == ".".join(online_join_conf.split("/")[-2:])
        assert json_payload['branch'] == "test-branch"
        assert json_payload['modeSchedules'] == {"BACKFILL": "@daily", "DEPLOY": "@daily"}

        # Check headers
        headers = call_args[1]['headers']
        assert headers['Content-Type'] == "application/json"

    @patch('requests.post')
    def test_cancel_end_to_end_post_request(
        self,
        mock_post,
        canary,
    ):
        """Test end-to-end that the cancel command makes the right API call."""
        # Mock the response from the cancel API
        mock_post.return_value.json.return_value = {
            "success": True,
            "message": "Workflow cancelled successfully"
        }
        mock_post.return_value.raise_for_status.return_value = None

        # Run cancel command
        runner = CliRunner()
        workflow_id = "test-workflow-123"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--repo', canary,
            '--no-use-auth',
            '--cloud', 'gcp',
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "Workflow cancelled" in plain_output
        assert workflow_id in plain_output

        # Verify the actual POST request was made with correct parameters
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check URL contains the workflow ID and cancel endpoint
        url = call_args[0][0]
        assert f"/workflow/v2/{workflow_id}/cancel" in url

    @patch('requests.post')
    def test_cancel_with_azure_and_customer_id(
        self,
        mock_post,
        canary,
    ):
        """Test cancel command with Azure cloud provider and customer ID."""
        # Mock the response from the cancel API
        mock_post.return_value.json.return_value = {
            "success": True,
            "message": "Workflow cancelled successfully"
        }
        mock_post.return_value.raise_for_status.return_value = None

        # Run cancel command with Azure and customer ID
        runner = CliRunner()
        workflow_id = "test-workflow-456"
        customer_id = "test-customer-123"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--repo', canary,
            '--no-use-auth',
            '--cloud', 'azure',
            '--customer-id', customer_id,
        ])

        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "Workflow cancelled" in plain_output
        assert workflow_id in plain_output

        # Verify the actual POST request was made
        mock_post.assert_called_once()
        call_args = mock_post.call_args

        # Check URL contains the workflow ID and cancel endpoint
        url = call_args[0][0]
        assert f"/workflow/v2/{workflow_id}/cancel" in url

    @patch('ai.chronon.repo.hub_runner.get_common_env_map')
    def test_cancel_with_azure_missing_customer_id(
        self,
        mock_get_common_env_map,
        canary,
    ):
        """Test cancel command fails when Azure is specified without customer ID."""
        # Mock get_common_env_map to return config without CUSTOMER_ID
        mock_get_common_env_map.return_value = {
            "HUB_URL": "http://localhost:3903",
            "FRONTEND_URL": "http://localhost:3000",
            # Intentionally not including CUSTOMER_ID
        }

        # Run cancel command with Azure but no customer ID
        runner = CliRunner()
        workflow_id = "test-workflow-789"
        result = self._run_and_print(runner, hub, [
            'cancel',
            workflow_id,
            '--repo', canary,
            '--no-use-auth',
            '--cloud', 'azure',
            # Intentionally not providing --customer-id
        ])

        # click.UsageError exits with code 2
        assert result.exit_code == 2
        assert "Customer ID is not set for Azure" in result.output

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_no_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are no compilation changes."""
        # Mock __compile to return no pending changes
        mock_compile.return_value = ({}, False, {"added": [], "changed": [], "deleted": []})

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--repo', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should succeed with exit code 0
        assert result.exit_code == 0
        plain_output = _plain(result.output)
        assert "No compilation changes detected" in plain_output

        # submit_schedule_all SHOULD be called since there are no changes
        mock_submit_schedule_all.assert_called_once()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_added_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are added configs."""
        # Create mock ConfigChange objects
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        added_change = ConfigChange(
            name="test_team.new_join",
            obj_type="Join",
            online=True,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [added_change], "changed": [], "deleted": []}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--repo', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Added: test_team.new_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_changed_configs(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are changed configs."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        changed_change = ConfigChange(
            name="test_team.existing_join",
            obj_type="Join",
            online=True,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [], "changed": [changed_change], "deleted": []}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--repo', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Changed: test_team.existing_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_deleted_configs(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are deleted configs."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        deleted_change = ConfigChange(
            name="test_team.old_join",
            obj_type="Join",
            online=False,
        )

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {"added": [], "changed": [], "deleted": [deleted_change]}
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--repo', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Deleted: test_team.old_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()

    @patch('ai.chronon.repo.hub_runner.submit_schedule_all')
    @patch('ai.chronon.click_helpers.__compile')
    def test_schedule_all_with_multiple_changes(
        self,
        mock_compile,
        mock_submit_schedule_all,
        canary,
    ):
        """Test schedule_all when there are multiple types of changes."""
        from ai.chronon.cli.compile.conf_validator import ConfigChange

        added_change = ConfigChange(name="test_team.new_join", obj_type="Join", online=True)
        changed_change = ConfigChange(name="test_team.existing_join", obj_type="Join", online=True)
        deleted_change = ConfigChange(name="test_team.old_join", obj_type="Join", online=False)

        # Mock __compile to return pending changes
        mock_compile.return_value = (
            {},
            False,
            {
                "added": [added_change],
                "changed": [changed_change],
                "deleted": [deleted_change]
            }
        )

        runner = CliRunner()
        result = self._run_and_print(runner, hub, [
            'schedule-all',
            '--repo', canary,
            '--cloud', 'gcp',
            '--no-use-auth',
        ])

        # Should fail with exit code 1
        assert result.exit_code == 1
        plain_output = _plain(result.output)
        assert "Compilation resulted in changes" in plain_output
        assert "Added: test_team.new_join" in plain_output
        assert "Changed: test_team.existing_join" in plain_output
        assert "Deleted: test_team.old_join" in plain_output

        # submit_schedule_all should NOT be called
        mock_submit_schedule_all.assert_not_called()


    @patch('ai.chronon.repo.hub_runner.get_schedule_modes')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.compute_and_upload_diffs')
    @patch('ai.chronon.repo.hub_runner.hub_uploader.build_local_repo_hashmap')
    @patch('ai.chronon.repo.hub_runner.get_current_branch')
    @patch('ai.chronon.repo.hub_runner.ZiplineHub')
    def test_schedule_all_skips_confs_with_none_str_schedules(
        self,
        mock_zipline_hub,
        mock_get_current_branch,
        mock_build_hashmap,
        mock_compute_diffs,
        mock_get_schedule_modes,
        canary,
    ):
        """Test that submit_schedule_all skips confs where both schedules are SCHEDULE_NONE_STR."""
        from ai.chronon.repo.hub_runner import (
            SCHEDULE_NONE_STR,
            ScheduleModes,
            submit_schedule_all,
        )
        from gen_thrift.api.ttypes import Conf

        mock_get_current_branch.return_value = "test-branch"
        mock_build_hashmap.return_value = {}

        # Mock compute_and_upload_diffs to return a conf without schedules
        conf_without_schedules = Conf(
            name="test_team.join_without_schedules",
            localPath="/path/to/conf",
            hash="hash1",
        )
        mock_compute_diffs.return_value = {
            "test_team.join_without_schedules": conf_without_schedules,
        }

        # Mock get_schedule_modes to return SCHEDULE_NONE_STR for both schedules
        mock_get_schedule_modes.return_value = ScheduleModes(
            offline_schedule=SCHEDULE_NONE_STR,
            online_schedule=SCHEDULE_NONE_STR
        )

        # Mock ZiplineHub instance
        mock_hub_instance = mock_zipline_hub.return_value

        # Call submit_schedule_all
        submit_schedule_all(
            repo=canary,
            cloud='gcp',
            customer_id=None,
            hub_url=None,
            use_auth=False
        )

        # Verify call_schedule_all_api was NOT called since all confs have no schedules
        mock_hub_instance.call_schedule_all_api.assert_not_called()
