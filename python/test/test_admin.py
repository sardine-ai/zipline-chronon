import base64
import json
import os
import re
from unittest.mock import MagicMock, patch

import pytest

from click.testing import CliRunner

from ai.chronon.repo.admin import (
    _authenticate_docker_hub,
    _base_domain,
    _creds_from_helper,
    _get_docker_credentials,
    _parse_registry,
    _should_update_latest,
    _upload_jars_to_store,
    admin,
)
from ai.chronon.repo.registry_client import DOCKER_HUB_REGISTRY


def strip_ansi(text):
    ansi_escape = re.compile(r'\x1b\[[0-9;]*[mGJKHF]')
    return ansi_escape.sub('', text)

# --- _parse_registry ---


class TestParseRegistry:
    def test_gcloud_style_with_path(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/canary-443022/canary-images")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "canary-443022/canary-images"

    def test_oci_style_hostname_only(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == ""

    def test_with_https_scheme(self):
        host, prefix = _parse_registry("https://us-central1-docker.pkg.dev/project/repo")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_ecr(self):
        host, prefix = _parse_registry("123456789.dkr.ecr.us-east-1.amazonaws.com")
        assert host == "123456789.dkr.ecr.us-east-1.amazonaws.com"
        assert prefix == ""

    def test_trailing_slash_stripped(self):
        host, prefix = _parse_registry("us-central1-docker.pkg.dev/project/repo/")
        assert host == "us-central1-docker.pkg.dev"
        assert prefix == "project/repo"

    def test_scheme_only_hostname(self):
        host, prefix = _parse_registry("https://ghcr.io")
        assert host == "ghcr.io"
        assert prefix == ""


# --- _base_domain ---


class TestBaseDomain:
    def test_full_url_with_path(self):
        assert _base_domain("https://index.docker.io/v1/") == "docker.io"

    def test_bare_hostname(self):
        assert _base_domain("registry-1.docker.io") == "docker.io"

    def test_subdomain_hostname(self):
        assert _base_domain("index.docker.io") == "docker.io"

    def test_gcr_artifact_registry(self):
        assert _base_domain("us-docker.pkg.dev") == "pkg.dev"

    def test_ecr(self):
        assert _base_domain("123456789.dkr.ecr.us-east-1.amazonaws.com") == "amazonaws.com"

    def test_ghcr(self):
        assert _base_domain("ghcr.io") == "ghcr.io"

    def test_single_label(self):
        assert _base_domain("localhost") == "localhost"

    def test_docker_hub_entries_match_each_other(self):
        """The canonical docker login URL and the registry API hostname should resolve to the same base domain."""
        assert _base_domain("https://index.docker.io/v1/") == _base_domain("registry-1.docker.io")
        assert _base_domain("https://index.docker.io/v1/") == _base_domain(DOCKER_HUB_REGISTRY)


# --- _get_docker_credentials ---


class TestGetDockerCredentials:
    def test_no_config_file(self, tmp_path):
        with patch.dict(os.environ, {"HOME": str(tmp_path)}):
            with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
                assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_inline_auths(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("registry-1.docker.io")
            assert username == "myuser"
            assert password == "mypass"

    def test_inline_auths_no_match(self, tmp_path):
        auth_value = base64.b64encode(b"myuser:mypass").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("ghcr.io") == (None, None)

    def test_cred_helpers(self, tmp_path):
        config = {
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "helperuser"
                assert password == "helperpass"
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("storeuser", "storepass")) as mock:
                username, password = _get_docker_credentials("registry-1.docker.io")
                assert username == "storeuser"
                assert password == "storepass"
                mock.assert_called_once_with("desktop", "https://index.docker.io/v1/")

    def test_cred_helpers_takes_priority_over_creds_store(self, tmp_path):
        config = {
            "credsStore": "desktop",
            "credHelpers": {
                "https://index.docker.io/v1/": "osxkeychain"
            },
            "auths": {
                "https://index.docker.io/v1/": {}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            with patch("ai.chronon.repo.admin._creds_from_helper", return_value=("helperuser", "helperpass")) as mock:
                _get_docker_credentials("registry-1.docker.io")
                mock.assert_called_once_with("osxkeychain", "https://index.docker.io/v1/")

    def test_malformed_base64_skipped(self, tmp_path):
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": "not-valid-base64!!!"}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_missing_colon_skipped(self, tmp_path):
        auth_value = base64.b64encode(b"nocolonhere").decode()
        config = {
            "auths": {
                "https://index.docker.io/v1/": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            assert _get_docker_credentials("registry-1.docker.io") == (None, None)

    def test_non_docker_hub_registry(self, tmp_path):
        auth_value = base64.b64encode(b"gcruser:gcrpass").decode()
        config = {
            "auths": {
                "us-docker.pkg.dev": {"auth": auth_value}
            }
        }
        docker_dir = tmp_path / ".docker"
        docker_dir.mkdir()
        (docker_dir / "config.json").write_text(json.dumps(config))

        with patch("ai.chronon.repo.admin.os.path.expanduser", return_value=str(tmp_path)):
            username, password = _get_docker_credentials("us-docker.pkg.dev")
            assert username == "gcruser"
            assert password == "gcrpass"


# --- _creds_from_helper ---


class TestCredsFromHelper:
    def test_successful_helper_call(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"Username": "user1", "Secret": "secret1"})

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result) as mock_run:
            username, password = _creds_from_helper("osxkeychain", "https://index.docker.io/v1/")
            assert username == "user1"
            assert password == "secret1"
            mock_run.assert_called_once_with(
                ["docker-credential-osxkeychain", "get"],
                input="https://index.docker.io/v1/",
                capture_output=True,
                text=True,
            )

    def test_helper_not_found(self):
        with patch("ai.chronon.repo.admin.subprocess.run", side_effect=FileNotFoundError):
            assert _creds_from_helper("missing", "https://index.docker.io/v1/") == (None, None)

    def test_helper_nonzero_exit(self):
        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch("ai.chronon.repo.admin.subprocess.run", return_value=mock_result):
            assert _creds_from_helper("osxkeychain", "https://index.docker.io/v1/") == (None, None)


# --- _authenticate_docker_hub ---


class TestAuthenticateDockerHub:
    def test_with_api_token(self):
        client = MagicMock()
        _authenticate_docker_hub(client, "my-token")
        client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")

    def test_with_local_credentials(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")):
            _authenticate_docker_hub(client, None)
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="localuser", password="localpass")

    def test_no_credentials_raises(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=(None, None)):
            with pytest.raises(Exception, match="Docker Hub credentials not found"):
                _authenticate_docker_hub(client, None)

    def test_api_token_takes_priority(self):
        client = MagicMock()
        with patch("ai.chronon.repo.admin._get_docker_credentials", return_value=("localuser", "localpass")) as mock:
            _authenticate_docker_hub(client, "my-token")
            mock.assert_not_called()
            client.authenticate.assert_called_once_with(DOCKER_HUB_REGISTRY, username="ziplineai", password="my-token")


# --- install release resolution ---


class TestInstallReleaseResolution:
    """Tests for the default --release resolution logic in the install command."""

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_defaults_to_package_version(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local"])
        assert result.exit_code == 0
        assert "Using release" in result.output and " 1.0.15 " in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.15"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="unknown")
    def test_falls_back_to_latest_when_package_not_installed(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local"])
        assert result.exit_code == 0
        assert "Using release " in result.output and " latest " in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "latest"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_explicit_release_matching_version_no_prompt(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.15"])
        assert result.exit_code == 0
        assert "does not match" not in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.15"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_mismatched_release_confirmed_proceeds(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"], input="y\n")
        assert result.exit_code == 0
        assert "does not match installed zipline cli version 1.0.15" in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.14"

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="1.0.15")
    def test_mismatched_release_declined_aborts(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"], input="n\n")
        assert result.exit_code == 1
        assert "does not match installed zipline cli version 1.0.15" in result.output
        mock_load.assert_not_called()

    @patch("ai.chronon.repo.admin._check_docker_available")
    @patch("ai.chronon.repo.admin._load_from_docker_hub")
    @patch("ai.chronon.repo.admin.get_package_version", return_value="unknown")
    def test_mismatched_release_skips_prompt_when_version_unknown(self, mock_version, mock_load, mock_docker):
        mock_load.return_value = []
        runner = CliRunner()
        result = runner.invoke(admin, ["install", "gcp", "--registry", "local", "--release", "1.0.14"])
        assert result.exit_code == 0
        assert "does not match" not in result.output
        mock_load.assert_called_once()
        assert mock_load.call_args[0][2] == "1.0.14"


# --- _should_update_latest ---


class TestShouldUpdateLatest:
    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value=None)
    def test_no_existing_marker_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True
        mock_read.assert_called_once_with("gs://bucket/store/release/latest/VERSION")

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_newer_version_returns_true(self, mock_read):
        assert _should_update_latest("2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_same_version_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="2.0.0")
    def test_older_version_returns_false(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is False

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="1.0.0")
    def test_v_prefix_stripped_from_release(self, mock_read):
        assert _should_update_latest("v2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="v1.0.0\n")
    def test_v_prefix_and_whitespace_stripped_from_marker(self, mock_read):
        assert _should_update_latest("2.0.0", "gs://bucket/store") is True

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value="not-a-version")
    def test_invalid_existing_marker_returns_true(self, mock_read):
        assert _should_update_latest("1.0.0", "gs://bucket/store") is True

    def test_invalid_release_version_returns_false(self):
        assert _should_update_latest("not-a-version", "gs://bucket/store") is False

    @patch("ai.chronon.repo.admin.read_from_blob_store", return_value=None)
    def test_trailing_slash_stripped_from_store(self, mock_read):
        _should_update_latest("1.0.0", "s3://bucket/store/")
        mock_read.assert_called_once_with("s3://bucket/store/release/latest/VERSION")


# --- _upload_jars_to_store ---


class TestUploadJarsToStore:
    def test_nonexistent_dir_returns_empty(self, tmp_path):
        results = _upload_jars_to_store(str(tmp_path / "nope"), "1.0.0", "gs://b/store")
        assert results == []

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_uploads_versioned_and_latest(self, mock_upload, mock_latest, tmp_path):
        jar = tmp_path / "foo.jar"
        jar.write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 1
        assert results[0][3] == "ok"
        # versioned + latest jar + VERSION marker = 3 uploads
        assert mock_upload.call_count == 3
        paths = [call.args[1] for call in mock_upload.call_args_list]
        assert "gs://b/store/release/1.0.0/jars/foo.jar" in paths
        assert "gs://b/store/release/latest/jars/foo.jar" in paths
        assert "gs://b/store/release/latest/VERSION" in paths

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=False)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_skips_latest_when_older(self, mock_upload, mock_latest, tmp_path):
        jar = tmp_path / "foo.jar"
        jar.write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "0.9.0", "gs://b/store")
        assert len(results) == 1
        assert results[0][3] == "ok"
        # Only versioned upload, no latest
        assert mock_upload.call_count == 1
        assert mock_upload.call_args.args[1] == "gs://b/store/release/0.9.0/jars/foo.jar"

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_multiple_jars(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "a.jar").write_text("a")
        (tmp_path / "b.jar").write_text("b")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 2
        assert all(r[3] == "ok" for r in results)
        # 2 versioned + 2 latest + 1 VERSION marker = 5 uploads
        assert mock_upload.call_count == 5

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store", side_effect=Exception("upload failed"))
    def test_upload_failure_recorded(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "foo.jar").write_text("data")
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert len(results) == 1
        assert "FAILED" in results[0][3]

    @patch("ai.chronon.repo.admin._should_update_latest", return_value=True)
    @patch("ai.chronon.repo.admin.upload_to_blob_store")
    def test_version_marker_not_written_on_failure(self, mock_upload, mock_latest, tmp_path):
        (tmp_path / "foo.jar").write_text("data")
        # First call (versioned) succeeds, second call (latest jar) raises
        mock_upload.side_effect = [None, Exception("fail")]
        results = _upload_jars_to_store(str(tmp_path), "1.0.0", "gs://b/store")
        assert "FAILED" in results[0][3]
        # No VERSION marker write attempted since results contain a failure
        paths = [call.args[1] for call in mock_upload.call_args_list]
        assert "gs://b/store/release/latest/VERSION" not in paths
