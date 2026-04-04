import pytest

from cluster_management_MCP.utils.cli_tools import copy_file_from_remote_host


class TestCliTools:

    def test_file_download_return_dict(self):
        remote_file = "~/gyroscope.py"
        target_host = "192.168.1.250"
        payload = copy_file_from_remote_host(remote_file, target_host)
        print(payload)
        assert isinstance(payload, dict)
    
    def test_local_path_not_present(self):
        remote_file = "~/gyroscope.py"
        target_host = "192.168.1.250"
        destination = "/mnt/c/Users/Fake_User/"
        payload = copy_file_from_remote_host(remote_file, target_host, destination_path=destination)
        assert payload.get("status") == "failure"
        assert f"Destination path {destination} on PC does not exist"
        
    def test_file_download_successful(self):
        remote_file = "~/gyroscope.py"
        target_host = "192.168.1.250"
        payload = copy_file_from_remote_host(remote_file, target_host)

        assert payload.get("status") == "success"
        assert "File successfully downloaded to" in payload.get("message")
        
    def test_file_download_node_unreachable(self):
        remote_file = "~/gyroscope.py"
        target_host = "123.238.1.620"
        payload = copy_file_from_remote_host(remote_file, target_host)

        assert payload.get("status") == "failure"
        assert payload.get("message") == "Host is not reachable"