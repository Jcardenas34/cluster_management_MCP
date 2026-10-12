import pytest
from cluster_management_MCP.utils.fraud_pipeline_tools import stop_fraud_inference, start_fraud_inference


class TestFraudServers():

    def test_stop_fraud_server(self):
        """Stops fraud servier on jetson nano"""
        payload = stop_fraud_inference()
        assert isinstance(payload,dict)

    def test_start_fraud_inference(self):
        """Starts fraud servier on jetson nano"""
        payload = start_fraud_inference()
        assert isinstance(payload,dict)