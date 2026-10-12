'''Configuration of environment variables for application'''

import os
from dotenv import load_dotenv
import subprocess

def get_windows_host_ip():
    '''
    Retrieve ipaddress of local machine via cmd line 
    '''
    result = subprocess.run(
        ["ip", "route", "show", "default"],
        capture_output=True, text=True
    )
    return result.stdout.split()[2]

load_dotenv(override=True)  # .env wins over shell exports, matching the MCP server subprocess (which gets a minimal env)

MASTER_HOST       = os.getenv("SPARK_MASTER_HOST", "")
WORKER_HOSTS      = os.getenv("SPARK_WORKER_HOSTS", "").split(",")
ADDITIONAL_HOSTS  = os.getenv("ADDITIONAL_HOSTS", "").split(",")
KAFKA_HOST        = os.getenv("KAFKA_HOST", "")  
SSH_USER          = os.getenv("SSH_USER", "")
SSH_KEY_PATH      = os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa")
PROMETHEUS_URL    = f"http://{get_windows_host_ip()}:9090"
SPARK_HOME        = os.getenv("SPARK_HOME", "/opt/spark")
CONDA_PATH        = os.getenv("CONDA_PATH", "")
SPARK_JOB_DIR     = os.getenv("SPARK_JOB_DIR", "")
NANO_GPU_HOST     = os.getenv("NANO_GPU_HOST","")
MLFLOW_SSH_HOST   = os.getenv("MLFLOW_SSH_HOST", MASTER_HOST)  # host running the loopback-only MLflow server
_DATASTREAM_PID_FILE = "/tmp/datastream.pid"
_DATASTREAM_LOG_FILE = "/tmp/datastream.log"