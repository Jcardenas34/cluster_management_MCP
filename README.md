# Cluster Management MCP
An MCP (Model Context Protocol) server that exposes a self built and managed Spark cluster, as a set of tools callable by an LLM. A local instance of Qwen2.5:1.5b running via Ollama interprets natural language prompts and submits tool calls for Spark job submission, and cluster health checks over a Wifi network.

This project is being developed as part of a broader infrastructure build for [Fraud Detection](https://github.com/Jcardenas34/fraud_detection/tree/main), and [Risk Simulation Jobs](https://github.com/Jcardenas34/risk_analysis_in_risk).

# Motivation
Cloud infrastructure abstracts away hardware constraints, network topologies and resource allocation. This tool was built to orchestrate a realistic end-to-end ML workload locally, exposing the deliberate choices needed to construct a functioning cluster, such as which node runs as the Spark master, which can serve as a Kafka node, and how jobs can be submitted while keeping the execution environment stable. This tool demonstrates how LLMs can be used to monitor these metrics,  and execute jobs through natural language.

# Spark Cluster Specifications
## Hardware

| Role | Hardware | OS |
|---|---|---|
| Head node / Spark master | Lenovo PC | Ubuntu 24.04 |
| Spark workers (×3) | Raspberry Pi 3B+ | Ubuntu |
| Kafka broker | Raspberry Pi 3 | Ubuntu |
| Edge inference host | NVIDIA Jetson Nano | Ubuntu |

The cluster is self-assembled and self-managed. Node configuration is 
handled via `spark-env.sh` on the head node.


# Architecture Specifications
```
User (natural language prompt)
        │
        ▼
Qwen2.5:1.5b via Ollama  ◄──►  MCP Server (tool definitions)
                                      │
                      ┌───────────────┼───────────────┐
                      ▼               ▼               ▼
               Spark REST API   Prometheus API   SSH / shell
               (job submission) (health metrics) (start/stop)
```
At present, a local instance Qwen2.5:1.5b is used to host Ollama, with a simultaneous run of the MCP server which submits jobs to the Spark headnode through a local Wifi network. Port forwarding for remote job submission is also configured. The MCP server currently supports health checks on the cluster like CPU load per node, memory usage, and number of nodes retrieved via API calls to a Prometheus server running locally.  

Cluster start-up and tear-down are supported, continguent on proper configuration of the available nodes is specified in the spark-env.sh file on the head node. 


# Example: LLM driven job submission
Ex. "Please simulate a Risk battle with 5 attackers and 3 defenders across 1000 trials."

This request is interpreted by Qwen2.5 as a tool call to submit the ["risk roll simulation"](https://github.com/Jcardenas34/risk_analysis_in_risk) job to the cluster with the appropriate input parameters, and stored on the master node for analysis. This job simulates the win probabilities of battle scenarios with different number of attackers and defenders in the game "Risk".     


# Future work
The job results are not currently returned back to the LLM. But instead written on disk for later analysis, and plotting. Creating functionality for live interpretation of results, after the job has finished executing is the next step in this work. 

## Stack

- **Orchestration:** Apache Spark (standalone cluster mode)
- **Messaging:** Apache Kafka
- **LLM runtime:** Ollama (Qwen2.5:1.5b)
- **Tool protocol:** MCP (Model Context Protocol)
- **Monitoring:** Prometheus, Grafana
- **Edge inference:** FastAPI on NVIDIA Jetson Nano
- **Networking:** Local Wi-Fi, port forwarding for remote access
