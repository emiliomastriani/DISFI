DISFI (Distributed Infrastructure Stress and Fault Injector) is a configurable fault-injection framework designed to induce controlled stress conditions and resource exhaustion in distributed, 
stateful systems. The tool generates sustained background telemetry traffic while selectively injecting pathological workload patterns, including connection storms, read/write amplification, 
intentional connection leaks, and CPU saturation. DISFI is specifically designed to trigger internal failure modes such as thread pool saturation, file descriptor exhaustion, request timeouts, 
and transient unavailability. All injected faults and execution parameters are explicitly logged, enabling precise temporal alignment between injected stress conditions and observed system anomalies. 
The framework supports reproducible experimental campaigns for anomaly detection, predictive maintenance, and reliability analysis in distributed databases and streaming platforms.

It is intended for:
- Testing predictive anomaly detection
- Producing labeled anomalies for ML pipelines
- Stress testing observability dashboards (Prometheus, Grafana)
- Validation of alerting rules

Please,
 1. Refer to the User Manual for more information (https://github.com/emiliomastriani/DISFI/blob/main/DISFI_V01_UserManual.pdf)
 2. Cite the tool with "Mastriani, E., Costa A.  DISFI (Distributed Infrastructure Stress and Fault Injector), https://github.com/emiliomastriani/DISFI"
