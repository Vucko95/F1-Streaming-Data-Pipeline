# _Formula 1 Telemetry - Data Streaming - Analytics_

_This project is designed to collect, process, and visualize telemetry data from Formula 1 races using various technologies. The incoming UDP packets from the race are categorized into two distinct data streams: Persistent Data and Real-Time Data._ 

<div style="text-align:center;">
  <img src="./images/dashv1.jpg" alt="Home Screen">
</div>

_All F1 Simulation racing series can export race telemetry data in form of UDP Packets. General specification where followed from [***F1 Official UDP Packets***](https://answers.ea.com/t5/General-Discussion/F1-24-UDP-Specification/m-p/13745220/thread-id/2650/highlight/true)_

## Arhitecture Overview
Project leverages a set of individual services to collect, process, and visualize Formula 1 race telemetry data. The telemetry data is divided into two key categories:

1. ***Real-Time Data
Displayed on Grafana:*** This data is processed and visualized in real-time, allowing for immediate insights into race telemetry metrics, such as car speed, lap times, and driver positions.
2. ***Persistent Data
Stored in PostgreSQL:*** This data represents historical telemetry metrics collected during races. It is stored for long-term analysis and can be queried or visualized later using Apache Superset.


All telemetry packets from the game are captured by a central Python application listening on `localhost:20777`.

Immediate Real-Time Data: Data fragments such as car speed, fuel levels, and battery status are forwarded directly to a WebSocket service, where they are instantly displayed in Grafana, providing real-time updates.

Processed Telemetry Data: Other telemetry packets are sent to Apache NiFi, which routes them to designated ***Kafka topics***. From Kafka, the data is picked up by Spark for processing and then relayed to ***PostgreSQL*** for long-term storage and analysis.

Additionally, some telemetry packets, specifically those tied to the current driver's session, are stored directly in ***PostgreSQL*** to capture session-specific details.
