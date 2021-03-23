# alarm-state-processor
Kafka Streams application to process alarm state in JAWS
# alarm-state-processor [![Java CI with Gradle](https://github.com/JeffersonLab/alarm-state-processor/workflows/Java%20CI%20with%20Gradle/badge.svg)](https://github.com/JeffersonLab/alarm-state-processor/actions?query=workflow%3A%22Java+CI+with+Gradle%22) [![Docker Image Version (latest semver)](https://img.shields.io/docker/v/slominskir/alarm-state-processor?sort=semver&label=DockerHub)](https://hub.docker.com/r/slominskir/alarm-state-processor)
A [Kafka Streams](https://kafka.apache.org/documentation/streams/) application to compute alarm state given active alarms, registered alarms, and overridden alarms in [JAWS](https://github.com/JeffersonLab/jaws).

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/alarms-filter#quick-start-with-compose)
 - [Build](https://github.com/JeffersonLab/alarms-filter#build)
 - [Configure](https://github.com/JeffersonLab/alarms-filter#configure)
 - [Deploy](https://github.com/JeffersonLab/alarms-filter#deploy)
 - [Docker](https://github.com/JeffersonLab/alarms-filter#docker)
 ---

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/alarm-state-processor
cd alarm-state-processor
```
2. Launch Docker
```
docker-compose up
```
3. Set an override
```
docker exec jaws /scripts/client/set-override.py --override Disabled alarm1
```
5. Verify state
```
docker exec state  /opt/alarm-state-processor/bin/list-state.sh
```
## Build
This [Java 11](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/alarm-state-processor
cd alarm-state-processor
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

**Note**: When developing the app you can mount the build artifact into the container by substituting the `docker-compose up` command with:
```
docker-compose -f docker-compose.yml -f docker-compose-dev.yml up
```

## Configure
Environment Variables

| Name | Description |
|---|---|
| BOOTSTRAP_SERVERS | Comma-separated list of host and port pairs pointing to a Kafka server to bootstrap the client connection to a Kafka Cluser; example: `kafka:9092` |
| SCHEMA_REGISTRY | URL to Confluent Schema Registry; example: `http://registry:8081` |

## Deploy
The Kafka Streams app is a regular Java application, and start scripts are created and dependencies collected by the Gradle distribution targets:

```
gradlew assembleDist
```

[Releases](https://github.com/JeffersonLab/alarms-filter/releases)

Launch with:

UNIX:
```
bin/alarm-state-processor
```
Windows:
```
bin/alarm-state-processor.bat
```

## Docker
```
docker pull slominskir/alarm-state-processor
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/alarm-state-processor)

