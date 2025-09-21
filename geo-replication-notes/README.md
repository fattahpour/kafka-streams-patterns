# Geo Replication Notes

This module collects documentation and configuration samples for running Apache Kafka across
multiple regions with MirrorMaker 2.0.

## Strategy Overview

* **Active/Active Mirroring** – Both regions run Kafka clusters and mirror each other’s topic
  subsets using MirrorMaker 2. Consumer groups remain region-local to avoid cross-region lag.
* **Failover** – Producers publish to their closest region. In the event of a regional outage the
  corresponding topics can be promoted by pointing producers to the surviving cluster and enabling
  the opposite replication flow.
* **Topic Selection** – MirrorMaker `replication.policy.class` is set to
  `org.apache.kafka.connect.mirror.DefaultReplicationPolicy`, so mirrored topics receive the
  `{sourceCluster}.{topic}` prefix. Applications should subscribe explicitly to the appropriate
  prefixed topics.
* **Offset Syncs** – Enable the offset sync topic to allow consumers to recover to the correct
  position when failing over between regions.

## MirrorMaker 2 Configuration

A sample configuration file is available in
`src/main/resources/mirrormaker2-multiregion.properties`. Adjust bootstrap servers, ACLs, and topic
lists to match your environments.

## Building

This module contains documentation only. The Maven build simply packages the resources for
consistency with the rest of the repository.
