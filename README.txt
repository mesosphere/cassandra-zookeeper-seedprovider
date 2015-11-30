Project: cassandra-zookeeper-seedprovider

Authors: Kenneth Owens (kenneth@mesosphere.io)

Version: 1.0

Description: The project contains the io.mesosphere.mesos.frameworks.cassandra.ZooKeeperSeedProvider which implements
the org.apache.cassandra.locator.SeedProvider interface and provides a Cassandra server with the current set of cluster
seeds from a ZooKeeper ensemble.

Building the Project: This project uses Maven to compile and package its distribution. After checking out the project,
run mvn package in the project's root directory (the directory containing the pom.xml) to produce a gzipped tape archive
named cassandara-zookeeper-seedprovider.tar.gz in the projects target directory.

Usage: An external source must create a ZNode containing the ip addresses of the seed nodes for the Cassandra ring as a
comma separated list (e.g host1,host2,host3) prior to launching a Cassandra server configured with ZooKeeperSeedProvider.
Servers that will use the provider must then have the dependencies contained in the
cassandra-zookeeper-seedprovider.tar.gz copied into the libs directory of the Cassandra installation for which the
ZooKeeperSeedProvider will be configured. The cassandra launch scripts will automatically have these jars added
to the runtime classpath of the server. Next the cassandra.yaml (this file is in the conf directory of the Cassandra
installation) must be updated to contain the necessary configuration parameters (See the example below).

seed_provider:
    - class_name: io.mesosphere.mesos.frameworks.cassandra.ZooKeeperSeedProvider
      parameters:
          - zookeeper_server_addresses : "host:port,host2:port, ... , hostn:port"
          - zookeeper_seeds_path : "/cassandra/seeds"
          - session_timout_ms : 10000
          - connection_timeout_ms : 10000
          - operation_retry_timemout_ms : -1

zookeeper_server_addresses (mandatory) - A comma separated list of host:port pairs that indicates the servers for the
ZooKeeper ensemble used to store the Cassandra ring's seed nodes.

zookeeper_seeds_path (mandatory) - A string indicating the path of the ZNode where the Cassandra ring's seed nodes will
be stored.

session_timeout_ms (optional default 10000) - A strictly non-negative integer indicating the length of the ZooKeeper
session timeout in milliseconds.

connection_timeout_ms (optional default 10000) - A strictly non-negative integer indicating the length of the ZooKeeper
connection timeout in milliseconds.

operation_retry_timeout_ms (optional default -1) - An integer indicating the operation timeout for ZooKeeper operations
in ms. If this value is negative operations will be retried forever.

Notes: The ZooKeeperSeedProvider will cache the last successful result from the ZooKeeper ensemble. Therefore, in the
presence of a temporary ZooKeeper failure, if operation_retry_timeout is configured, the Cassandra ring will still be
able to retrieve its seed nodes.




