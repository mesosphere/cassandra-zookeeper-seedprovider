/**
 * Copyright (C) 2015 Mesosphere, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ZookeeperSeedProvider implements a seed provider for Cassandra that uses
 * reads the set of Cassandra seed nodes from a Zookeeper node. The path of the
 * zookeeper node is /cassandara/seeds, and the node must be created, and
 * contain least one seed prior to launching the Cassandra instance. The
 * following configuration should be added to the cassandra.yaml in order to
 * use the provider.
 * <p>
 * seed_provider:
 * - class_name: io.mesosphere.mesos.frameworks.cassandra.ZookeeperSeedProvider
 * parameters:
 * - zookeeper_server_addresses : "host:port,host2:port, ... , hostn:port"
 * - zookeeper_seeds_path : "/cassandra/seeds"
 * -session_timout_ms : 10000
 * -connection_timeout_ms : 5000
 * -operation_retry_timemout_ms : -1
 * <p>
 * zookeeper_server_addresses is a comma sparated list of address:port pairs
 * that indicates the locations of the servers in the ZooKeeper ensemble
 * hosting the cassandra seeds. It is mandatory and failure to configure this
 * will cause a runtime error on server startup.
 * <p>
 * zookeeper_seeds_path is the fully qualified path to the znode containing the
 * Cassandra seed nodes. It is mandatory and failure to configure this will
 * cause a runtime error on server startup.
 * <p>
 * session_timeout_ms controls the session timeout between the SeedProvider and
 * the zookeeper servers. It is optional and will default to 6000 ms.
 * <p>
 * connection_timeout_ms controls the connection timeout between the
 * SeedProvider and the zookeeper servers (The amount of time the
 * SeedProvider will wait to connect prior to failing). It is optional and
 * will default to 6000 ms.
 * <p>
 * operation_retry_timeout_ms controls the amount of time the SeedProvider
 * will retry an operation before failing. If set to a value less than zero
 * all operations will remain pending until a zookeeper ensemble leader can
 * be written to. It defaults to -1 which is the least safe in terms of
 * protecting memory limits but the most robust setting.
 *
 * @author Kenneth Owens (kenneth@mesoshere.io)
 * @version 1.0
 */
public class ZooKeeperSeedProvider implements SeedProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (ZooKeeperSeedProvider.class);


    private static final Joiner ON_COMMA = Joiner.on(',');

    private final ZooKeeperSeedProviderConfig config;

    private final CuratorFramework client;

    private List<InetAddress> cached = Collections.emptyList();

    /**
     * @param parameters A dictionary containing the configuration properties
     *                   to parse. It must contain zookeeper_server_address,
     *                   a comma separated list of host:port pairs that
     *                   indicates the servers in the ZooKeeper ensemble, and
     *                   zookeeper_seeds_path, a fully qualified path to the
     *                   ZNode containing the Cassandra cluster seeds. It may
     *                   contain session_timeout_ms, a strictly positive
     *                   integer that indicates the zookeeper session timeout
     *                   in milliseconds, connection_timeout_ms, a strictly
     *                   positive integer indicating the timeout for
     *                   connection to a ZooKeeper server in ms, and
     *                   operation_timeout, which, if positive, indicates the
     *                   amount of time an operations should be retried in ms.
     * @throws RuntimeException contain any exception encountered due to
     *                          misconfiguration or a failure to establish a connection with ZooKeeper.
     */
    public ZooKeeperSeedProvider(Map<String, String> parameters) {

        try {
            config = ZooKeeperSeedProviderConfig.from(parameters);

            client = CuratorFrameworkFactory.newClient(
                    config.getZkServers(),
                    config.getSessionTimeoutMs(),
                    config.getConnectionTimeoutMs(),
                    (config.getOperationTimeoutMs() > 0) ?
                            new RetryUntilElapsed(
                                    config.getOperationTimeoutMs(),
                                    config.getOperationTimeoutMs() / 4) :
                            new RetryForever(250));

            client.start();

        } catch (Exception e) {

            LOGGER.error("invalid configuration", e);

            throw new RuntimeException("invalid configuration", e);
        }

        ///This may throw a runtime exception and should hopefully kill the
        ///server


    }

    private static String asString(byte[] bytes) {

        if (bytes == null) {
            return "";
        } else {
            return new String(bytes);
        }
    }

    private static List<InetAddress> parseAddresses(String string) {

        if (string == null || string.isEmpty()) {

            return Collections.emptyList();

        } else {

            String[] hosts = string.split(",");

            List<InetAddress> addresses = Lists.newArrayListWithCapacity
                    (hosts.length);


            for (String host : hosts) {

                try {

                    addresses.add(InetAddress.getByName(host));

                } catch (UnknownHostException uhe) {

                    LOGGER.error(
                            String.format("Failed to resolve seed $s", host),
                            uhe);
                }
            }


            return addresses;

        }
    }

    private static Optional<Integer> parseInt(
            Optional<String> stringOption,
            String key) {

        if (stringOption.isPresent()) {

            String string = stringOption.get();

            try {
                return Optional.of(Integer.parseInt(string));

            } catch (NumberFormatException nfe) {

                LOGGER.error(String.format(
                        "%s must be set to a valid integer %s provided",
                        key,
                        string)
                        , nfe);

                return Optional.absent();
            }
        } else {

            return Optional.absent();
        }
    }


    private byte[] getSeedsData() {

        try {

            return client.getData().forPath(config.getSeedsPath());

        } catch (Throwable throwable) {

            LOGGER.error("failed to retrieve seeds from zookeeper", throwable);

            return null;
        }
    }

    /**
     * Gets the set of Cassandra seed node IP addresses from a ZooKeeper
     * ensemble. If an error occurs in communicating with the ZooKeeper
     * ensemble, and a previous value has been retrieved and cached, the
     * cached value is returned.
     *
     * @return The set of Cassandra seed node addresses.
     */
    public List<InetAddress> getSeeds() {

        List<InetAddress> zkSeeds = parseAddresses(asString(getSeedsData()));


        if (!zkSeeds.isEmpty()) {

            LOGGER.info("retrieved seeds from zookeeper seeds = [{}]",
                    ON_COMMA.join(zkSeeds));

            cached = zkSeeds;

            return zkSeeds;

        } else if (!cached.isEmpty()) {

            LOGGER.warn("failed to retrieve sees from zookeeper returning " +
                            "cached seeds seeds = [{}]",
                    ON_COMMA.join(cached));

            return cached;

        } else {

            LOGGER.error("failed to retrieve seeds from zookeeper and no " +
                    "cached value is available");

            return cached;
        }
    }


}
