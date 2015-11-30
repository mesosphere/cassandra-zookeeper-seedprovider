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

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An immutable configuration parser for ZooKeeperSeedProvider.
 */
public class ZooKeeperSeedProviderConfig {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    ZooKeeperSeedProviderConfig.class);

    private final String seedsPath;

    private final String zkServers;

    private final int sessionTimeoutMs;

    private final int connectionTimeoutMs;

    private final int operationTimeoutMs;

    private static Optional<String> optionalString(
            String key,
            Map<String, String> parameters) {
        return Optional.fromNullable(parameters.get(key));
    }

    private static Optional<Integer> optionalInteger(
            String key,
            Map<String, String> parameters
    ) {
        Optional<String> stringOption = optionalString(key, parameters);

        if (stringOption.isPresent()) {

            try {

                return Optional.fromNullable(
                        Integer.parseInt(stringOption.get()));

            } catch (NumberFormatException nfe) {

                LOGGER.error(
                        String.format("failed to parse %s", key),
                        nfe
                );

                return Optional.absent();
            }

        } else {
            return Optional.absent();
        }

    }

    /**
     * Creates a new ZooKeeperSeedProviderConfig from parameters.
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
     * @throws MissingConfigException If parameters does not contain
     * zookeeper_server_addresses or zookeeper_seeds_path.
     * @throws InvalidConfigException If parameters contains
     * session_timeout_ms or operation_timeout_ms and either parameter is set
     * to a non-positive integer.
     */
    protected ZooKeeperSeedProviderConfig(Map<String, String> parameters)
            throws MissingConfigException,
            InvalidConfigException {

        Optional<String> zkServers = optionalString(ZOOKEEPERS_KEY, parameters);

        Optional<String> seedsPath = optionalString(SEEDS_PATH_KEY, parameters);

        Optional<Integer> sessionTimeout =
                optionalInteger(SESSION_TIMEOUT_KEY, parameters);

        Optional<Integer> connectionTimeout =
                optionalInteger(CONNECTION_TIMEOUT_KEY, parameters);

        Optional<Integer> operationTimeout =
                optionalInteger(OPERATION_TIMEOUT_KEY, parameters);

        validate(zkServers,
                seedsPath,
                sessionTimeout,
                connectionTimeout,
                operationTimeout);

        this.zkServers = zkServers.get();

        this.seedsPath = seedsPath.get();

        this.sessionTimeoutMs = sessionTimeout.or(DEFAULT_SESSION_TIMEOUT);

        this.operationTimeoutMs =
                operationTimeout.or(DEFAULT_OPERATION_TIMEOUT);

        this.connectionTimeoutMs =
                connectionTimeout.or(DEFAULT_CONNECTION_TIMEOUT);
    }

    protected void validate(
            Optional<String> zkServers,
            Optional<String> seedsPath,
            Optional<Integer> sessionTimeoutMs,
            Optional<Integer> connectionTimeoutMs,
            Optional<Integer> operationTimeoutMs
    ) throws MissingConfigException, InvalidConfigException {

        if (!zkServers.isPresent()) {

            LOGGER.error(String.format(
                    "%s must be set to a comma separated list of ZooKeeper  " +
                            "servers in cassandra.yaml",
                    ZOOKEEPERS_KEY
            ));

            throw new MissingConfigException(ZOOKEEPERS_KEY);
        }

        if(!seedsPath.isPresent()){

            LOGGER.error(String.format(
                    "%s must be set to a ZNode path in cassandra.yaml",
                    SEEDS_PATH_KEY
            ));

            throw new MissingConfigException(SEEDS_PATH_KEY);
        }

        if(!sessionTimeoutMs.isPresent()){

            LOGGER.info("{} not configured defaulting to {} ms",
                    SESSION_TIMEOUT_KEY,DEFAULT_SESSION_TIMEOUT);
        } else if (sessionTimeoutMs.get() <= 0){

            throw new InvalidConfigException(
                    SESSION_TIMEOUT_KEY,
                    "a strictly positive Integer",
                    sessionTimeoutMs.get().toString()
            );
        }

        if(!connectionTimeoutMs.isPresent()){

            LOGGER.info("{} not configured defaulting to {} ms",
                    CONNECTION_TIMEOUT_KEY,DEFAULT_CONNECTION_TIMEOUT);
        } else if (connectionTimeoutMs.get() <= 0){

            throw new InvalidConfigException(
                    CONNECTION_TIMEOUT_KEY,
                    "a strictly positive Integer",
                    sessionTimeoutMs.get().toString()
            );
        }

        if(!operationTimeoutMs.isPresent()){

            LOGGER.info("{} not configured defaulting to {} ms",
                    OPERATION_TIMEOUT_KEY,DEFAULT_OPERATION_TIMEOUT);

        }
    }

    /**
     * The parameter key for the path to the ZNode contain the Cassandra seed
     * nodes.
     */
    public static final String SEEDS_PATH_KEY = "zookeeper_seeds_path";

    /**
     * The parameter key for the comma separated list of host port pairs that
     * indicate the ZooKeeper servers in the ensemble.
     */
    public static final String ZOOKEEPERS_KEY = "zookeeper_server_addresses";

    /**
     * The parameter key for the ZooKeeper session timeout.
     */
    public static final String SESSION_TIMEOUT_KEY = "session_timeout_ms";

    /**
     * The parameter key for the ZooKeeper connection timeout.
     */
    public static final String CONNECTION_TIMEOUT_KEY =
            "connection_timeout_ms";

    /**
     * The parameter key for the ZooKeeper operation timeout.
     */
    public static final String OPERATION_TIMEOUT_KEY =
            "operation_retry_timeout_ms";

    /**
     * The default ZooKeeper session timeout (10000 ms)
     */
    public static final int DEFAULT_SESSION_TIMEOUT = 10000;

    /**
     * The default ZooKeeper operation timeout (-1 ms)
     */
    public static final int DEFAULT_OPERATION_TIMEOUT = -1;

    /**
     * The default ZooKeeper connection timeout (10000 ms)
     */
    public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

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
     * @return A ZooKeeperSeedProviderConfig with its properties parsed from
     * parameters.
     * @throws MissingConfigException If parameters does not contain
     * zookeeper_server_addresses or zookeeper_seeds_path.
     * @throws InvalidConfigException If parameters contains
     * session_timeout_ms or operation_timeout_ms and either parameter is set
     * to a non-positive integer.
     */
    public static ZooKeeperSeedProviderConfig from(
            Map<String, String> parameters)
            throws MissingConfigException,
            InvalidConfigException {

        return new ZooKeeperSeedProviderConfig(parameters);
    }

    /**
     * @return The fully qualified path to the ZNode containing the seeds for
     * the cluster.
     */
    public String getSeedsPath() {
        return seedsPath;
    }

    /**
     * @return A comma separated String containing the host:port pairs of the
     * ZooKeeper ensemble hosting the Cassandra seeds ZNode.
     */
    public String getZkServers() {
        return zkServers;
    }

    /**
     * @return The maximum time for a ZooKeeper session to timeout in ms.
     */
    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    /**
     * @return The maximum time that the SeedProvider should wait for a
     * connection before failing.
     */
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    /**
     * @return The maximum time that the SeedProvider should retry an operation
     * before timing it out.
     */
    public int getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    /**
     * Equality test.
     * @param o Object to be tested for equality.
     * @return If Object is a ZooKeeperSeedProviderConfig with all of its
     * properties equal to those of the instance on which it is invoked.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ZooKeeperSeedProviderConfig that = (ZooKeeperSeedProviderConfig) o;

        if (sessionTimeoutMs != that.sessionTimeoutMs) {
            return false;
        }

        if (connectionTimeoutMs != that.connectionTimeoutMs){
            return false;
        }

        if (operationTimeoutMs != that.operationTimeoutMs) {
            return false;
        }

        if (seedsPath != null ?
                !seedsPath.equals(that.seedsPath) :
                that.seedsPath != null){
            return false;
        }

        return !(zkServers != null ?
                !zkServers.equals(that.zkServers) :
                that.zkServers != null);

    }

    /**
     * @return A hash code based on all properites of the object on which it is
     * invoked.
     */
    @Override
    public int hashCode() {
        int result = seedsPath != null ? seedsPath.hashCode() : 0;
        result = 31 * result + (zkServers != null ? zkServers.hashCode() : 0);
        result = 31 * result + sessionTimeoutMs;
        result = 31 * result + connectionTimeoutMs;
        result = 31 * result + operationTimeoutMs;
        return result;
    }

    /**
     * @return A human readable String representation of the configuration.
     */
    @Override
    public String toString() {
        return "ZooKeeperSeedProviderConfig{" +
                "seedsPath='" + seedsPath + '\'' +
                ", zkServers='" + zkServers + '\'' +
                ", sessionTimeoutMs=" + sessionTimeoutMs +
                ", connectionTimeoutMs=" + connectionTimeoutMs +
                ", operationTimeoutMs=" + operationTimeoutMs +
                '}';
    }
}
