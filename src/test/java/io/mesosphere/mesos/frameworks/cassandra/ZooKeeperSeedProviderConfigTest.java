package io.mesosphere.mesos.frameworks.cassandra;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ZooKeeperSeedProviderConfigTest {

    private static Map<String, String> validParameters() {
        return new HashMap<String, String>() {
            {
                put(ZooKeeperSeedProviderConfig.ZOOKEEPERS_KEY,
                        "localhost:1234,localhost:1235,localhost:1236");
                put(ZooKeeperSeedProviderConfig.SEEDS_PATH_KEY,
                        "/cassandra/seeds");

                put(ZooKeeperSeedProviderConfig.CONNECTION_TIMEOUT_KEY,
                        Integer.toString(6000));

                put(ZooKeeperSeedProviderConfig.OPERATION_TIMEOUT_KEY,
                        Integer.toString(5000));

                put(ZooKeeperSeedProviderConfig.SESSION_TIMEOUT_KEY,
                        Integer.toString(4000));
            }
        };
    }

    @Test(expected = MissingConfigException.class)
    public void throwsNoServers() throws MissingConfigException,
            InvalidConfigException {

        Map<String, String> params = validParameters();

        params.remove(ZooKeeperSeedProviderConfig.ZOOKEEPERS_KEY);

        ZooKeeperSeedProviderConfig.from(params);
    }

    @Test(expected = MissingConfigException.class)
    public void throwsNoSeedPath() throws MissingConfigException,
            InvalidConfigException {

        Map<String, String> params = validParameters();

        params.remove(ZooKeeperSeedProviderConfig.SEEDS_PATH_KEY);

        ZooKeeperSeedProviderConfig.from(params);
    }

    @Test(expected = InvalidConfigException.class)
    public void throwsNegativeSession() throws MissingConfigException,
            InvalidConfigException {


        Map<String, String> params = validParameters();

        params.put(ZooKeeperSeedProviderConfig.SESSION_TIMEOUT_KEY,
                Integer.toString(-1));

        ZooKeeperSeedProviderConfig.from(params);
    }

    @Test(expected = InvalidConfigException.class)
    public void throwsNegativeConnection() throws MissingConfigException,
            InvalidConfigException {


        Map<String, String> params = validParameters();

        params.put(ZooKeeperSeedProviderConfig.CONNECTION_TIMEOUT_KEY,
                Integer.toString(-1));

        ZooKeeperSeedProviderConfig.from(params);
    }

    @Test
    public void createsValid()
            throws MissingConfigException, InvalidConfigException {

        Map<String, String> params = validParameters();
        ZooKeeperSeedProviderConfig config =
                ZooKeeperSeedProviderConfig.from(params);

        assertEquals(
                config.getZkServers(),
                params.get(ZooKeeperSeedProviderConfig.ZOOKEEPERS_KEY)
        );

        assertEquals(config.getSeedsPath(),
                params.get(ZooKeeperSeedProviderConfig.SEEDS_PATH_KEY));

        assertEquals(config.getConnectionTimeoutMs(),
                Integer.parseInt(params.get(ZooKeeperSeedProviderConfig
                        .CONNECTION_TIMEOUT_KEY))
        );

        assertEquals(config.getSessionTimeoutMs(),
                Integer.parseInt(params.get(ZooKeeperSeedProviderConfig
                        .SESSION_TIMEOUT_KEY))
        );

        assertEquals(config.getOperationTimeoutMs(),
                Integer.parseInt(params.get(ZooKeeperSeedProviderConfig
                        .OPERATION_TIMEOUT_KEY))
        );
    }

    @Test
    public void defaultsValues()
            throws MissingConfigException, InvalidConfigException {

        Map<String, String> params = validParameters();

        params.remove(ZooKeeperSeedProviderConfig.CONNECTION_TIMEOUT_KEY);

        params.remove(ZooKeeperSeedProviderConfig.OPERATION_TIMEOUT_KEY);

        params.remove(ZooKeeperSeedProviderConfig.SESSION_TIMEOUT_KEY);

        ZooKeeperSeedProviderConfig config =
                ZooKeeperSeedProviderConfig.from(params);

        assertEquals(
                config.getZkServers(),
                params.get(ZooKeeperSeedProviderConfig.ZOOKEEPERS_KEY)
        );

        assertEquals(config.getSeedsPath(),
                params.get(ZooKeeperSeedProviderConfig.SEEDS_PATH_KEY));

        assertEquals(config.getConnectionTimeoutMs(),
                ZooKeeperSeedProviderConfig.DEFAULT_CONNECTION_TIMEOUT
        );

        assertEquals(config.getSessionTimeoutMs(),
                ZooKeeperSeedProviderConfig.DEFAULT_SESSION_TIMEOUT
        );

        assertEquals(config.getOperationTimeoutMs(),
                ZooKeeperSeedProviderConfig.DEFAULT_OPERATION_TIMEOUT);

    }

}
