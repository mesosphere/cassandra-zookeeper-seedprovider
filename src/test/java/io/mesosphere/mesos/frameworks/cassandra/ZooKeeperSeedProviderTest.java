package io.mesosphere.mesos.frameworks.cassandra;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;


import static org.junit.Assert.assertEquals;

/**
 * Created by kowens on 11/17/15.
 */
public class ZooKeeperSeedProviderTest {


    private static TestingServer server = null;

    private static CuratorFramework client = null;

    private static ZooKeeperSeedProvider provider;

    @BeforeClass
    public static void before() throws Exception{


        server = new TestingServer(1234);

        client = CuratorFrameworkFactory.newClient("localhost:1234",new
                RetryUntilElapsed(1000,250));

        client.start();

        client.blockUntilConnected();

        provider = new ZooKeeperSeedProvider(new HashMap<String,String>(){
            {
                put("zookeeper_server_addresses","127.0.0.1:1234");
                put("zookeeper_seeds_path","/cassandra/seeds");
            }
        });

    }

    @AfterClass
    public static void after() throws IOException {

        client.close();

        server.close();
    }

    @Test
    public void getAddresses() throws Exception {

        client.create().creatingParentsIfNeeded().forPath("/cassandra/seeds");

        client.setData().forPath("/cassandra/seeds","localhost".getBytes());

        List<InetAddress> addresses = provider.getSeeds();

        assertEquals(addresses.size(),1);

        assertEquals(addresses.get(0),InetAddress.getByName("localhost"));

        client.setData().forPath("/cassandra/seeds","localhost,google.com".getBytes());

        addresses = provider.getSeeds();

        assertEquals(addresses.size(),2);

        assertEquals(addresses.get(0),InetAddress.getByName("localhost"));

        assertEquals(addresses.get(1),InetAddress.getByName("google.com"));


        client.setData().forPath("/cassandra/seeds",("localhost,google.com," +
                "averybadaddress").getBytes ());


        addresses = provider.getSeeds();

        assertEquals(addresses.size(),2);

        assertEquals(addresses.get(0),InetAddress.getByName("localhost"));

        assertEquals(addresses.get(1),InetAddress.getByName("google.com"));

        client.delete().deletingChildrenIfNeeded().forPath("/cassandra/seeds");

        ///test caching

        addresses = provider.getSeeds();

        assertEquals(addresses.size(),2);

        assertEquals(addresses.get(0),InetAddress.getByName("localhost"));

        assertEquals(addresses.get(1),InetAddress.getByName("google.com"));



    }
}
