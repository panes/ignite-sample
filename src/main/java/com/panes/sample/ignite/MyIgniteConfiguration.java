package com.panes.sample.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;

public class MyIgniteConfiguration {
    public enum TcpDiscoveryMode {
        DIRECT_IP {
            @Override
            public TcpDiscoveryIpFinderAdapter ipFinder() {
                TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
                ipFinder.setAddresses(Collections.singletonList("ignite:47500..47510"));
                return ipFinder;
            }
        };

        public abstract TcpDiscoveryIpFinderAdapter ipFinder();
    }

    private static final Integer TCP_COMMUNICATION_SPI_PORT = 31100;
    public static final int CACHE_EXPIRATION_SEC = 10;
    private final IgniteCache<String, DummyData> dummyCache;

    MyIgniteConfiguration(final Optional<TcpDiscoveryMode> tcpDiscoveryMode) {
        IgniteConfiguration myIgniteConfiguration = igniteConfiguration(tcpDiscoveryMode);
        Ignite myIgnite = ignite(myIgniteConfiguration);
        this.dummyCache = dummyCache(myIgnite);
    }

    IgniteCache<String, DummyData> getDummyCache() {
        return dummyCache;
    }

    private IgniteConfiguration igniteConfiguration(final Optional<TcpDiscoveryMode> tcpDiscoveryMode) {
        System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

        final IgniteConfiguration myIgniteConfiguration = new IgniteConfiguration();

        if(!tcpDiscoveryMode.isPresent()) {
            myIgniteConfiguration.setClientMode(false);
        } else {
            myIgniteConfiguration.setClientMode(true);
            initTcpDiscovery(myIgniteConfiguration, tcpDiscoveryMode.get());
        }

        initTmpWorkingDirectory(myIgniteConfiguration);
        initMarshaller(myIgniteConfiguration);
        initTcpCommunicationSpi(myIgniteConfiguration);
        initDataStorageConfiguration(myIgniteConfiguration);

        return myIgniteConfiguration;
    }

    private void initTmpWorkingDirectory(IgniteConfiguration myIgniteConfiguration) {
        // Each Ignite client should have its own "work" directory (to avoid overrides).
        String tmpDirPath = System.getProperty("java.io.tmpdir");
        myIgniteConfiguration.setWorkDirectory(Paths.get(tmpDirPath, "ignite_test").toString());
    }

    private void initMarshaller(IgniteConfiguration myIgniteConfiguration) {
        OptimizedMarshaller marshaller = new OptimizedMarshaller();
        marshaller.setRequireSerializable(true);
        myIgniteConfiguration.setMarshaller(marshaller);
        myIgniteConfiguration.setClassLoader(Thread.currentThread().getContextClassLoader());
    }

    private void initTcpCommunicationSpi(IgniteConfiguration myIgniteConfiguration) {
        final TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
        try {
            tcpCommunicationSpi.setLocalAddress(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            throw new RuntimeException("Not able to get the host address.", e);
        }
        tcpCommunicationSpi.setLocalPort(TCP_COMMUNICATION_SPI_PORT);
        myIgniteConfiguration.setCommunicationSpi(tcpCommunicationSpi);
    }

    private void initDataStorageConfiguration(IgniteConfiguration myIgniteConfiguration) {
        final DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();
        dataStorageConfiguration.setMetricsEnabled(true);
        myIgniteConfiguration.setDataStorageConfiguration(dataStorageConfiguration);
        myIgniteConfiguration.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setMetricsEnabled(true);
    }

    private void initTcpDiscovery(final IgniteConfiguration myIgniteConfiguration, final TcpDiscoveryMode tcpDiscoveryMode) {
        final TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(tcpDiscoveryMode.ipFinder());
        myIgniteConfiguration.setDiscoverySpi(spi);
    }

    private Ignite ignite(IgniteConfiguration igniteConfiguration) throws IgniteException {
        final Ignite ignite = Ignition.start(igniteConfiguration);
        ignite.cluster().active(true);
        return ignite;
    }

    private IgniteCache<String, DummyData> dummyCache(Ignite ignite) {
        final org.apache.ignite.configuration.CacheConfiguration<String, DummyData> cacheConfiguration = createCacheConfiguration("dummy_cache");
        IgniteCache<String, DummyData> cache = ignite.getOrCreateCache(cacheConfiguration);
        cache.enableStatistics(true);

        return cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(TimeUnit.SECONDS, CACHE_EXPIRATION_SEC)));
    }

    private <K, V> org.apache.ignite.configuration.CacheConfiguration<K, V> createCacheConfiguration(final String cacheName) {
        final org.apache.ignite.configuration.CacheConfiguration<K, V> cacheConfiguration = new org.apache.ignite.configuration.CacheConfiguration<>();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setEagerTtl(true);
        cacheConfiguration.setStatisticsEnabled(true);

        return cacheConfiguration;
    }
}

