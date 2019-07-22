package com.panes.sample.ignite;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ignite.IgniteCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MyIgniteLoadRunnerTest {
    private static final MyIgniteConfiguration MY_IGNITE_CONFIGURATION = new MyIgniteConfiguration(Optional.empty());

    @BeforeEach
    void before() throws InterruptedException {
        clearCache();
    }

    @Test
    void put10kVerySmallData() throws InterruptedException {
        final int maxAttempts = 30;
        int counter = 1;
        while(counter <= maxAttempts) {
            System.out.println("--------------------------------------------------------------------");
            System.out.println("Start attempt" + " : " + counter);
            putDataAndValidateExpiration("LOOP", 100000, 10);
            System.out.println("End attempt" + " : " + counter);
            counter++;
        }
    }

    private void clearCache() throws InterruptedException {
        getDummyCache().removeAll();
        Thread.sleep(1000);
        long cacheSize = getDummyCache().metrics().getCacheSize();
        assertThat(cacheSize).isEqualTo(0L);
        System.out.println("[INIT] Cache cleared.");
    }

    private void shouldPutData(String name, long size, int nbChars) throws InterruptedException {
        Thread.sleep(2000);
        long beforeCacheSize = getActualCacheSize();
        final String data = buildString(nbChars);
        final long memoryUsed = 8 * ((((nbChars) * 2) + 45) / 8);
        System.out.println("[" + name + "] Memory footprint per object: " + memoryUsed + " octets");
        System.out.println("[" + name + "] Total Memory used expected: " + (memoryUsed * size) + " octets");

        for (int i = 0; i < size; i++) {
            //long start = System.nanoTime();
            final DummyData dummyData = new DummyData(UUID.randomUUID().toString(), data);
            getDummyCache().put(dummyData.getKey(), dummyData);
            //long end = System.nanoTime();
            //LOGGER.info("Data(UID=" + dummyData.getKey() + "): " + (end-start)/1000/1000+"ms");
        }

        Thread.sleep(2000);
        long actualCacheSize = getActualCacheSize();
        assertThat(actualCacheSize - beforeCacheSize).isEqualTo(size);
    }

    private IgniteCache<String, DummyData> getDummyCache() {
        return MY_IGNITE_CONFIGURATION.getDummyCache();
    }

    private String buildString(int length) {
        return RandomStringUtils.random(length, true, false);
    }

    private void printOffHeapEntriesCount(String message) {
        long actualCacheSize = getActualCacheSize();
        System.out.println(message + " : " + actualCacheSize);
    }

    private boolean validateZeroOffHeapEntriesAfterExpirationTime(String name) throws InterruptedException {
        long expiryTimeInSec = MyIgniteConfiguration.CACHE_EXPIRATION_SEC;
        int extraTimeSec = 10;
        long expiryTimeInMillis = expiryTimeInSec * 1000 + extraTimeSec * 1000;
        System.out.println("[" + name + "] Waiting for expiration in " + expiryTimeInSec + "s");
        Thread.sleep(expiryTimeInMillis);
        long actualCacheSize = getActualCacheSize();
        System.out.println("[" + name + "] Final cache size " + actualCacheSize);
        assertThat(actualCacheSize).isEqualTo(0L);
        return true;
    }

    private long getActualCacheSize() {
        return getDummyCache().metrics().getOffHeapEntriesCount();
    }

    private boolean putDataAndValidateExpiration(String name, long size, int nbChars) throws InterruptedException {
        printOffHeapEntriesCount("[" + name + "] Before cache size");
        shouldPutData(name, size, nbChars);
        printOffHeapEntriesCount("[" + name + "] After cache size");
        return validateZeroOffHeapEntriesAfterExpirationTime(name);
    }
}
