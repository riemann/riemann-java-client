package com.aphyr.riemann.client;

import java.net.UnknownHostException;

/**
 * A "Smarter" localhost resolver
 * see issue: https://github.com/aphyr/riemann-java-client/issues/44
 * Trying to avoid a lot of calls to java.net.InetAddress.getLocalHost()
 * which under AWS trigger DNS resolving and have relatively high latency *per event*
 * usually, the hostname doesn't change so often to warrant a real query.
 *
 * A real call to java.net.InetAddress.getLocalHost().getHostName()
 * is made only if:
 * 1) the refresh interval has passed (=result is stale)
 * AND
 * 2) no env vars that identify the hostname are found
 */
public class LocalhostResolver {

    // default hostname env var names on Win/Nix
    public static final String COMPUTERNAME = "COMPUTERNAME"; // Windows
    public static final String HOSTNAME = "HOSTNAME"; // Nix

    // how often should we refresh the cached hostname
    public static long refreshIntervalMillis = 60 * 1000;

    // cached hostname result
    private static boolean envResolved = false;
    private static String hostname;

    // update (refresh) time management
    private static long lastUpdate = 0;
    public static long getLastUpdateTime() { return lastUpdate; }
    // this is mostly for testing, plus ability to force a refresh
    public static void setLastUpdateTime(long time) {
        lastUpdate = time;
    }

    static {
        resolveByEnv();
    }

    /**
     * get resolved hostname.
     * encapsulates all lookup and caching logic.
     *
     * @return the hostname
     */
    public static String getResolvedHostname() {
        long now = System.currentTimeMillis();
        if((now - refreshIntervalMillis) > lastUpdate) {
            refreshResolve();
        }

        return hostname;
    }

    /**
     * forces a new resolve even if refresh interval has not passed yet
     */
    public static void refreshResolve() {
        try {
            if(hostname == null || hostname.isEmpty()) {
                hostname = java.net.InetAddress.getLocalHost().getHostName();
                if (hostname == null) {
                    hostname = "localhost";
                }
                lastUpdate = System.currentTimeMillis();
            }
        } catch (UnknownHostException e) {
            // fallthrough
        }
    }

    /**
     * try to resolve the hostname by env vars
     *
     */
    public static void resolveByEnv() {
        String var;
        if(System.getProperty("os.name").startsWith("Windows")) {
            var =  System.getenv(COMPUTERNAME);
            if(var == null) {
                var  = "localhost";
            }
        }
        else {
            var = System.getenv(HOSTNAME);
        }

        hostname = var;
    }
}
