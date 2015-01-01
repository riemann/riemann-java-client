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
    public static long RefreshIntervalMillis = 60 * 1000;
    // enables setting a custom env var used for resolving
    public static String CustomEnvVarName = null;

    // cached hostname result
    private static String hostname;

    // update (refresh) time management
    private static long lastUpdate = 0;
    private static long lastNetUpdate = 0;
    public static long getLastUpdateTime() { return lastUpdate; }
    public static long getLastNetUpdateTime() { return lastNetUpdate; }
    public static void resetUpdateTimes() {
        lastUpdate = 0;
        lastNetUpdate = 0;
    }

    /**
     * get resolved hostname.
     * encapsulates all lookup and caching logic.
     *
     * @return the hostname
     */
    public static String getResolvedHostname() {
        long now = System.currentTimeMillis();
        if(now - RefreshIntervalMillis > lastUpdate) {
            refreshResolve();
        }

        return hostname;
    }

    /**
     * forces a new resolve even if refresh interval has not passed yet
     */
    public static void refreshResolve() {
        try {
            hostname = resolveByEnv();
            if(hostname == null || hostname.isEmpty()) {
                hostname = java.net.InetAddress.getLocalHost().getHostName();
                lastNetUpdate = System.currentTimeMillis();
            }
        } catch (UnknownHostException e) {
            //e.printStackTrace();
        }
        finally {
            lastUpdate = System.currentTimeMillis();
        }
    }

    /**
     * try to resolve the hostname by env vars
     *
     * @return
     */
    private static String resolveByEnv() {
        if(CustomEnvVarName != null) {
            return System.getenv(CustomEnvVarName);
        }

        if(System.getProperty("os.name").startsWith("Windows")) {
            return System.getenv(COMPUTERNAME);
        }

        return System.getenv(HOSTNAME);
    }
}
