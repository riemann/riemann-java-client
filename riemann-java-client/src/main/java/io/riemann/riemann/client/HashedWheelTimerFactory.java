package io.riemann.riemann.client;

import org.jboss.netty.util.HashedWheelTimer;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Creates hashed wheel timers
 */
public class HashedWheelTimerFactory {

    public static ThreadFactory daemonThreadFactory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread retVal = Executors.defaultThreadFactory().newThread(r);
            retVal.setDaemon(true);

            return retVal;
        }
    };

    /**
     * Creates hashed wheel timer that uses daemon threads
     *
     * @return HashedWheelTimer
     */
    public static HashedWheelTimer CreateDaemonHashedWheelTimer() {
        return new HashedWheelTimer(daemonThreadFactory);
    }

}
