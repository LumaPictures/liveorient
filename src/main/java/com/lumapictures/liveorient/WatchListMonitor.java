package com.lumapictures.liveorient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Subscribes to a queue and manages a query -> object watch list.
 */
public class WatchListMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WatchListMonitor.class);

    public final String address;
    public final String topic;

    private final ZMQ.Socket subscriber;

    /**
     * Watch list to maintain.
     */
    private final ConcurrentMap<String, Set<String>> watchList;

    private boolean running;
    private Thread thread;

    /**
     *
     * @param ctx ZeroMQ context
     * @param topic queue topic for watchlist management
     * @param address address ZeroMQ publisher address
     * @param watchList the watch list to manage
     */
    public WatchListMonitor(ZMQ.Context ctx, String topic, String address, ConcurrentMap<String, Set<String>> watchList) {
        subscriber = ctx.socket(ZMQ.SUB);
        this.address = address;
        this.topic = topic;
        this.watchList = watchList;
    }

    /**
     *
     * @param queryKey
     * @param rid
     */
    public void watchQueryObjects(String queryKey, String rid) {
        Set<String> ridSet = watchList.get(queryKey);
        if (ridSet == null)
            ridSet = new HashSet<String>();

        ridSet.add(rid);
    }

    /**
     *
     * @param queryKey
     * @param rids
     */
    public void watchQueryObjects(String queryKey, Collection<String> rids) {
        Set<String> ridSet = watchList.get(queryKey);
        if (ridSet == null)
            ridSet = new HashSet<String>();

        ridSet.addAll(rids);
    }

    /**
     *
     * @param queryKey
     */
    public void unwatchQuery(String queryKey) {
        watchList.remove(queryKey);
    }

    /**
     *
     * @param queryKey
     * @param rids
     */
    public void rewatchQuery(String queryKey, Collection<String> rids) {
        unwatchQuery(queryKey);
        watchQueryObjects(queryKey, rids);
    }

    @Override
    public void run() {
        log.info("Queue subscriber connecting to {}...", address);

        running = true;

        subscriber.connect(address);
        subscriber.subscribe(topic.getBytes());
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                String address = subscriber.recvStr(); // Envelope
                String contents = subscriber.recvStr(); // Message contents
                log.info("Recv {}: {}", address, contents);

                // TODO: Update watchList
            }
        } finally {
            subscriber.close();
            log.info("Queue subscriber stopped");
        }
    }

    /**
     *
     * @return
     */
    public Thread startThread() {
        thread = new Thread(this);
        thread.start();
        return thread;
    }

    /**
     *
     */
    public void stopThread() {
        running = false;
    }
}
