package com.lumapictures.liveorient;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Wires up OrientDB server to ZeroMQ, where inserts/updates/deletes get published.
 * This is used by clients that want to subscribe to database changes.
 */
public class LiveOrientHook extends OServerPluginAbstract implements ODatabaseLifecycleListener, ORecordHook {
    private static final Logger log = LoggerFactory.getLogger(LiveOrientHook.class);

    private ZMQ.Context ctx;
    private ZMQ.Socket publisher;
    private int queuePort = 5555;

    private static final String WATCHES_TOPIC = "watches";
    private static final String CHANGES_TOPIC = "changes";

    /**
     * List of RIDs, keyed by query.
     * TODO: Find or implement a concurrent version of LinkedHashMap (JDK doesn't have a ConcurrentLinkedHashMap).
     */
    private ConcurrentMap<String, Set<String>> watchList = new ConcurrentHashMap<String, Set<String>>();

    private WatchListMonitor watchMonitor;

    public LiveOrientHook() {
        log.info("Initializing the LiveOrientHook...");
        ctx = ZMQ.context(1);

        Orient.instance().addDbLifecycleListener(this);
    }

    /**
     * Publish update to ZeroMQ server.
     * Any connected clients will receive updates.
     *
     * @param record the affected record.
     */
    private synchronized void publishChange(ORecord record) {
        //String database = "DB"; //record.getDatabase().??;
        //String className = "CLASS"; // ??
        String rid = record.getIdentity().toString();
        //int version = record.getVersion();

        log.info("Object {} changed", rid);

        String queryKey = getWatchedQuery(rid);
        if (true || queryKey != null) {
            log.info("Publishing change for query {}", queryKey);

            publisher.sendMore(CHANGES_TOPIC); // Envelope
            publisher.send("foo");
            //publisher.send(queryKey); // Payload
        }
    }

    /**
     * Checks if there's a query that's interested in changes.
     *
     * @param rid record ID to test
     * @return matching queryKey in watchList
     */
    private String getWatchedQuery(String rid) {
        for (Map.Entry<String, Set<String>> entry : watchList.entrySet()) {
            String queryKey = entry.getKey();
            Set<String> rids = entry.getValue();
            if (rids.contains(rid))
                return queryKey;
        }

        return null;
    }

    // OServerPluginAbstract

    @Override
    public void config(OServer server, OServerParameterConfiguration[] params) {
        for (OServerParameterConfiguration param : params) {
            if (param.name.equalsIgnoreCase("queuePort"))
                queuePort = Integer.parseInt(param.value);
        }

        log.info("Initializing ZeroMQ...");
        publisher = ctx.socket(ZMQ.PUB);
        publisher.bind(String.format("tcp://*:%d", queuePort));

        // Maintain the watchList
        watchMonitor = new WatchListMonitor(ctx, WATCHES_TOPIC,
                String.format("tcp://localhost:%d", queuePort), watchList);
        watchMonitor.startThread();
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    // ORecordHook

    @Override
    public void onUnregister() {
        //log.info("Hook unregistered");
    }

    @Override
    public RESULT onTrigger(TYPE eventType, ORecord record) {
        switch (eventType) {
            //case AFTER_CREATE:
            case AFTER_UPDATE:
            case AFTER_DELETE:
                if (!record.getIdentity().isTemporary())
                    publishChange(record);
                break;
        }

        return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
    }

    // ODatabaseLifecycleListener

    @Override
    public PRIORITY getPriority() {
        return PRIORITY.REGULAR;
    }

    @Override
    public void onCreate(ODatabaseInternal database) {
        //log.info("Registering hook (onCreate)");
        ((ODatabaseComplex<?>)database).registerHook(this);
    }

    @Override
    public void onOpen(ODatabaseInternal database) {
        //log.info("Registering hook (onOpen)");
        ((ODatabaseComplex<?>)database).registerHook(this);
    }

    @Override
    public void onClose(ODatabaseInternal database) {
        //log.info("Unregistering hook (onClose)");
        ((ODatabaseComplex<?>)database).unregisterHook(this);
    }
}
