package com.lumapictures.liveorient;

import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * Wires up OrientDB server to ZeroMQ, where inserts/updates/deletes get published.
 * This is used by clients that want to subscribe to database changes.
 */
public class LiveOrientHook extends ORecordHookAbstract {
    private static final Logger log = LoggerFactory.getLogger(LiveOrientHook.class);

    private ZMQ.Context queueCtx;
    private ZMQ.Socket queueSock;

    /**
     * Initialize ZeroMQ server, where updates will be published to.
     */
    private synchronized void initZMQ() {
        if (queueSock == null) {
            log.debug("Initializing ZeroMQ...");
            queueCtx = ZMQ.context(1);
            queueSock = queueCtx.socket(ZMQ.PUB);
            queueSock.bind("tcp://*:5555");
        }
    }

    /**
     * Publish update to ZeroMQ server.
     * Any connected clients will receive updates.
     *
     * @param action char representing action that occurred: (I)nsert, (U)pdate, (D)elete.
     * @param record the affected record.
     */
    private void publish(String action, ORecord<?> record) {
        initZMQ();
        log.info("Publishing: %s", action);
        queueSock.send("nothing to see here... move along...");
    }

    @Override
    public void onRecordAfterUpdate(ORecord<?> iiRecord) {
        publish("U", iiRecord);
        super.onRecordAfterUpdate(iiRecord);
    }

    @Override
    public void onRecordAfterCreate(ORecord<?> iiRecord) {
        publish("I", iiRecord);
        super.onRecordAfterCreate(iiRecord);
    }

    @Override
    public void onRecordAfterDelete(ORecord<?> iiRecord) {
        publish("D", iiRecord);
        super.onRecordAfterDelete(iiRecord);
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return DISTRIBUTED_EXECUTION_MODE.BOTH;
    }
}