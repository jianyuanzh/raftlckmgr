package com.jsongrts.raftlckmgr.server;

import com.jsongrts.raftlckmgr.client.LckMgrConnection;
import com.jsongrts.raftlckmgr.client.LckMgrConnectionFactory;
import com.jsongrts.raftlckmgr.common.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

/**
 * Created by jsong on 5/12/16.
 */
public class StepDebug {
    private static final Logger _logger = LogManager.getLogger(Constants.LOGGER_NAME);
    private static final int _port = 8888;
    private static final String _lckName = "mylock";

    @Test
    public void test() {
        Controller c = null;

        try {
            c = _startController(_port);

            LckMgrConnectionFactory.init("127.0.0.1", _port);
            LckMgrConnection conn = LckMgrConnectionFactory.newConnection();
            conn.lock(_lckName);
            conn.close();
            Thread.sleep(50000);
        }
        catch (Exception e) {
            _logger.error("Encounter an exception", e);
        }
        finally {
            if (c != null)
                c.shutdown();
            LckMgrConnectionFactory.shutdown();
        }
    }

    private Controller _startController(final int port) throws Exception {
        Controller c = new Controller(port);
        c.start();
        return c;
    }
}
