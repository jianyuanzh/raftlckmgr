package com.jsongrts.raftlckmgr.server;

import com.jsongrts.raftlckmgr.common.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by jsong on 4/16/16.
 */
public class Main {
    private static final Logger _logger = LogManager.getLogger(Constants.LOGGER_NAME);

    public static void main(String[] args) throws Exception {
        Controller c = new Controller(8888);
        try {
            _logger.info("Start");
            c.start();
            c.awaitTermination();
        }
        finally {
            _logger.info("Shutdown");
            c.shutdown();
        }
    }
}
