package com.jsongrts.raftlckmgr.common;

import com.jsongrts.raftlckmgr.msg.CSMsg;

/**
 * Created by jsong on 4/17/16.
 */
public class Constants {
    public static final String LOGGER_NAME = "raftlckmgr";

    public static final String status2String(final CSMsg.Status status) {
        switch (status) {
            case OK:
                return "OK";

            case INVALID_LOCK:
                return "Invalid lock";

            case NOT_LOCK_OWNER:
                return "Not lock owner";

            case TRYLOCK_TIMEOUT:
                return "Trylock timeout";

            case UNKNOWN:
                return "Unknown";

            default:
                throw new LckMgrException();
        }
    }
}
