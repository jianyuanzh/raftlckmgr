package com.jsongrts.raftlckmgr.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.jsongrts.raftlckmgr.common.LckMgrException;
import com.jsongrts.raftlckmgr.msg.CSMsg;
import io.netty.channel.Channel;

import java.util.concurrent.Semaphore;

/**
 * LckMgrConnection isn't thread-safe. It's up to the application to synchronize the access to this object.
 *
 */
public class LckMgrConnection {
    private Channel _channel;

    /**
     * Used to coordinate the processing of the response. The caller thread waits
     * on _sem after sending the request to the server by calling _waitForResponse().
     * Upon receiving the response the Netty evenloop thread will call doResponse() which
     * release the semaphore. This will wake up the caller thread.
     */
    private final Semaphore _sem = new Semaphore(0);
    private CSMsg.Message _response = null;

    public LckMgrConnection(final Channel channel) {
        Preconditions.checkNotNull(channel);

        _channel = channel;
    }


    protected Channel channel() {
        return _channel;
    }

    /**
     * @throws LckMgrException
     */
    public void lock(final String lckName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(lckName));
        Preconditions.checkState(_channel != null);

        CSMsg.Message.Builder b = CSMsg.Message.newBuilder();
        b.setCommand(CSMsg.Message.Command.LOCK);

        CSMsg.LockPayload.Builder pb = CSMsg.LockPayload.newBuilder();
        pb.setLockName(lckName);
        b.setLockPayload(pb);

        _channel.writeAndFlush(b.build());
        CSMsg.Message m = _waitForResponse();

        if (!m.hasLockResponse())
            throw new LckMgrException(); // the channel might be corrupted.

        CSMsg.LockResponse r = m.getLockResponse();
        if (r.getStatus() != CSMsg.Status.OK)
            throw new LckMgrException();
    }


    private CSMsg.Message _waitForResponse() {
        try {
            _sem.acquire();
            CSMsg.Message m = _response;
            _response = null;
            return m;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LckMgrException();
        }
    }


    /**
     * Called by the netty eventloop thread to notify this connection a response
     * is received.
     */
    protected void doResponse(final CSMsg.Message msg) {
        _response = msg;
        _sem.release();
    }


    public void unlock(final String lckName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(lckName));
        Preconditions.checkState(_channel != null);

        CSMsg.Message.Builder b = CSMsg.Message.newBuilder();
        b.setCommand(CSMsg.Message.Command.UNLOCK);

        CSMsg.UnlockPayload.Builder pb = CSMsg.UnlockPayload.newBuilder();
        pb.setLockName(lckName);
        b.setUnlockPayload(pb);

        _channel.writeAndFlush(b.build());
        CSMsg.Message m = _waitForResponse();

        if (!m.hasUnlockResponse())
            throw new LckMgrException(); // the channel might be corrupted.

        CSMsg.UnlockResponse r = m.getUnlockResponse();
        if (r.getStatus() != CSMsg.Status.OK)
            throw new LckMgrException();
    }


    /**
     * returns true if acquires the lock, false otherwise
     * @throws LckMgrException
     */
    public boolean tryLock(final String lckName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(lckName));
        Preconditions.checkState(_channel != null);

        CSMsg.Message.Builder b = CSMsg.Message.newBuilder();
        b.setCommand(CSMsg.Message.Command.TRYLOCK);

        CSMsg.TrylockPayload.Builder pb = CSMsg.TrylockPayload.newBuilder();
        pb.setLockName(lckName);
        b.setTrylockPayload(pb);

        _channel.writeAndFlush(b.build());
        CSMsg.Message m = _waitForResponse();

        if (!m.hasTrylockResponse())
            throw new LckMgrException(); // the channel might be corrupted.

        CSMsg.TrylockResponse r = m.getTrylockResponse();
        if (r.getStatus() == CSMsg.Status.TRYLOCK_TIMEOUT)
            return false;
        else if (r.getStatus() == CSMsg.Status.OK)
            return true;
        else
            throw new LckMgrException();
    }


    /**
     * The connection can't be reused after being closed.
     */
    public void close() {
        if (_channel != null) {
            try {
                _channel.close().sync();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                LckMgrConnectionFactory.deleteConnection(this);
                _channel = null;
            }
        }
    }
}
