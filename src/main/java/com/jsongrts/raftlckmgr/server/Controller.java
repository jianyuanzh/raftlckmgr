package com.jsongrts.raftlckmgr.server;

import com.jsongrts.raftlckmgr.common.Constants;
import com.jsongrts.raftlckmgr.msg.CSMsg;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jsong on 4/17/16.
 */
public class Controller {
    private static final Logger _logger = LogManager.getFormatterLogger(Constants.LOGGER_NAME);

    private final int _listenPort;

    private EventLoopGroup _bossGroup = null;
    private EventLoopGroup _workerGroup = null;
    private ChannelFuture _listeningFuture = null;

    private CountDownLatch _stopLatch = null;

    /**
     * All active channels and locks currently hold by them
     */
    private ConcurrentHashMap<Channel, LinkedList<String>> _channels = null;

    /**
     * Waiting list of all locks
     */
    private ConcurrentHashMap<String, LinkedList<Channel>> _waitingList = null;

    /**
     * All locks and its current holder (channel)
     */
    private ConcurrentHashMap<String, Channel> _currentHolder = null;


    public Controller(final int listenPort) {
        _listenPort = listenPort;
    }

    public void start() throws Exception {
        _channels = new ConcurrentHashMap<>();
        _waitingList = new ConcurrentHashMap<>();
        _currentHolder = new ConcurrentHashMap<>();
        _initNetty();
        _stopLatch = new CountDownLatch(1);
    }

    public void awaitTermination() throws InterruptedException {
        _stopLatch.await();
    }

    public void shutdown() {
        _logger.info("Shutdown controller");

        if (_listeningFuture != null) {
            try {
                _listeningFuture.channel().close().sync();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            _listeningFuture = null;
        }

        if (_bossGroup != null) _bossGroup.shutdownGracefully();
        if (_workerGroup != null) _workerGroup.shutdownGracefully();

        _bossGroup = null;
        _workerGroup = null;

        _stopLatch.countDown();
        _stopLatch = null;
    }

    /**
     * called from netty eventloop thread. Since we only have one
     * eventloop thread, so there isn't synchronization issue.
     */
    protected void lock(final Channel ch, final String lckName) {
        if (!_channels.containsKey(ch)) {
            _logger.warn("Find an orphan channel. ch=%s, lock=%s", ch.toString(), lckName);
            ch.close();
        }
        else {
            if (!_currentHolder.containsKey(lckName)) {
                // this is a new lock
                _addNewLock(ch, lckName);
                _currentHolder.put(lckName, ch);
                _sendResponse(ch, CSMsg.Message.Command.LOCK_RESPONSE, CSMsg.Status.OK);
            }
            else {
                // someone already holds the lock
                Channel curHolder = _currentHolder.get(lckName);
                if (curHolder == ch) {
                    // this guy tries to relock the lock it already hold. It's ok
                    _sendResponse(ch, CSMsg.Message.Command.LOCK_RESPONSE, CSMsg.Status.OK);
                }
                else {
                    // someone else holds the lock right now, you have to wait in the waiting list
                    LinkedList<Channel> waitingList = null;
                    if (!_waitingList.containsKey(lckName)) {
                        waitingList = new LinkedList<>();
                        _waitingList.put(lckName, waitingList);
                    }
                    else {
                        waitingList = _waitingList.get(lckName);
                    }

                    waitingList.add(ch);
                }
            }
        }
    }


    /**
     * Called from netty eventloop thread. Since we only have one
     * eventloop therad, so there isn't synchronization issue.
     */
    protected void unlock(final Channel ch, final String lckName) {
        if (!_channels.containsKey(ch)) {
            ch.close();
            return;
        }

        Channel currHolder = _currentHolder.get(lckName);
        if (currHolder != ch) {
            _sendResponse(ch, CSMsg.Message.Command.LOCK_RESPONSE, CSMsg.Status.NOT_LOCK_OWNER);
            return;
        }

        // remove the lock from the lock list currently hold by this channel
        LinkedList<String> locks = _channels.get(ch);
        locks.remove(lckName);
        _currentHolder.remove(lckName);

        // handle the waiting list
        LinkedList<Channel> waitingList = _waitingList.get(lckName);
        if (waitingList != null) {
            // someone else is waiting for the lock
            Channel nextCh = waitingList.removeFirst();
            if (waitingList.size() == 0)
                _waitingList.remove(lckName);
            _currentHolder.put(lckName, nextCh);
            _addNewLock(nextCh, lckName);
            _sendResponse(nextCh, CSMsg.Message.Command.LOCK_RESPONSE, CSMsg.Status.OK);
        }

        _sendResponse(ch, CSMsg.Message.Command.UNLOCK_RESPONSE, CSMsg.Status.OK);
    }


    /**
     * Called from netty eventloop thread. Since we only have one
     * eventloop therad, so there isn't synchronization issue.
     */
    protected void tryLock(final Channel ch, final String lckName) {
        if (!_channels.containsKey(ch)) {
            ch.close();
        }
        else {
            if (!_currentHolder.containsKey(lckName)) {
                // this is a new lock
                _addNewLock(ch, lckName);
                _currentHolder.put(lckName, ch);
                _sendResponse(ch, CSMsg.Message.Command.TRYLOCK_RESPONSE, CSMsg.Status.OK);
            }
            else {
                // someone already holds the lock
                Channel curHolder = _currentHolder.get(lckName);
                if (curHolder == ch) {
                    // this guy tries to relock the lock it already hold. It's ok
                    _sendResponse(ch, CSMsg.Message.Command.TRYLOCK_RESPONSE, CSMsg.Status.OK);
                }
                else {
                    // someone else holds the lock right now
                    _sendResponse(ch, CSMsg.Message.Command.TRYLOCK_RESPONSE, CSMsg.Status.TRYLOCK_TIMEOUT);
                }
            }
        }
    }


    /**
     * Called from netty eventloop thread. Since we only have one
     * eventloop therad, so there isn't synchronization issue.
     */
    protected void doConnectionClose(final Channel ch) {
        if (!_channels.containsKey(ch))
            return;

        // clean up waiting list
        for (LinkedList<Channel> waitingList : _waitingList.values()) {
            waitingList.remove(ch);
        }

        // release all locks hold by this channel
        LinkedList<String> locks = _channels.get(ch);
        _channels.remove(ch);
        for (String lck : locks) {
            _currentHolder.remove(lck);

            LinkedList<Channel> waitingList = _waitingList.get(lck);
            if (waitingList == null)
                continue;

            // someone else is waiting for the lock
            Channel nextCh = waitingList.removeFirst();
            _currentHolder.put(lck, nextCh);
            _addNewLock(nextCh, lck);
            _sendResponse(nextCh, CSMsg.Message.Command.LOCK_RESPONSE, CSMsg.Status.OK);
            if (waitingList.size() == 0)
                _waitingList.remove(lck);
        }
    }


    /**
     * Add a new lock into the lock list hold by the specified channel
     */
    private void _addNewLock(final Channel ch, final String lckName) {
        LinkedList<String> locks = _channels.get(ch);
        if (locks == null) {
            locks = new LinkedList<String>();
            _channels.put(ch, locks);
        }

        locks.add(lckName);
    }


    private void _sendResponse(final Channel ch, final CSMsg.Message.Command cmd, final CSMsg.Status status) {
        CSMsg.Message.Builder b = CSMsg.Message.newBuilder();
        b.setCommand(cmd);
        switch (cmd)
        {
            case LOCK_RESPONSE:
                CSMsg.LockResponse.Builder pb = CSMsg.LockResponse.newBuilder();
                pb.setStatus(status).setErrmsg(Constants.status2String(status));
                b.setLockResponse(pb);
                break;

            case UNLOCK_RESPONSE:
                CSMsg.UnlockResponse.Builder pb1 = CSMsg.UnlockResponse.newBuilder();
                pb1.setStatus(status).setErrmsg(Constants.status2String(status));
                b.setUnlockResponse(pb1);
                break;

            case TRYLOCK_RESPONSE:
                CSMsg.TrylockResponse.Builder pb2 = CSMsg.TrylockResponse.newBuilder();
                pb2.setStatus(status).setErrmsg(Constants.status2String(status));
                b.setTrylockResponse(pb2);
                break;
        }

        ch.writeAndFlush(b.build());
    }

    static class RequestHandler extends ChannelInboundHandlerAdapter {
        private final Controller _controller;

        public RequestHandler(final Controller controller) {
            _controller = controller;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            CSMsg.Message m = (CSMsg.Message)msg;
            switch (m.getCommand())
            {
                case LOCK:
                    _controller.lock(ctx.channel(), ((CSMsg.Message) msg).getLockPayload().getLockName());
                    break;

                case UNLOCK:
                    _controller.unlock(ctx.channel(), ((CSMsg.Message) msg).getUnlockPayload().getLockName());
                    break;

                case TRYLOCK:
                    _controller.tryLock(ctx.channel(), ((CSMsg.Message) msg).getUnlockPayload().getLockName());
                    break;

                default:
                    _logger.warn("Unsupported command, cmd=" + m.getCommand());
                    // todo: close the channel
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            _logger.error("Encounter exception", cause);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            _controller.doConnectionClose(ctx.channel());
            ctx.fireChannelInactive();
        }
    }


    private void _initNetty() throws Exception {
        Controller c = this;
        _bossGroup = new NioEventLoopGroup(1);
        _workerGroup = new NioEventLoopGroup(1);

        ServerBootstrap b = new ServerBootstrap();
        b.group(_bossGroup, _workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        _logger.info("Get a new channel, ch=%s", ch.toString());
                        ch.pipeline()
                                // inbound
                                .addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(CSMsg.Message.getDefaultInstance()))
                                .addLast(new RequestHandler(c))
                                // outbound
                                .addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder());
                        _channels.put(ch, new LinkedList<String>());
                    }
                });

        _listeningFuture = b.bind(_listenPort).sync();
    }
}
