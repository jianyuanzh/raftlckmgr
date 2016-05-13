package com.jsongrts.raftlckmgr.client;

import com.google.common.base.Preconditions;
import com.jsongrts.raftlckmgr.common.Constants;
import com.jsongrts.raftlckmgr.common.LckMgrException;
import com.jsongrts.raftlckmgr.msg.CSMsg;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class LckMgrConnectionFactory {
    private static final Logger _logger = LogManager.getLogger(Constants.LOGGER_NAME);

    /**
     * Flag indicating if the factory is initialized
     */
    private static volatile boolean _inited = false;

    /**
     * LckMgr server's ip or dns
     */
    private static String _serverAddr = null;

    /**
     * LckMgr server's listening port
     */
    private static int _serverPort = 0;

    private static Bootstrap _b = null;

    /**
     * This hashmap tracks all outstanding connections.
     */
    private static ConcurrentHashMap<Channel, LckMgrConnection> _outstandingConns = new ConcurrentHashMap<>();

    /**
     * @throws LckMgrException
     */
    public static LckMgrConnection newConnection() {
        Preconditions.checkState(_inited);

        try {
            ChannelFuture f = _b.connect(_serverAddr, _serverPort).sync();
            LckMgrConnection c = new LckMgrConnection(f.channel());
            _outstandingConns.put(c.channel(), c);
            return c;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LckMgrException();
        }
    }



    /**
     * @throws LckMgrException
     */
    public static void init(final String serverAddr, final int serverPort) {
        Preconditions.checkState(!_inited);
        Preconditions.checkNotNull(serverAddr);
        Preconditions.checkArgument(serverPort > 0);

        _serverAddr = serverAddr;
        _serverPort = serverPort;

        // init netty
        EventLoopGroup group = new NioEventLoopGroup(1);
        _b = new Bootstrap();
        _b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // outbound (write)
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender())
                                .addLast(new ProtobufEncoder());

                        // inbound (read)
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder())
                                .addLast(new ProtobufDecoder(CSMsg.Message.getDefaultInstance()))
                                .addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                                        CSMsg.Message m = (CSMsg.Message)msg;
                                        LckMgrConnectionFactory.doResponse(ctx.channel(), m);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
                                        // Close the connection when an exception is raised.
                                        _logger.warn("Encounter exception when sending request", cause);
                                        _outstandingConns.remove(ctx.channel());
                                        ctx.close();
                                    }

                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) {
                                        _logger.info("A channel is closed. ch={}", ctx.channel().toString());
                                        _outstandingConns.remove(ctx.channel());
                                    }
                                });
                    }
                });

        _inited = true;
    }

    protected static void doResponse(final Channel ch, final CSMsg.Message response) {
        Preconditions.checkNotNull(ch);
        Preconditions.checkNotNull(response);

        LckMgrConnection conn = _outstandingConns.get(ch);
        if (conn != null) {
            conn.doResponse(response);
        }
        else {
            _logger.warn("The connection not found");
        }
    }

    protected static void deleteConnection(final LckMgrConnection conn) {
        Preconditions.checkState(_inited);
        Preconditions.checkNotNull(conn);

        _outstandingConns.remove(conn.channel());
    }

    public static void shutdown() {
        Preconditions.checkState(_inited);

        _inited = false;

        _serverPort = 0;
        _serverAddr = null;

        if (_b != null) {
            for (LckMgrConnection c : _outstandingConns.values()) {
                c.close();
            }
            _outstandingConns.clear();

            _b.group().shutdownGracefully();
            _b = null;
        }
    }
}
