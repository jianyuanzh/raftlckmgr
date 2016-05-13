package com.jsongrts.raftlckmgr.server;

import com.jsongrts.raftlckmgr.common.Constants;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by jsong on 4/18/16.
 */
public class TestController {
    private static final Logger _logger = LogManager.getLogger(Constants.LOGGER_NAME);

    static class Handler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object m) {
            CSMsg.Message msg = (CSMsg.Message)m;
            _logger.info(String.format("command=%s", msg.getCommand()));
        }
    }

    private Controller _controller = null;

    @Before
    public void setUp() throws Exception {
        _controller = new Controller(8888);
        _controller.start();
    }

    @After
    public void tearDown() {
        _controller.shutdown();
    }

    @Test
    public void test() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
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
                                    .addLast(new Handler());
                        }
                    });

            ChannelFuture f = null;

            f = b.connect("127.0.0.1", 8888).sync();
            Channel ch = f.channel();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            group.shutdownGracefully();
        }
    }
}
