package com.taobao.metamorphosis.server.assembly;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


/**
 * An embed zookeeper server.
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public final class EmbedZookeeperServer {

    private ServerCnxnFactory standaloneServerFactory;

    private final int port = Integer.parseInt(System.getProperty("zk.server.port", "2181"));
    private final int tickTime = Integer.parseInt(System.getProperty("zk.server.tickTime", "1000"));

    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    private final String snapDir = System.getProperty("zk.server.snapDirectory", TMP_DIR + "/" + "zookeeper");
    private final String logDir = System.getProperty("zk.server.logDirectory", TMP_DIR + "/" + "zookeeper");
    private final int maxConnections = Integer.parseInt(System.getProperty("zk.server.max.connections", "4096"));

    static final Log logger = LogFactory.getLog(EmbedZookeeperServer.class);

    private static EmbedZookeeperServer instance = new EmbedZookeeperServer();


    private EmbedZookeeperServer() {

    }


    public static EmbedZookeeperServer getInstance() {
        return instance;
    }


    public void start() throws IOException, InterruptedException {
        File snapFile = new File(this.snapDir).getAbsoluteFile();
        if (!snapFile.exists()) {
            snapFile.mkdirs();
        }
        File logFile = new File(this.logDir).getAbsoluteFile();
        if (!logFile.exists()) {
            logFile.mkdirs();
        }

        ZooKeeperServer server = new ZooKeeperServer(snapFile, logFile, this.tickTime);
        this.standaloneServerFactory =
                NIOServerCnxnFactory.createFactory(new InetSocketAddress(this.port), this.maxConnections);
        this.standaloneServerFactory.startup(server);
        logger.info("Startup zookeeper server at port :" + this.port);
    }


    public void stop() {
        if (this.standaloneServerFactory != null) {
            this.standaloneServerFactory.shutdown();
        }
    }

}
