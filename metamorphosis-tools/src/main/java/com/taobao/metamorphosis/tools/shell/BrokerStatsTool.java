package com.taobao.metamorphosis.tools.shell;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.StringUtil;
import com.taobao.metamorphosis.utils.ResourceUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * Usage: stats -config configFilePath -item item
 * 
 * @author apple
 * 
 */
public class BrokerStatsTool extends ShellTool {

    public BrokerStatsTool(PrintStream out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new BrokerStatsTool(System.out).doMain(args);
    }


    public BrokerStatsTool(PrintWriter out) {
        super(out);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine = getCommandLine(args);

        String item = commandLine.getOptionValue("item", null);
        String configFile = commandLine.getOptionValue("config");

        Properties props = ResourceUtils.getResourceAsProperties(configFile);
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        String host = props.getProperty("hostName");
        if (StringUtils.isBlank(host)) {
            host = "localhost";
        }
        String serverUrl = "meta://" + host + ":" + props.getProperty("serverPort");
        metaClientConfig.setServerUrl(serverUrl);
        metaClientConfig.setZkConfig(initZkConfig(props));
        MetaMessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

        sessionFactory.getRemotingClient().connect(serverUrl);
        sessionFactory.getRemotingClient().awaitReadyInterrupt(serverUrl);

        BooleanCommand resp =
                (BooleanCommand) sessionFactory.getRemotingClient().invokeToGroup(serverUrl, new StatsCommand(0, item));
        if (resp != null) {
            System.out.println(resp.getErrorMsg());
        }
        sessionFactory.shutdown();
    }


    private static ZKConfig initZkConfig(Properties serverProperties) throws IOException {
        final String zkConnect = serverProperties.getProperty("zk.zkConnect");
        final String zkRoot = serverProperties.getProperty("zk.zkRoot");
        if (!StringUtil.empty(zkConnect)) {
            final int zkSessionTimeoutMs = Integer.parseInt(serverProperties.getProperty("zk.zkSessionTimeoutMs"));
            final int zkConnectionTimeoutMs =
                    Integer.parseInt(serverProperties.getProperty("zk.zkConnectionTimeoutMs"));
            final int zkSyncTimeMs = Integer.parseInt(serverProperties.getProperty("zk.zkSyncTimeMs"));
            return setZkRoot(new ZKConfig(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, zkSyncTimeMs), zkRoot);
        }
        else {
            return null;
        }
    }


    private static ZKConfig setZkRoot(final ZKConfig zkConfig2, final String zkRoot) {
        if (StringUtils.isNotBlank(zkRoot)) {
            zkConfig2.zkRoot = zkRoot;
        }
        return zkConfig2;
    }


    private CommandLine getCommandLine(String[] args) {
        Option itemOpt = new Option("item", true, "stats item");
        itemOpt.setRequired(false);
        Option configOpt = new Option("config", true, "start partition num");
        configOpt.setRequired(true);
        return CommandLineUtils.parseCmdLine(args, new Options().addOption(itemOpt).addOption(configOpt));
    }
}
