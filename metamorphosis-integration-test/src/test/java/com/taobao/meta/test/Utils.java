package com.taobao.meta.test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;

import com.taobao.metamorphosis.EnhancedBroker;
import com.taobao.metamorphosis.client.Shutdownable;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-12 ÉÏÎç10:46:30
 */

public class Utils {

    public static final String diamondZKDataId = "metamorphosis.integrationTestZkConfig";


    public static void clearDataDir(MetaConfig metaConfig) throws IOException {
        File dataDir = new File(metaConfig.getDataPath());
        System.out.println("delete:" + dataDir);
        FileUtils.deleteDirectory(dataDir);
    }


    public static void clearConsumerInfoInZk(ZkClient zkClient, MetaZookeeper metaZookeeper) throws Exception {
        ZkUtils.deletePathRecursive(zkClient, metaZookeeper.consumersPath);
        Thread.sleep(5000);
    }


    public static void stopServers(Collection<MetaMorphosisBroker> brokers) {
        for (MetaMorphosisBroker broker : brokers) {
            if (broker == null) {
                continue;
            }
            broker.stop();
        }
    }


    public static void stopWrapperServers(Collection<EnhancedBroker> brokers) {
        for (EnhancedBroker broker : brokers) {
            if (broker == null) {
                continue;
            }
            broker.stop();
        }
    }


    public static int shutdown(Collection<? extends Shutdownable> collection) throws MetaClientException {
        int count = 0;
        if (collection != null && !collection.isEmpty()) {
            for (Shutdownable shutdownable : collection) {
                count += shutdown(shutdownable);
            }
        }
        return count;
    }


    public static int shutdown(Shutdownable shutdownable) throws MetaClientException {
        if (shutdownable != null) {
            shutdownable.shutdown();
            return 1;
        }
        return 0;
    }


    public static byte[] getData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 127);
        }
        return data;

    }
}
