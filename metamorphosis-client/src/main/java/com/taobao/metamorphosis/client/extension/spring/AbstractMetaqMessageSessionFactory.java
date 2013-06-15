package com.taobao.metamorphosis.client.extension.spring;

import java.util.Properties;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * Base factory bean class to create message sessionfactory.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * @param <T>
 */
public abstract class AbstractMetaqMessageSessionFactory<T extends MessageSessionFactory> implements FactoryBean,
DisposableBean {
    protected MetaClientConfig metaClientConfig = new MetaClientConfig();

    {
        this.metaClientConfig.setZkConfig(new ZKConfig());
    }

    protected T sessionFactory;


    @Override
    public void destroy() throws Exception {
        if (this.sessionFactory != null) {
            this.sessionFactory.shutdown();
        }
    }


    public int getRecoverThreadCount() {
        return this.metaClientConfig.getRecoverThreadCount();
    }


    public void setRecoverThreadCount(int recoverThreadCount) {
        this.metaClientConfig.setRecoverThreadCount(recoverThreadCount);
    }


    public long getRecoverMessageIntervalInMills() {
        return this.metaClientConfig.getRecoverMessageIntervalInMills();
    }


    public void setRecoverMessageIntervalInMills(long recoverMessageIntervalInMills) {
        this.metaClientConfig.setRecoverMessageIntervalInMills(recoverMessageIntervalInMills);
    }


    public ZKConfig getZkConfig() {
        return this.metaClientConfig.getZkConfig();
    }


    public void setZkConfig(ZKConfig zkConfig) {
        this.metaClientConfig.setZkConfig(zkConfig);
    }


    public String getServerUrl() {
        return this.metaClientConfig.getServerUrl();
    }


    public void setServerUrl(String serverUrl) {
        this.metaClientConfig.setServerUrl(serverUrl);
    }


    public void setPartitionsInfo(Properties partitionsInfo) {
        this.metaClientConfig.setPartitionsInfo(partitionsInfo);
    }


    public Properties getPartitionsInfo() {
        return this.metaClientConfig.getPartitionsInfo();
    }


    public MetaClientConfig getMetaClientConfig() {
        return this.metaClientConfig;
    }


    public void setMetaClientConfig(MetaClientConfig metaClientConfig) {
        this.metaClientConfig = metaClientConfig;
    }


    public String getZkRoot() {
        return this.metaClientConfig.getZkRoot();
    }


    public void setZkRoot(String zkRoot) {
        this.metaClientConfig.setZkRoot(zkRoot);
    }


    public boolean isZkEnable() {
        return this.metaClientConfig.isZkEnable();
    }


    public void setZkEnable(boolean zkEnable) {
        this.metaClientConfig.setZkEnable(zkEnable);
    }


    public String getZkConnect() {
        return this.metaClientConfig.getZkConnect();
    }


    public void setZkConnect(String zkConnect) {
        this.metaClientConfig.setZkConnect(zkConnect);
    }


    public int getZkSessionTimeoutMs() {
        return this.metaClientConfig.getZkSessionTimeoutMs();
    }


    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.metaClientConfig.setZkSessionTimeoutMs(zkSessionTimeoutMs);
    }


    public int getZkConnectionTimeoutMs() {
        return this.metaClientConfig.getZkConnectionTimeoutMs();
    }


    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.metaClientConfig.setZkConnectionTimeoutMs(zkConnectionTimeoutMs);
    }


    public int getZkSyncTimeMs() {
        return this.metaClientConfig.getZkSyncTimeMs();
    }


    public void setZkSyncTimeMs(int zkSyncTimeMs) {
        this.metaClientConfig.setZkSyncTimeMs(zkSyncTimeMs);
    }


    @Override
    public boolean isSingleton() {
        return true;
    }

}
