/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.tools.query;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO.QueryType;
import com.taobao.metamorphosis.tools.utils.StringUtil;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ResourceUtils;
import com.taobao.metamorphosis.utils.ZkUtils.StringSerializer;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * offset查询参数
 * 
 * @author pingwei
 */
public class Query {
    ZkClient zkClient = null;
    Connection connect = null;

    ZkOffsetStorageQuery zkQuery = null;
    MysqlOffsetStorageQuery mysqlQuery = null;

    // DiamondManager diamondManager;
    ZKConfig zkConfig;
    MetaZookeeper metaZookeeper;
    private String brokerId;
    static final Log log = LogFactory.getLog(Query.class);


    public Query() {
    }


    public String queryOffset(final OffsetQueryDO queryDO) {
        normalizeQuery(queryDO);
        final OffsetStorageQuery query = this.chooseQuery(queryDO.getType());
        return query.getOffset(queryDO);
    }


    private void normalizeQuery(final OffsetQueryDO queryDO) {
        String partition = queryDO.getPartition();
        if (partition.indexOf("-") < 0) {
            queryDO.setPartition(this.brokerId + "-" + partition);
        }
    }


    private OffsetStorageQuery chooseQuery(final QueryType type) {
        OffsetStorageQuery query = null;
        if (type == QueryType.zk) {
            if (this.zkClient == null) {
                System.out.println("there is no zkClient connect for query offset.");
            }
            query = this.zkQuery;
        }
        else if (type == QueryType.mysql) {
            if (this.connect == null) {
                System.out.println("there is no mysql connect for query offset");
            }
            query = this.mysqlQuery;
        }
        return query;
    }


    public void init(final String serverConf, final String jdbcConf) throws InitException {
        if (!StringUtil.empty(serverConf)) {
            try {
                this.zkConfig = this.initZkConfig(serverConf);
                this.zkClient = this.newZkClient(this.zkConfig);
                this.metaZookeeper = new MetaZookeeper(zkClient, zkConfig.zkRoot);
                this.zkQuery = new ZkOffsetStorageQuery(this.zkClient, metaZookeeper);
            }
            catch (final IOException e) {
                throw new InitException("初始化zk客户端失败", e);
            }
        }
        if (!StringUtil.empty(jdbcConf)) {
            this.initMysqlClient(jdbcConf);
            this.mysqlQuery = new MysqlOffsetStorageQuery(this.connect);
        }

    }


    private void initMysqlClient(final String jdbcConf) throws InitException {
        try {
            final Properties jdbcProperties = ResourceUtils.getResourceAsProperties(jdbcConf);

            final String url = jdbcProperties.getProperty("jdbc.url");
            final String userName = jdbcProperties.getProperty("jdbc.username");
            final String userPassword = jdbcProperties.getProperty("jdbc.password");
            final String jdbcUrl = url + "&user=" + userName + "&password=" + userPassword;
            System.out.println("mysql connect parameter is :\njdbc.url=" + jdbcUrl);
            Class.forName("com.mysql.jdbc.Driver");
            this.connect = DriverManager.getConnection(jdbcUrl);
        }
        catch (final FileNotFoundException e) {
            throw new InitException(e.getMessage(), e.getCause());
        }
        catch (final Exception e) {
            throw new InitException("mysql connect init failed. " + e.getMessage(), e.getCause());
        }
    }


    private ZkClient newZkClient(final ZKConfig zkConfig) throws InitException {
        return new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs,
            new StringSerializer());
    }


    private ZKConfig initZkConfig(final String serverConf) throws IOException {
        final Properties serverProperties =
                com.taobao.metamorphosis.utils.Utils.getResourceAsProperties(serverConf, "GBK");
        brokerId = serverProperties.getProperty("brokerId");
        final String zkConnect = serverProperties.getProperty("zk.zkConnect");
        final String zkRoot = serverProperties.getProperty("zk.zkRoot");
        if (!StringUtil.empty(zkConnect)) {
            final int zkSessionTimeoutMs = Integer.parseInt(serverProperties.getProperty("zk.zkSessionTimeoutMs"));
            final int zkConnectionTimeoutMs =
                    Integer.parseInt(serverProperties.getProperty("zk.zkConnectionTimeoutMs"));
            final int zkSyncTimeMs = Integer.parseInt(serverProperties.getProperty("zk.zkSyncTimeMs"));
            return this.setZkRoot(new ZKConfig(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, zkSyncTimeMs),
                zkRoot);
        }
        else {
            return null;
        }
    }


    private ZKConfig setZkRoot(final ZKConfig zkConfig2, final String zkRoot) {
        if (StringUtils.isNotBlank(zkRoot)) {
            zkConfig2.zkRoot = zkRoot;
        }
        return zkConfig2;
    }


    public ZKConfig getZkConfig(final String serverConf) throws IOException {
        return this.zkConfig != null ? this.zkConfig : this.initZkConfig(serverConf);
    }


    // --------add by wuhua below---------
    public List<String/* group */> getConsumerGroups(final QueryType type) {
        return this.chooseQuery(type).getConsumerGroups();
    }


    public List<String> getTopicsExistOffset(final String group, final QueryType type) {
        return this.chooseQuery(type).getTopicsExistOffset(group);
    }


    public List<String> getPartitionsOf(final String group, final String topic, final QueryType type) {
        return this.chooseQuery(type).getPartitionsOf(group, topic);
    }


    public String getOffsetPath(final String group, final String topic, final Partition partition) {
        return metaZookeeper.new ZKGroupTopicDirs(topic, group).consumerOffsetDir + "/" + partition.toString();
    }


    public ZkClient getZkClient() {
        return this.zkClient;
    }


    public void close() {
        if (this.zkClient != null) {
            this.zkClient.close();
        }
        // if (this.diamondManager != null) {
        // this.diamondManager.close();
        // this.diamondManager = null;
        // }
    }
}