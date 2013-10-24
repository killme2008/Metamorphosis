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
package com.taobao.metamorphosis.tools.monitor.core;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

import com.googlecode.aviator.AviatorEvaluator;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.tools.domain.Group;
import com.taobao.metamorphosis.tools.domain.MetaServer;
import com.taobao.metamorphosis.utils.Utils;


/**
 * @author 无花
 * @since 2011-5-24 上午11:19:13
 */

public class MonitorConfig {
    private static Logger logger = Logger.getLogger(MonitorConfig.class);

    public static final String DEFAULT_CONFIG_FILE = "monitor.properties";
    public static final String DYNAMIC_CONFIG_FILE = "dynamic.properties";
    public static final String DEFAULT_CONFIG_INI = "monitor.ini";

//    private List<String> serverUrlList = new ArrayList<String>();

    private List<String> wangwangList = Collections.emptyList();

    private List<String> mobileList = Collections.emptyList();
    public List<Group> getGroupList() {
		return this.groupList;
	}


	public List<MetaServer> getMetaServerList() {
		return this.metaServerList;
	}

	private List<Group> groupList = Collections.emptyList();
    private List<MetaServer> metaServerList = Collections.emptyList();
//
//    private final Map<GroupTopicPair, List<String>> groupAlertWW =
//            new ConcurrentHashMap<GroupTopicPair, List<String>>();
//    private final Map<GroupTopicPair, List<String>> groupAlertMobile =
//            new ConcurrentHashMap<GroupTopicPair, List<String>>();

    private long sendTimeout = 10000L;

    private long receiveTimeout = 10000L;

    private long probeInterval = 15000L;

    private long msgProbeCycleTime = 5000L;

    private long statsProbCycleTime = 12 * 60 * 1000;

    private int systemProbCycleTime = 2;// 单位为分钟
    private final Set<String> filterTopicList = new HashSet<String>();

    

	private long realtimePutFailThreshold = 10;

    private int offsetProbCycleTime = 6;
    private int offsetNoChangeTimeThreshold = 6;
    private String configPath;
    private long lastModified = -1;

    private int monitorPoolCoreSize = 20;

    private String loginUser;

    private String loginPassword;

    private int cpuLoadThreshold = 8;
    private int diskUsedThreshold = 85;

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);

    private int jmxPort = 9999;

    /** 每个meta客户端Ip连接数阀值 */
    private int metaConnectionPerIpThreshold = 5;

    /** meta服务器连接总数阀值 */
    private int metaConnectionThreshold = 150;

    private int preparedTransactionCountThreshold = 10;
    
    private int offsetMaxGap = 0;
    private int zkMaxOverStep = 0;
    
	private Map<String, Integer> topicOffsetMaxGapMap = new HashMap<String, Integer>();

	//add by liuqingjie.pt
    public void loadInis(String resource) throws IOException {
        this.configPath = resource;
        this.isConfigFileChanged(true);
        File iniFile = Utils.getResourceAsFile(resource);
        final Ini iniConfig = new Ini();
        //ini4j解析“/”时，把其当做分隔字符，编码更改分隔字符
        iniConfig.getConfig().setPathSeparator('+');
        iniConfig.getConfig().setFileEncoding(Charset.forName("GBK"));
		iniConfig.load(iniFile);
        this.populateConfig(iniConfig);
    }
    

    private void populateConfig(Ini iniConfig) {
		this.populateSystemConfig(iniConfig);
		this.populateServerConfig(iniConfig);
		this.populateGroupConfig(iniConfig);
		this.populateFilterTopicConfig(iniConfig);
	}


	private void populateFilterTopicConfig(Ini iniConfig) {
		final Section filterTopicConf = iniConfig.get("filterTopic");
		if(!StringUtils.isBlank(filterTopicConf.get("topic"))){
			this.filterTopicList.addAll(Arrays.asList(filterTopicConf.get("topic").split(",")));
		}
	}


	private void populateGroupConfig(Ini iniConfig) {
		final Set<String> set = iniConfig.keySet();
		List<Group> newGroupList = new LinkedList<Group>();
        for (final String name : set) {
            if (name != null && name.startsWith("group=")) {
            	final Section section = iniConfig.get(name);
            	Group group = new Group();
            	group.setGroup(name.substring("group=".length()));
                group.setMobileList(Arrays.asList(section.get("mobile").split(",")));
                group.setTopicList(Arrays.asList(section.get("topic").split(",")));
                group.setWwList(Arrays.asList(section.get("ww").split(",")));
                if(!StringUtils.isBlank(section.get("ignoreTopic"))){
                	group.setIgnoreTopicList(Arrays.asList(section.get("ignoreTopic").split(",")));	
                }
                newGroupList.add(group);
            }
        }
        if(!this.groupList.equals(newGroupList)){
        	this.groupList.clear();
        	this.groupList = newGroupList;
        }
	}


	private void populateServerConfig(Ini iniConfig) {
		
		final Set<String> set = iniConfig.keySet();
		List<String> newServerUrlList = new LinkedList<String>();
		List<MetaServer> newMetaServerList = new LinkedList<MetaServer>();
        for (final String name : set) {
            if (name != null && name.startsWith("server=")) {
            	final Section section = iniConfig.get(name);
            	MetaServer metaServer = new MetaServer();
            	metaServer.setUrl(name.substring("server=".length()));
            	newServerUrlList.add(metaServer.getUrl());
            	metaServer.setCluster(section.get("cluster"));
            	metaServer.setHostIp(section.get("hostIp"));
            	metaServer.setHostName(section.get("hostName"));
            	metaServer.setJmxPort(this.getInt(section, "jmxPort"));
            	metaServer.setServerPort(this.getInt(section,"serverPort"));
            	metaServer.setStatus(this.getInt(section,"status"));
            	metaServer.setBrokeId(this.getInt(section,"brokeId"));
            	newMetaServerList.add(metaServer);
            }
        }
        if(!this.metaServerList.equals(newMetaServerList)){
        	this.metaServerList.clear();
        	this.metaServerList = newMetaServerList;
        	//构造serverUrlList serverUrlList
//    		serverUrlList.clear();
//    		serverUrlList = newServerUrlList;
        }
        
	}


	private void populateSystemConfig(Ini iniConfig) {
		final Section sysConf = iniConfig.get("system");
		
		if (!StringUtils.isBlank(sysConf.get("wangwangList"))) {
            this.wangwangList = Arrays.asList(StringUtils.split(sysConf.get("wangwangList"), ","));
        }else{
        	this.wangwangList = Collections.emptyList(); 
        }

        if (!StringUtils.isBlank(sysConf.get("mobileList"))) {
            this.mobileList = Arrays.asList(StringUtils.split(sysConf.get("mobileList"), ","));
        }else{
        	this.mobileList = Collections.emptyList(); 
        }

        if (!StringUtils.isBlank(sysConf.get("sendTimeout"))) {
            this.sendTimeout = Integer.parseInt(sysConf.get("sendTimeout"));
        }

        if (!StringUtils.isBlank(sysConf.get("receiveTimeout"))) {
            this.receiveTimeout = Integer.parseInt(sysConf.get("receiveTimeout"));
        }

        if (!StringUtils.isBlank(sysConf.get("realtimePutFailThreshold"))) {
            this.realtimePutFailThreshold = Integer.parseInt(sysConf.get("realtimePutFailThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("probeInterval"))) {
            this.probeInterval = Integer.parseInt(sysConf.get("probeInterval"));
        }

        if (!StringUtils.isBlank(sysConf.get("msgProbeCycleTime"))) {
            this.msgProbeCycleTime = Integer.parseInt(sysConf.get("msgProbeCycleTime"));
        }

        if (!StringUtils.isBlank(sysConf.get("statsProbCycleTime"))) {
            this.statsProbCycleTime = Integer.parseInt(sysConf.get("statsProbCycleTime"));
        }

        if (!StringUtils.isBlank(sysConf.get("offsetProbCycleTime"))) {
            this.offsetProbCycleTime = Integer.parseInt(sysConf.get("offsetProbCycleTime"));
        }

        if (!StringUtils.isBlank(sysConf.get("systemProbCycleTime"))) {
            this.systemProbCycleTime = Integer.parseInt(sysConf.get("systemProbCycleTime"));
        }

        if (!StringUtils.isBlank(sysConf.get("monitorPoolCoreSize"))) {
            this.monitorPoolCoreSize = Integer.parseInt(sysConf.get("monitorPoolCoreSize"));
        }
        if (!StringUtils.isBlank(sysConf.get("offsetNoChangeTimeThreshold"))) {
            this.offsetNoChangeTimeThreshold = Integer.parseInt(sysConf.get("offsetNoChangeTimeThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("cpuLoadThreshold"))) {
            this.cpuLoadThreshold = Integer.parseInt(sysConf.get("cpuLoadThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("diskUsedThreshold"))) {
            this.diskUsedThreshold = Integer.parseInt(sysConf.get("diskUsedThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("metaConnectionPerIpThreshold"))) {
            this.metaConnectionPerIpThreshold = Integer.parseInt(sysConf.get("metaConnectionPerIpThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("metaConnectionThreshold"))) {
            this.metaConnectionThreshold = Integer.parseInt(sysConf.get("metaConnectionThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("preparedTransactionCountThreshold"))) {
            this.preparedTransactionCountThreshold =
                    Integer.parseInt(sysConf.get("preparedTransactionCountThreshold"));
        }

        if (!StringUtils.isBlank(sysConf.get("jmxPort"))) {
            this.jmxPort = Integer.parseInt(sysConf.get("jmxPort"));
        }

        if (!StringUtils.isBlank(sysConf.get("loginUser"))) {
            this.loginUser = sysConf.get("loginUser");
        }

        if (!StringUtils.isBlank(sysConf.get("loginPassword"))) {
            this.loginPassword = sysConf.get("loginPassword");
        }
        if (!StringUtils.isBlank(sysConf.get("offsetMaxGap"))) {
            this.offsetMaxGap = Integer.parseInt(sysConf.get("offsetMaxGap"));
        }
        if (!StringUtils.isBlank(sysConf.get("zkMaxOverStep"))) {
            this.zkMaxOverStep = Integer.parseInt(sysConf.get("zkMaxOverStep"));
        }

        if (!StringUtils.isBlank(sysConf.get("topicOffsetMaxGap"))) {
            Map<String, Integer> newTopicOffsetMaxGapMap = new HashMap<String, Integer>();
            String topicOffsetMaxGap = sysConf.get("topicOffsetMaxGap");
            String[] topicOffsetMaxGapArr = topicOffsetMaxGap.split(",");
            for (int i = 0; i < topicOffsetMaxGapArr.length; i++) {
				String offsetItem = topicOffsetMaxGapArr[i];
				String[] offsetItemArr = offsetItem.split(":");
				newTopicOffsetMaxGapMap.put(offsetItemArr[0], Integer.parseInt(offsetItemArr[1]));
			}
            if(!this.topicOffsetMaxGapMap.equals(newTopicOffsetMaxGapMap)){
            	this.topicOffsetMaxGapMap.clear();
            	this.topicOffsetMaxGapMap = newTopicOffsetMaxGapMap;
            }
        }
	}

     public static void main(String[] args) throws IOException {
	     MonitorConfig config = new MonitorConfig();
	     config.loadInis("d:/monitor.ini");
     }

    static void processGroupAlert(String property, Map<GroupTopicPair, List<String>> map) throws IOException {
        Map<GroupTopicPair, List<String>> tmpMap = new HashMap<GroupTopicPair, List<String>>();
        try {
            String[] strings = StringUtils.split(property, ";");
            for (String string : strings) {
                String[] tmpArr = StringUtils.split(string, ",");
                List<String> wwList = null;
                GroupTopicPair groupTopicPair = null;
                if (tmpArr.length == 2) {
                    groupTopicPair = new GroupTopicPair(tmpArr[0], null);
                    wwList = parseWW(property, tmpArr[1]);
                }
                else if (tmpArr.length == 3) {
                    groupTopicPair = new GroupTopicPair(tmpArr[0], tmpArr[1]);
                    wwList = parseWW(property, tmpArr[2]);
                }

                if (groupTopicPair != null && wwList != null) {
                    tmpMap.put(groupTopicPair, wwList);
                }
                else {
                    throw new IOException("解析错误:" + property);
                }
            }

            // 当一切都没错误的时候,清空旧数据放入新数据,否则使用旧数据
            if (!tmpMap.isEmpty()) {
                map.clear();
                map.putAll(tmpMap);
            }
        }
        catch (Exception e) {
            throw new IOException("解析配置项出错,maybe将使用旧的数据:" + property, e);
        }
    }


    static List<String> parseWW(String property, String string) throws IOException {
        if (!string.startsWith("[") || !string.endsWith("]")) {
            throw new IOException("解析字符串错误:" + property);
        }
        List<String> list = Arrays.asList(StringUtils.split(string.substring(1, string.length() - 1), "|"));
        return list;
    }


    public MetaClientConfig metaClientConfigOf(String serverUrl) {
        return metaClientConfigOf(serverUrl, this);
    }

    public static class GroupTopicPair {
        private final String group;
        private final String topic;


        public GroupTopicPair(String group, String topic) {
            this.group = group;
            this.topic = topic;
        }


        public String getGroup() {
            return this.group;
        }


        public String getTopic() {
            return this.topic;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof GroupTopicPair) {
                GroupTopicPair that = (GroupTopicPair) obj;
                if (this.group == null || that.group == null) {
                    return this.group == null && that.group == null;
                }
                if (this.topic == null || that.topic == null) {
                    return this.topic == null && that.topic == null;
                }

                return this.group.equals(that.group) && this.topic.equals(that.topic);
            }
            return false;
        }


        @Override
        public int hashCode() {
            return (this.group + this.topic).hashCode();
        }

    }


    static private MetaClientConfig metaClientConfigOf(String serverUrl, MonitorConfig monitorConfig) {
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setServerUrl(serverUrl);
        return metaClientConfig;
    }


//    public List<String> getServerUrlList() {
//        return this.serverUrlList;
//    }
//
//
//    public void setServerUrlList(List<String> serverUrlList) {
//        this.serverUrlList = serverUrlList;
//    }

    public long getSendTimeout() {
        return this.sendTimeout;
    }


    public long getReceiveTimeout() {
        return this.receiveTimeout;
    }


    public long getProbeInterval() {
        return this.probeInterval;
    }


    public void setMsgProbeCycleTime(long msgProbeCycleTime) {
        this.msgProbeCycleTime = msgProbeCycleTime;
    }


    public long getMsgProbeCycleTime() {
        return this.msgProbeCycleTime;
    }


    public List<String> getWangwangList() {
        return this.wangwangList;
    }


    public List<String> getMobileList() {
        return this.mobileList;
    }


    public long getStatsProbCycleTime() {
        return this.statsProbCycleTime;
    }


    public long getRealtimePutFailThreshold() {
        return this.realtimePutFailThreshold;
    }


    public int getOffsetNoChangeTimeThreshold() {
        return this.offsetNoChangeTimeThreshold;
    }


    public int getOffsetProbCycleTime() {
        return this.offsetProbCycleTime;
    }


//    public Map<GroupTopicPair, List<String>> getGroupAlertWW() {
//        return this.groupAlertWW;
//    }
//
//
//    public Map<GroupTopicPair, List<String>> getGroupAlertMobile() {
//        return this.groupAlertMobile;
//    }


    public String getConfigPath() {
        return this.configPath;
    }


    // public void setConfigPath(String configPath) {
    // this.configPath = configPath;
    // }

    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener) {
        this.propertyChangeSupport.addPropertyChangeListener(propertyName, listener);
    }


    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        this.propertyChangeSupport.removePropertyChangeListener(listener);
    }


    /**
     * Reload configuration
     */
    public void reload() {
        try {
            if (this.isConfigFileChanged(true)) {
                logger.info("Reloading configuration......");
//                Properties props = Utils.getResourceAsProperties(this.configPath, "GBK");
//                this.populateConfig(props);
                File iniFile = Utils.getResourceAsFile(this.configPath);
                final Ini iniConfig = new Ini();
                iniConfig.load(iniFile);
                this.populateConfig(iniConfig);
                logger.info(this.toString());
                logger.info("wwList:"+this.getWangwangList().toString());
                logger.info("Reload topics successfully");
            }
            else {
                logger.info("配置文件没发生改变,不需要reload");
            }
        }
        catch (final Exception e) {
            logger.error("Reload config failed", e);
        }
    }


    public boolean isConfigFileChanged() throws IOException {
        return this.isConfigFileChanged(false);
    }


    private boolean isConfigFileChanged(boolean updateLastModified) throws IOException {
        final File file = Utils.getResourceAsFile(this.configPath);
        boolean isChanged = file.lastModified() != this.lastModified;
        if (isChanged && updateLastModified) {
            this.lastModified = file.lastModified();
        }
        return isChanged;
    }


    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }


    public int getMonitorPoolCoreSize() {
        return this.monitorPoolCoreSize;
    }


    public String getLoginUser() {
        return this.loginUser;
    }

    public Set<String> getFilterTopicList() {
		return this.filterTopicList;
	}

    public void setLoginUser(String loginUser) {
        this.loginUser = loginUser;
    }


    public String getLoginPassword() {
        return this.loginPassword;
    }


    public void setLoginPassword(String loginPassword) {
        this.loginPassword = loginPassword;
    }


    public int getSystemProbCycleTime() {
        return this.systemProbCycleTime;
    }


    public void setSystemProbCycleTime(int systemProbCycleTime) {
        this.systemProbCycleTime = systemProbCycleTime;
    }


    public int getCpuLoadThreshold() {
        return this.cpuLoadThreshold;
    }


    public void setCpuLoadThreshold(int cpuLoadThreshold) {
        this.cpuLoadThreshold = cpuLoadThreshold;
    }


    public int getDiskUsedThreshold() {
        return this.diskUsedThreshold;
    }


    public void setDiskUsedThreshold(int diskUsedThreshold) {
        this.diskUsedThreshold = diskUsedThreshold;
    }


    public int getJmxPort() {
        return this.jmxPort;
    }


    public int getMetaConnectionPerIpThreshold() {
        return this.metaConnectionPerIpThreshold;
    }


    public int getMetaConnectionThreshold() {
        return this.metaConnectionThreshold;
    }


    public int getPreparedTransactionCountThreshold() {
        return this.preparedTransactionCountThreshold;
    }
    
    public int getOffsetMaxGap() {
		return this.offsetMaxGap;
	}


	public void setOffsetMaxGap(int offsetMaxGap) {
		this.offsetMaxGap = offsetMaxGap;
	}
	
	public Map<String, Integer> getTopicOffsetMaxGapMap() {
		return this.topicOffsetMaxGapMap;
	}
	public int getZkMaxOverStep() {
		return this.zkMaxOverStep;
	}


	public void setZkMaxOverStep(int zkMaxOverStep) {
		this.zkMaxOverStep = zkMaxOverStep;
	}


	public void setTopicOffsetMaxGapMap(Map<String, Integer> topicOffsetMaxGapMap) {
		this.topicOffsetMaxGapMap = topicOffsetMaxGapMap;
	}
    
    private int getInt(final Section section, final String key) {
        final String value = section.get(key);
        if (StringUtils.isBlank(value)) {
            throw new NullPointerException("Blank value for " + key);
        }
        else {
            final Long rt = (Long) AviatorEvaluator.execute(value);
            return rt.intValue();
        }
    }

}