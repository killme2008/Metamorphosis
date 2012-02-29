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
package com.taobao.metamorphosis.tools.monitor.offsetcompareprob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.domain.Group;
import com.taobao.metamorphosis.tools.domain.MetaServer;
import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.AbstractProber;
import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ProbTask;
import com.taobao.metamorphosis.tools.monitor.core.StatsResult;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO.QueryType;
import com.taobao.metamorphosis.tools.query.Query;
import com.taobao.metamorphosis.tools.query.ZkOffsetStorageQuery;
import com.taobao.metamorphosis.utils.Utils;
import com.taobao.metamorphosis.utils.Utils.Action;

public class OffsetCompareProber extends AbstractProber {
	private final static Logger logger = Logger
			.getLogger(OffsetCompareProber.class);
	private Query query;
	private final List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>();
//	private Map<String, Long> serverMaxOffsetMap = new HashMap<String, Long>();
	private final static String serverBigerFormat = "group:[%s]上的topic:[%S]的partition[%S]在metaServer:[%s]的offset[%S]比ZK上的offset:[%S]大[%S]";
	private final static String zkBigerFormat = "group:[%s]上的topic:[%S]的partition[%S]在ZK上的offset:[%S]比metaServer:[%s]的offset[%S]大[%s]";
	
	public OffsetCompareProber(CoreManager coreManager) {
		super(coreManager);
	}

	public void init() throws InitException {
		this.query = new Query();
		
		this.query.init(this.getMonitorConfig().getConfigPath(), null);

	}

	@Override
	protected void doStopProb() {
		cancelFutures(this.futures);
	}

	@Override
	protected void doProb() throws InterruptedException {
		this.futures.add(this.getProberExecutor().scheduleWithFixedDelay(
				new ProbTask() {

					@Override
					protected void doExecute() throws Exception {
						if (logger.isDebugEnabled()) {
							logger.debug("offset prob...");
						}
						OffsetCompareProber.this.probOnce();
					}

					@Override
					protected void handleException(Throwable e) {
						logger.error("unexpected error in offset prob thread.",
								e);
					}

				}, 0, this.getMonitorConfig().getOffsetProbCycleTime(),
				TimeUnit.HOURS));
//		 }, 0, this.getMonitorConfig().getOffsetProbCycleTime()*1000*5,
//		 TimeUnit.MILLISECONDS));
		logger.debug("offset prob started");

	}

	protected void probOnce() {
//		serverMaxOffsetMap.clear();
		List<String> consumerGroups = this.query
				.getConsumerGroups(QueryType.zk);
		for (String group : consumerGroups) {
			List<String> topicsList = this.query.getTopicsExistOffset(group,
					QueryType.zk);
			for (String topic : topicsList) {
				List<String> partitions = this.query.getPartitionsOf(group,
						topic, QueryType.zk);
				for (String partition : partitions) {
					// brokeId+topic组成的key
					String key = partition+"_"+topic;
					String serverUrl = getServerUrl(partition);
					long zkOffset = ZkOffsetStorageQuery.parseOffsetAsLong(this.query.queryOffset(new OffsetQueryDO(topic, group, partition, QueryType.zk.toString())));
					long serverMaxOffset;
//					if (null == serverMaxOffsetMap.get(key)) {
//						从server端获取offset的值
						
//						serverMaxOffset = getServerMaxOffset(serverUrl,topic,partition,serverMaxOffsetMap);
						serverMaxOffset = getServerMaxOffset(serverUrl,topic,partition);
//						serverMaxOffsetMap.put(key, serverMaxOffset);
//					} else {
//						serverMaxOffset = serverMaxOffsetMap.get(key);
//					}
					// 对比offset，并报警
					int offsetMaxGap = getOffsetMaxGap(topic);
					int zkMaxOverStep = this.getMonitorConfig().getZkMaxOverStep();
					String serverInfo = getServerInfo(partition);
					if (serverMaxOffset!=-1&&zkOffset-serverMaxOffset>zkMaxOverStep) {
						String msg = String.format(zkBigerFormat, group,topic,partition,zkOffset,serverInfo,serverMaxOffset,zkOffset-serverMaxOffset);
						alertMsg(group, topic, msg);
					}else if (serverMaxOffset!=-1&&serverMaxOffset - zkOffset > offsetMaxGap) {
						String msg = String.format(serverBigerFormat, group,topic,partition,serverInfo,serverMaxOffset,zkOffset,serverMaxOffset - zkOffset);
						alertMsg(group, topic, msg);
					}

				}
			}
		}
	}

	private void alertMsg(String group, String topic, String msg) {
		logger.warn(msg);
		//实现topic在莫个group下不报警的功能：ignoreTopic
		List<String> wwList = new ArrayList<String>();
		List<String> mobileList = new ArrayList<String>();
		List<String> defaultWWList = this.getMonitorConfig()
				.getWangwangList();
		for (String ww : defaultWWList) {
			if (!wwList.contains(ww)) {
				wwList.add(ww);
			}
		}
		
		this.findAlertList(this
				.getMonitorConfig().getGroupList(), "ww",
				topic,group,wwList);
		this.findAlertList(this
				.getMonitorConfig().getGroupList(), "mobile",
				topic,group,mobileList);

		logger.warn("alart to[" + wwList + "]mobiles["
				+ mobileList + "]");
		Alarm.start().wangwangs(wwList).mobiles(mobileList)
				.alert(msg);
	}

	private int getOffsetMaxGap(String topic) {
		if(this.getMonitorConfig().getTopicOffsetMaxGapMap().containsKey(topic)){
			return this.getMonitorConfig().getTopicOffsetMaxGapMap().get(topic);
		}else{
			return this.getMonitorConfig().getOffsetMaxGap();
		}
	}

	private long getServerMaxOffset(String serverUrl, final String topic,String partition) {
		final List<Long> serverMaxOffsetList = new ArrayList<Long>();
//		String key=partition+"_"+topic;
		final String partitionId = partition.substring(partition.indexOf("-")+1, partition.length());
		MsgSender msgSender = this.coreManager.getSender(serverUrl);
//		final String brokerId = partition.substring(0,partition.indexOf("-"));
        StatsResult ret = msgSender.getStats("offsets", 5000);
        if (ret.isSuccess()) {
        	try {
                Utils.processEachLine(ret.getStatsInfo(), new Action() {
                	@Override
                    public void process(String line) {
                        String[] tmp = StringUtils.splitByWholeSeparator(line, " ");
                        if (tmp != null && tmp.length == 7) {
                            if(topic.equals(tmp[0])&&partitionId.equals(tmp[2])){
                        	  	serverMaxOffsetList.add(Long.parseLong(tmp[6]));
                            	return;
                            }
                        }
                    }
                });
            }
            catch (IOException e) {
                logger.error("IOException",e);
            }
        }
        if(null!=serverMaxOffsetList&&serverMaxOffsetList.size()>0){
        	return serverMaxOffsetList.get(0);
        }else{
        	return -1;
        }
        
	}

	private String getServerInfo(String partition) {
		int brokeId = Integer.parseInt(partition.substring(0,
				partition.indexOf("-")));
		List<MetaServer> metaServerList = this.getMonitorConfig()
				.getMetaServerList();
		for (MetaServer metaServer : metaServerList) {
			if (metaServer.getBrokeId() == brokeId) {
				return metaServer.getHostIp() + ","+ metaServer.getHostName();
			}
		}
		return "未知brokeId";
	}
	
	private String getServerUrl(String partition) {
		int brokeId = Integer.parseInt(partition.substring(0,
				partition.indexOf("-")));
		List<MetaServer> metaServerList = this.getMonitorConfig()
				.getMetaServerList();
		for (MetaServer metaServer : metaServerList) {
			if (metaServer.getBrokeId() == brokeId) {
				return metaServer.getUrl();
			}
		}
		return "未知brokeId";
	}
	

	public void findAlertList(List<Group> groupList, String alertKind,
			String topic, String groupStr, List<String>alertList) {
		if (groupList == null || groupList.isEmpty()) {
			return ;
		}
		
		for (Group group : groupList) {
			if(group.getGroup().equals(groupStr)){
				for(String ignoreTopic : group.getIgnoreTopicList()){
					if(ignoreTopic.equals(topic)){
						alertList.clear();
						return;
					}
				}
				for(String filterTopic : group.getTopicList()){
					if(filterTopic.equals(topic)){
						if("ww".equals(alertKind)){
							for(String ww:group.getWwList()){
								if(!alertList.contains(ww)){
									alertList.add(ww);
								}
							}
						}else{
							for(String mobile:group.getMobileList()){
								if(!alertList.contains(mobile)){
									alertList.add(mobile);
								}
							}
						}
						return;
					}
				}
			}
		}
	}
}