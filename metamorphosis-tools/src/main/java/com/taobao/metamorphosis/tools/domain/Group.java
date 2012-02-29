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
package com.taobao.metamorphosis.tools.domain;

import java.util.List;

public class Group {
	private String group;
	private List<String> topicList;
	private List<String> wwList;
	private List<String> mobileList;
	private List<String> ignoreTopicList;
	
	public List<String> getIgnoreTopicList() {
		return ignoreTopicList;
	}
	public void setIgnoreTopicList(List<String> ignoreTopicList) {
		this.ignoreTopicList = ignoreTopicList;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public List<String> getWwList() {
		return wwList;
	}
	public void setWwList(List<String> wwList) {
		this.wwList = wwList;
	}
	public List<String> getMobileList() {
		return mobileList;
	}
	public void setMobileList(List<String> mobileList) {
		this.mobileList = mobileList;
	}
	public List<String> getTopicList() {
		return topicList;
	}
	public void setTopicList(List<String> topicList) {
		this.topicList = topicList;
	}
}