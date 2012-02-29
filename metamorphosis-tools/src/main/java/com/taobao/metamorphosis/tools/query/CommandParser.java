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

import java.util.HashMap;
import java.util.Map;

public class CommandParser {
	Query query;

	public CommandParser(Query query) {
		this.query = query;
	}

	public int prase(String command) {
		String[] args = command.split(" ");
		try {
			String task = args[0];
			if ("help".equals(task)) {
				printHelp();
			} else if("quit".equals(task)){
				return -1;
			} else if ("showoffset".equals(task)) {

				Map<String, String> map = new HashMap<String, String>();
				if (args != null && args.length > 3) {
					for (int i = 1; i < args.length; i++) {
						String param = args[i];
						String[] tmp = param.split("=");
						if (tmp.length == 2) {
							map.put(tmp[0], tmp[1]);
						}
					}
				}
				String group = map.get("group");
				if (group == null) {
					group = map.get("g");
				}
				String topic = map.get("topic");
				if (topic == null) {
					topic = map.get("t");
				}
				String partition = map.get("partition");
				if (partition == null) {
					partition = map.get("p");
				}
				String type = map.get("type");
				if (type == null) {
					type = "zk";
				}
				OffsetQueryDO queryDO = new OffsetQueryDO(topic, group, partition, type);
				System.out.println(query.queryOffset(queryDO));
			} else {
				System.out.println("illegal  command.");
				printHelp();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 1;
	}
	
	private void printHelp(){
		System.out.println("##############################");
		System.out.println("showoffset topic=${topic} group=${group} partition=${partition} type=${type} \nor\nshowoffset t=${topic} g=${group} p=${partition} type=${type}");
		System.out.println("##############################");
	}

	public static void main(String[] args) {
		CommandParser p = new CommandParser(null);
		p.prase("-so g=g1 topic=t1 p=0-1");
	}
}