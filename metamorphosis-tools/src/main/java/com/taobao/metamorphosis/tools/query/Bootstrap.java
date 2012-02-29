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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.taobao.metamorphosis.tools.monitor.InitException;

public class Bootstrap {

	static Options option = null;
	static BasicParser p = null;

	static {
		option = new Options();
		p = new BasicParser();
		option.addOption("s", true, "server.properties file path");
		option.addOption("j", true, "jdbc.properties file path");
	}

	public static void main(String[] args) {
		try {
			CommandLine cl = p.parse(option, args);
			String serverConf = null;
			String jdbcConf = null;
			if(cl.hasOption("s")){
				serverConf = cl.getOptionValue("s");
			} else {
				System.out.println("[error] has no server.properties file, start up failed");
				return ;
			}
			if(cl.hasOption("j")){
				jdbcConf = cl.getOptionValue("j");
			} else {
				System.out.println("[info]has no jdbc.properties file, can not query offset value from mysql. ");
			}
			Query query = new Query();
			try {
				query.init(serverConf, jdbcConf);
				ConsoleThread console = new ConsoleThread(query);
				console.start();
			} catch (InitException e) {
				System.out.println(e.getMessage());
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
	
}