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
package com.taobao.metamorphosis.tools.shell;

import java.io.PrintStream;
import java.io.PrintWriter;


/**
 * 代表各种小工具
 * 
 * @author 无花
 * @since 2011-8-23 下午3:44:25
 */

public abstract class ShellTool {

    protected PrintWriter out;

    final static protected String METACONFIG_NAME = "com.taobao.metamorphosis.server.utils:type=MetaConfig,*";
    public ShellTool(PrintWriter out) {
        this.out = out;
    }


    public ShellTool(PrintStream out) {
        this.out = new PrintWriter(out);
    }


    /** 主功能入口 */
    abstract public void doMain(String[] args) throws Exception;


    protected void println(String x) {
        if (this.out != null) {
            this.out.println(x);
            this.out.flush();
        }
        else {
            System.out.println(x);
        }
    }

}