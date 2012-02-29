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
package com.taobao.metamorphosis.tools.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-8-23 ÉÏÎç11:01:10
 */

public class CommandLineUtils {

    public static CommandLine parseCmdLine(String[] args, Options options) {
        HelpFormatter hf = new HelpFormatter();
        try {
            return new PosixParser().parse(options, args);
        }
        catch (ParseException e) {
            hf.printHelp("className", options, true);
            throw new RuntimeException("Parse command line failed", e);
        }
    }
}