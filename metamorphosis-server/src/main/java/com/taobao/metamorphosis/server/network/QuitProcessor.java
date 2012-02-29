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
package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.server.CommandProcessor;


/**
 * ÍË³öÃüÁî´¦ÀíÆ÷
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class QuitProcessor implements RequestProcessor<QuitCommand> {
    private final CommandProcessor processor;


    public QuitProcessor(final CommandProcessor processor) {
        super();
        this.processor = processor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }


    @Override
    public void handleRequest(final QuitCommand request, final Connection conn) {
        this.processor.processQuitCommand(request, SessionContextHolder.getOrCreateSessionContext(conn, null));
    }

}