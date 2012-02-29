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
package com.taobao.metamorphosis.gregor.slave;

import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;


/**
 * 同步命令处理接口
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public interface SyncCommandProcessor {
    /**
     * 处理同步命令
     * 
     * @param request
     * @param sessionContext
     * @param cb
     */
    public void processSyncCommand(final SyncCommand request, final SessionContext sessionContext, final PutCallback cb);
}