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
package com.taobao.metamorphosis.server.transaction.store;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore.AddMsgLocation;


/**
 * 添加消息位置的序列化工具类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-22
 * 
 */
public class AddMsgLocationUtils {

    public static ByteBuffer encodeLocation(final Map<String, JournalTransactionStore.AddMsgLocation> locations) {
        int capactity = 0;
        for (final Map.Entry<String, JournalTransactionStore.AddMsgLocation> entry : locations.entrySet()) {
            capactity += entry.getValue().encode().remaining();
        }
        final ByteBuffer buf = ByteBuffer.allocate(capactity);
        for (final Map.Entry<String, JournalTransactionStore.AddMsgLocation> entry : locations.entrySet()) {
            buf.put(entry.getValue().encode());
        }
        buf.flip();
        return buf;
    }


    public static final Map<String, JournalTransactionStore.AddMsgLocation> decodeLocations(final ByteBuffer buf) {

        AddMsgLocation location = null;

        final Map<String, JournalTransactionStore.AddMsgLocation> rt =
                new LinkedHashMap<String, JournalTransactionStore.AddMsgLocation>();

        while ((location = AddMsgLocation.decode(buf)) != null) {
            rt.put(location.storeDesc, location);
        }
        return rt;
    }
}