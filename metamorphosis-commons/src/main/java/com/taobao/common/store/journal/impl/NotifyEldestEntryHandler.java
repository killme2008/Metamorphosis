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
 *   boyan <killme2008@gmail.com>
 */
package com.taobao.common.store.journal.impl;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import com.taobao.common.store.journal.OpItem;
import com.taobao.common.store.util.BytesKey;
import com.taobao.common.store.util.LRUHashMap.EldestEntryHandler;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-10-20 …œŒÁ11:17:23
 */

public class NotifyEldestEntryHandler implements EldestEntryHandler<BytesKey, OpItem> {

    private OpItemHashMap diskMap;
    private final Log log = LogFactory.getLog(NotifyEldestEntryHandler.class);


    public NotifyEldestEntryHandler(final int capacity, final String cacheFilePath) throws IOException {
        this.diskMap = new OpItemHashMap(2 * capacity, cacheFilePath, false);
    }


    public OpItemHashMap getDiskMap() {
        return diskMap;
    }


    public void setDiskMap(final OpItemHashMap diskMap) {
        this.diskMap = diskMap;
    }


    public void close() throws IOException {
        this.diskMap.close();
    }


    @Override
    public boolean process(final Entry<BytesKey, OpItem> eldest) {
        try {
            // ≥¢ ‘¥Ê»Î¥≈≈Ã
            return this.diskMap.put(eldest.getKey(), eldest.getValue());
        }
        catch (final IOException e) {
            e.printStackTrace();
            log.error("–¥»Î¥≈≈Ãª∫¥Ê ß∞‹", e);
        }
        return false;
    }

}