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
package com.taobao.metamorphosis.server.transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.store.JournalStore;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore;
import com.taobao.metamorphosis.server.utils.MetaConfig;


public abstract class BaseTransactionUnitTest {

    protected JournalTransactionStore transactionStore;
    protected JournalStore journalStore;
    protected MessageStoreManager messageStoreManager;
    protected String path;


    @Before
    public void setUp() throws Exception {
        this.path = this.getTempPath();
        FileUtils.deleteDirectory(new File(this.path));
        this.init(this.path);
    }


    public String getTempPath() {
        final String path = System.getProperty("java.io.tmpdir");
        final String pathname = path + File.separator + "meta";
        return pathname;
    }


    protected void init(final String path) throws Exception {

        final MetaConfig config = new MetaConfig();
        config.setFlushTxLogAtCommit(1);
        config.setNumPartitions(10);
        final List<String> topics = new ArrayList<String>();
        topics.add("topic1");
        topics.add("topic2");
        config.setTopics(topics);
        config.setDataPath(path);
        this.messageStoreManager = new MessageStoreManager(config, null);
        this.transactionStore = new JournalTransactionStore(path, this.messageStoreManager, config);
        this.journalStore = this.transactionStore.getJournalStore();
    }


    @After
    public void tearDown() {
        this.messageStoreManager.dispose();
        this.transactionStore.dispose();
    }

}