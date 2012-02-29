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

import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

import com.taobao.metamorphosis.transaction.TransactionId;


public abstract class TransactionUnitTest {

    protected TransactionStore transactionStore;

    protected TransactionId xid;


    @Before
    public void setUp() {
        this.transactionStore = EasyMock.createMock(TransactionStore.class);
    }


    @After
    public void tearDown() {
        EasyMock.verify(this.transactionStore);
    }


    protected void mockStoreCommitTwoPhase() throws IOException {
        this.transactionStore.commit(this.xid, true);
        EasyMock.expectLastCall();
    }


    protected void mockStoreCommitOnePhase() throws IOException {
        this.transactionStore.commit(this.xid, false);
        EasyMock.expectLastCall();
    }


    protected void mockStorePrepare() throws IOException {
        this.transactionStore.prepare(this.xid);
        EasyMock.expectLastCall();
    }


    protected void mockStoreRollback() throws IOException {
        this.transactionStore.rollback(this.xid);
        EasyMock.expectLastCall();
    }

}