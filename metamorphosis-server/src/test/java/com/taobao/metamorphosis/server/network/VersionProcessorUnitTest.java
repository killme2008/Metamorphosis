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

import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.utils.BuildProperties;


public class VersionProcessorUnitTest extends BaseProcessorUnitTest {

    private VersionProcessor processor;


    @Before
    public void setUp() {
        this.mock();
        this.processor = new VersionProcessor(this.commandProcessor);
    }


    @Test
    public void testHandleRequest() throws Exception {
        final int opaque = 1;
        this.conn.response(new BooleanCommand(opaque, HttpStatus.Success, BuildProperties.VERSION));
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        this.processor.handleRequest(new VersionCommand(opaque), this.conn);
        this.mocksControl.verify();
    }
}