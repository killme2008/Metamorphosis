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
package com.taobao.metamorphosis.server.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;

import org.easymock.classextension.EasyMock;
import org.junit.Test;


public class ArchiveDeletePolicyUnitTest {
    private ArchiveDeletePolicy policy;


    @Test
    public void testCanDelete() {
        this.policy = new ArchiveDeletePolicy();
        this.policy.setMaxReservedTime(1000L);
        File file = EasyMock.createMock(File.class);

        EasyMock.expect(file.lastModified()).andReturn(System.currentTimeMillis() - 2000);
        EasyMock.replay(file);
        assertTrue(this.policy.canDelete(file, System.currentTimeMillis()));
        EasyMock.verify(file);
    }


    @Test
    public void testInit() {
        this.policy = new ArchiveDeletePolicy();
        this.policy.init("12", "24");
        assertEquals(12 * 3600 * 1000, this.policy.getMaxReservedTime());
    }


    @Test
    public void testProcess() throws Exception {
        this.policy = new ArchiveDeletePolicy();
        File file = File.createTempFile("DailyDeletePolicyUnitTest", ".test");
        assertTrue(file.exists());
        this.policy.process(file);
        assertFalse(file.exists());
        final File archiveFile = new File(file.getParent(), file.getName() + ".arc");
        assertTrue(archiveFile.exists());
        archiveFile.delete();
    }


    @Test
    public void testProcessCompress() throws Exception {
        this.policy = new ArchiveDeletePolicy();
        this.policy.init("1", "true");
        File file = File.createTempFile("DailyDeletePolicyUnitTest", ".test");
        FileOutputStream out = new FileOutputStream(file);
        for (int i = 0; i < 1024; i++) {
            out.write("hello world".getBytes());
        }
        out.close();
        assertTrue(file.exists());
        this.policy.process(file);
        assertFalse(file.exists());
        final File archiveFile = new File(file.getParent(), file.getName() + ".zip");
        assertTrue(archiveFile.exists());
        archiveFile.delete();
    }
}