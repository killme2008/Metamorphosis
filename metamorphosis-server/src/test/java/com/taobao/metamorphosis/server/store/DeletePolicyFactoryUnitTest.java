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

import org.junit.Test;

import com.taobao.metamorphosis.server.exception.UnknownDeletePolicyException;


public class DeletePolicyFactoryUnitTest {

    @Test(expected = UnknownDeletePolicyException.class)
    public void testGetDeletePolicyUnknow() {
        DeletePolicyFactory.getDeletePolicy("test");
    }


    @Test
    public void testGetDailyDeletePolicy() {
        DeletePolicy policy = DeletePolicyFactory.getDeletePolicy("delete,3");
        assertNotNull(policy);
        assertTrue(policy instanceof DiscardDeletePolicy);
        assertEquals(3 * 3600 * 1000, ((DiscardDeletePolicy) policy).getMaxReservedTime());
    }


    @Test
    public void testGetArchiveDeletePolicy() {
        DeletePolicy policy = DeletePolicyFactory.getDeletePolicy("archive,3,true");
        assertNotNull(policy);
        assertTrue(policy instanceof ArchiveDeletePolicy);
        assertEquals(3 * 3600 * 1000, ((ArchiveDeletePolicy) policy).getMaxReservedTime());
        assertTrue(((ArchiveDeletePolicy) policy).isCompress());
    }
}