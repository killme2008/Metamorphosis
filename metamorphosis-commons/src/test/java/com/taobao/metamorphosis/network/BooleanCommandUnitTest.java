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
package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class BooleanCommandUnitTest {
    @Test
    public void testEncodeWithMessage() {
        final BooleanCommand cmd = new BooleanCommand(99, HttpStatus.NotFound, "not found");
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("result 404 9 99\r\nnot found", new String(buf.array()));
    }


    @Test
    public void testEncodeWithoutMessage() {
        final BooleanCommand cmd = new BooleanCommand(99, HttpStatus.NotFound, null);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("result 404 0 99\r\n", new String(buf.array()));
    }
}