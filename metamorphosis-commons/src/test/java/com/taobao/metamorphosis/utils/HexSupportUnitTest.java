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
package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class HexSupportUnitTest {

    @Test
    public void testToHexFromBytesToBytesFromHex() {
        final byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            final byte t = (byte) (i % Byte.MAX_VALUE);
            bytes[i] = (byte) (i % 2 == 0 ? t : -t);
        }

        final String s = HexSupport.toHexFromBytes(bytes);
        System.out.println(s);
        assertEquals(s, HexSupport.toHexFromBytes(bytes));
        assertEquals(s, HexSupport.toHexFromBytes(bytes));

        final byte[] decodedBytes = HexSupport.toBytesFromHex(s);
        assertArrayEquals(bytes, decodedBytes);
        assertArrayEquals(bytes, HexSupport.toBytesFromHex(s));
        assertArrayEquals(bytes, HexSupport.toBytesFromHex(s));
    }
}