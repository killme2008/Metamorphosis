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
package com.taobao.common.store;

import java.io.IOException;
import java.util.Iterator;


/**
 * 
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-12-11 ÉÏÎç11:17:22
 */
public interface Store {

    void add(byte[] key, byte[] data) throws IOException;


    void add(byte[] key, byte[] data, boolean force) throws IOException;


    boolean remove(byte[] key) throws IOException;


    boolean remove(byte[] key, boolean force) throws IOException;


    byte[] get(byte[] key) throws IOException;


    boolean update(byte[] key, byte[] data) throws IOException;


    int size();


    public long getMaxFileCount();


    public void setMaxFileCount(long maxFileCount);


    Iterator<byte[]> iterator() throws IOException;


    void close() throws IOException;
}