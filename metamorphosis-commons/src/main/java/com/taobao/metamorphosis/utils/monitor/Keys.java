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
package com.taobao.metamorphosis.utils.monitor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * @author mengting
 * @date 2008-06-17
 */

public class Keys implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String placeholder = "level-";
    private List<String> keys = new ArrayList<String>(4);

    private String key1;
    private String key2;
    private String key3;
    private String appName;


    public Keys() {

    }


    public Keys(final List<String> keys) {
        this.keys.addAll(keys);
        if (keys != null && keys.size() == 4) {
            this.appName = keys.get(1);
        }
    }


    public Keys(final String key) {
        this(null, key, null, null);
    }


    public Keys(final String key1, final String key2) {
        this(null, key1, key2, null);

    }


    public Keys(final String key1, final String key2, final String key3) {
        this(null, key1, key2, key3);
    }


    public Keys(final String appName, final String key1, final String key2, final String key3) {
        this.keys.add(appName);
        this.keys.add(key1);
        this.keys.add(key2);
        this.keys.add(key3);

        this.appName = appName;
        this.key1 = key1;
        this.key2 = key2;
        this.key3 = key3;

    }


    @Override
    public boolean equals(final Object comparedKeys) {
        if (!(comparedKeys instanceof Keys)) {
            return false;
        }
        return keys.equals(((Keys) comparedKeys).getKeys());
    }


    @Override
    public int hashCode() {
        return keys.hashCode();
    }


    public String getString(final String splitter) {
        final StringBuilder sb = new StringBuilder();
        boolean isNotFirst = false;
        int i = 0;
        for (String key : keys) {

            if (++i == 1 && key == null && appName == null) {
                continue;
            }
            else if (key == null) {
                key = placeholder + (i - 1);
            }

            if (isNotFirst) {
                sb.append(splitter);
            }
            else {
                isNotFirst = true;
            }

            sb.append(key);
        }

        return sb.toString();
    }


    private List<String> getKeys() {
        return keys;
    }


    public void setKeys(final List<String> keys) {
        this.keys = keys;
    }


    public String getKey1() {
        return key1;
    }


    public void setKey1(final String key1) {
        this.key1 = key1;
    }


    public String getKey2() {
        return key2;
    }


    public void setKey2(final String key2) {
        this.key2 = key2;
    }


    public String getKey3() {
        return key3;
    }


    public void setKey3(final String key3) {
        this.key3 = key3;
    }


    public String getAppName() {
        return appName;
    }


    public void setAppName(final String appName) {
        this.appName = appName;
    }
}