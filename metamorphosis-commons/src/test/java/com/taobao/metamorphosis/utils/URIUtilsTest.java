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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-22 ÏÂÎç04:11:30
 */

public class URIUtilsTest {

    @Test
    public void testParseParameters() throws URISyntaxException {
        Map<String, String> params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true"), null);
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true&xx=yy&ww=qq"), null);
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?xx=yy&ww=qq"), null);
        Assert.assertTrue(params.get("isSlave") == null);
        
        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=false&xx=yy&ww=qq"), null);
        Assert.assertFalse(Boolean.parseBoolean(params.get("isSlave")));
    }
    
    @Test
    public void testParseParameters_utf8() throws URISyntaxException {
        Map<String, String> params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true"), "UTF-8");
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true&xx=yy&ww=qq"), "UTF-8");
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?xx=yy&ww=qq"), "UTF-8");
        Assert.assertTrue(params.get("isSlave") == null);
        
        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=false&xx=yy&ww=qq"), "UTF-8");
        Assert.assertFalse(Boolean.parseBoolean(params.get("isSlave")));
    }

}