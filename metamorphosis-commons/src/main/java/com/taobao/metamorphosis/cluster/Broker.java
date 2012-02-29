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
package com.taobao.metamorphosis.cluster;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.utils.URIUtils;


/**
 * 表示一台meta服务器
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-26
 */
public class Broker {
    private int id;
    private String host;
    private int port;
    private int slaveId = -1;


    public Broker(final int id, final String host, final int port) {
        this(id, host, port, -1);
    }


    public Broker(final int id, final String host, final int port, final int slaveId) {
        super();
        this.id = id;
        this.host = host;
        this.port = port;
        this.slaveId = slaveId;
    }


    /**
     * 返回broker节点数据字符串 meta://host:port
     * 
     * @return
     */
    public String getZKString() {
        // ipv6
        if (this.host.contains(":")) {
            if (this.host.startsWith("[")) {
                return "meta://" + this.host + ":" + this.port;
            }
            else {
                return "meta://[" + this.host + "]:" + this.port;
            }
        }
        else {
            return "meta://" + this.host + ":" + this.port;
        }
    }


    public int getId() {
        return this.id;
    }


    public void setId(final int id) {
        this.id = id;
    }


    /**
     * @param id
     * @param data
     *            , meta://host:port or meta://host:port?slaveId=xx
     * 
     * */
    public Broker(final int id, final String data) {
        this.id = id;
        try {
            final URI uri = new URI(data);
            this.host = uri.getHost();
            this.port = uri.getPort();
            this.slaveId = slaveIdByUri(uri);
        }
        catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    static int slaveIdByUri(final URI uri) {
        final String slaveIdStr = URIUtils.parseParameters(uri, "UTF-8").get("slaveId");
        return StringUtils.isNotBlank(slaveIdStr) ? Integer.parseInt(slaveIdStr) : -1;
    }


    public String getHost() {
        return this.host;
    }


    public void setHost(final String host) {
        this.host = host;
    }


    public int getPort() {
        return this.port;
    }


    public void setPort(final int port) {
        this.port = port;
    }


    public boolean isSlave() {
        return this.slaveId >= 0;
    }


    public int getSlaveId() {
        return this.slaveId;
    }


    @Override
    public String toString() {
        return this.getZKString();
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.host == null ? 0 : this.host.hashCode());
        result = prime * result + this.id;
        result = prime * result + this.port;
        result = prime * result + this.slaveId >= 0 ? this.slaveId : -1;
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final Broker other = (Broker) obj;
        if (this.host == null) {
            if (other.host != null) {
                return false;
            }
        }
        else if (!this.host.equals(other.host)) {
            return false;
        }
        if (this.id != other.id) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        if (this.isSlave() != other.isSlave()) {
            return false;
        }
        if (this.isSlave() && other.isSlave()) {
            if (this.slaveId != other.slaveId) {
                return false;
            }
        }
        return true;
    }

}