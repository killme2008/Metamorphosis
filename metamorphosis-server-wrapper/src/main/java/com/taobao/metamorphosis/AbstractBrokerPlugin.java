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
 *   wuhua <wq163@163.com> 
 */
package com.taobao.metamorphosis;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç02:22:38
 */

abstract public class AbstractBrokerPlugin implements BrokerPlugin {

    protected static final Log log = LogFactory.getLog(AbstractBrokerPlugin.class);

    protected MetaMorphosisBroker broker;
    protected Properties props;


    @Override
    public boolean equals(final Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj instanceof BrokerPlugin) {
            final BrokerPlugin that = (BrokerPlugin) obj;
            if (this.name() == that.name()) {
                return true;
            }
            if (this.name() != null) {
                return this.name().equals(that.name());
            }
            if (this.name() == null) {
                return that.name() == null;
            }
        }
        return false;
    }


    @Override
    public int hashCode() {
        return this.name() != null ? this.name().hashCode() : 0;
    }
}