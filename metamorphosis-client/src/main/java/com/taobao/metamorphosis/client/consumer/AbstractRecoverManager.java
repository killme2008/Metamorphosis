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
package com.taobao.metamorphosis.client.consumer;

import com.taobao.metamorphosis.exception.UnknowCodecTypeException;
import com.taobao.metamorphosis.utils.codec.Deserializer;
import com.taobao.metamorphosis.utils.codec.Serializer;
import com.taobao.metamorphosis.utils.codec.impl.Hessian1Deserializer;
import com.taobao.metamorphosis.utils.codec.impl.Hessian1Serializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaDeserializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-10-31 ÏÂÎç5:42:50
 */

public abstract class AbstractRecoverManager implements RecoverManager {

    private final String META_RECOVER_CODEC_TYPE = System.getProperty("meta.recover.codec", "java");
    protected final Serializer serializer;
    protected final Deserializer deserializer;


    public AbstractRecoverManager() {
        if (this.META_RECOVER_CODEC_TYPE.equals("java")) {
            this.serializer = new JavaSerializer();
            this.deserializer = new JavaDeserializer();
        }
        else if (this.META_RECOVER_CODEC_TYPE.equals("hessian1")) {
            this.serializer = new Hessian1Serializer();
            this.deserializer = new Hessian1Deserializer();
        }
        else {
            throw new UnknowCodecTypeException(this.META_RECOVER_CODEC_TYPE);
        }
    }
}