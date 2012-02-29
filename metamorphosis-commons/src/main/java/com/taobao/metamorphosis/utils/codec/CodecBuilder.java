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
package com.taobao.metamorphosis.utils.codec;

import java.util.HashMap;
import java.util.Map;

import com.taobao.metamorphosis.utils.codec.impl.Hessian1Deserializer;
import com.taobao.metamorphosis.utils.codec.impl.Hessian1Serializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaDeserializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;


/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 上午10:07:42
 */
public final class CodecBuilder {
    public static final Map<Codec_Type, Deserializer> decoderMap;

    public static final Map<Codec_Type, Serializer> encoderMap;

    static {
        decoderMap = new HashMap<Codec_Type, Deserializer>();
        encoderMap = new HashMap<Codec_Type, Serializer>();
        decoderMap.put(Codec_Type.JAVA, new JavaDeserializer());
        decoderMap.put(Codec_Type.HESSIAN1, new Hessian1Deserializer());
        encoderMap.put(Codec_Type.JAVA, new JavaSerializer());
        encoderMap.put(Codec_Type.HESSIAN1, new Hessian1Serializer());
    }


    public static Serializer buildSerializer(final Codec_Type type) {
        return encoderMap.get(type);
    }


    public static Deserializer buildDeserializer(final Codec_Type type) {
        return decoderMap.get(type);
    }

    public static enum Codec_Type {
        JAVA,
        HESSIAN1;

        public static Codec_Type parseByte(final byte type) {
            switch (type) {
            case 0:
                return JAVA;
            case 1:
                return HESSIAN1;
            }
            throw new IllegalArgumentException("Invalid Codec type: " + "现在只支持JAVA, HESSIAN及其SIMPLE.");
        }


        public byte toByte() {
            switch (this) {
            case JAVA:
                return 0;
            case HESSIAN1:
                return 1;
            }
            throw new IllegalArgumentException("Invalid Codec type: " + "现在只支持JAVA, HESSIAN及其SIMPLE.");
        }
    }

}