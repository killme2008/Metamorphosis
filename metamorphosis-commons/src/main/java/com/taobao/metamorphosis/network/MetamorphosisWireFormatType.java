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

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.CommandFactory;
import com.taobao.gecko.core.command.CommandHeader;
import com.taobao.gecko.core.command.ResponseStatus;
import com.taobao.gecko.core.command.kernel.BooleanAckCommand;
import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;
import com.taobao.gecko.core.core.CodecFactory;
import com.taobao.gecko.core.core.Session;
import com.taobao.gecko.core.util.ByteBufferMatcher;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.core.util.ShiftAndByteBufferMatcher;
import com.taobao.gecko.service.config.WireFormatType;
import com.taobao.metamorphosis.exception.MetaCodecException;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.TransactionInfo.TransactionType;


/**
 * Metamorphosis wire format type
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public class MetamorphosisWireFormatType extends WireFormatType {

    public static final String SCHEME = "meta";


    @Override
    public String getScheme() {
        return SCHEME;
    }


    @Override
    public String name() {
        return "metamorphosis";
    }


    @Override
    public CodecFactory newCodecFactory() {
        return new MetaCodecFactory();
    }


    @Override
    public CommandFactory newCommandFactory() {
        return new MetaCommandFactory();
    }

    static final ByteBufferMatcher LINE_MATCHER = new ShiftAndByteBufferMatcher(IoBuffer.wrap(MetaEncodeCommand.CRLF));
    static final Pattern SPLITER = Pattern.compile(" ");

    static class MetaCommandFactory implements CommandFactory {

        @Override
        public BooleanAckCommand createBooleanAckCommand(final CommandHeader request,
                final ResponseStatus responseStatus, final String errorMsg) {
            int httpCode = -1;
            switch (responseStatus) {
            case NO_ERROR:
                httpCode = HttpStatus.Success;
                break;
            case THREADPOOL_BUSY:
            case NO_PROCESSOR:
                httpCode = HttpStatus.ServiceUnavilable;
                break;
            case TIMEOUT:
                httpCode = HttpStatus.GatewayTimeout;
                break;
            default:
                httpCode = HttpStatus.InternalServerError;
                break;
            }
            return new BooleanCommand(request.getOpaque(), httpCode, errorMsg);
        }


        @Override
        public HeartBeatRequestCommand createHeartBeatCommand() {
            return new VersionCommand(OpaqueGenerator.getNextOpaque());
        }

    }

    static final Log log = LogFactory.getLog(MetaCodecFactory.class);

    static class MetaCodecFactory implements CodecFactory {

        @Override
        public Decoder getDecoder() {
            return new Decoder() {

                @Override
                public Object decode(final IoBuffer buff, final Session session) {
                    if (buff == null || !buff.hasRemaining()) {
                        return null;
                    }
                    buff.mark();
                    final int index = LINE_MATCHER.matchFirst(buff);
                    if (index >= 0) {
                        final byte[] bytes = new byte[index - buff.position()];
                        buff.get(bytes);
                        // 跳过\r\n
                        buff.position(buff.position() + 2);
                        final String line = ByteUtils.getString(bytes);
                        if (log.isDebugEnabled()) {
                            log.debug("Receive command:" + line);
                        }
                        final String[] tmps = SPLITER.split(line);
                        if (tmps == null || tmps.length == 0) {
                            throw new MetaCodecException("Unknow command:" + line);
                        }
                        final byte op = (byte) tmps[0].charAt(0);
                        switch (op) {
                        case 'p':
                            return this.decodePut(buff, tmps);
                        case 'g':
                            return this.decodeGet(tmps);
                        case 't':
                            return this.decodeTransaction(tmps);
                        case 'r':
                            return this.decodeBoolean(buff, tmps);
                        case 'v':
                            if (tmps[0].equals("value")) {
                                return this.decodeData(buff, tmps);
                            }
                            else {
                                return this.decodeVersion(tmps);
                            }
                        case 's':
                            if (tmps[0].equals("stats")) {
                                return this.decodeStats(tmps);
                            }
                            else {
                                return this.decodeSync(buff, tmps);
                            }
                        case 'o':
                            return this.decodeOffset(tmps);
                        case 'q':
                            return this.decodeQuit();
                        default:
                            throw new MetaCodecException("Unknow command:" + line);
                        }
                    }
                    else {
                        return null;
                    }
                }


                private Object decodeQuit() {
                    return new QuitCommand();
                }


                private Object decodeVersion(final String[] tmps) {
                    if (tmps.length >= 2) {
                        return new VersionCommand(Integer.parseInt(tmps[1]));
                    }
                    else {
                        return new VersionCommand(Integer.MAX_VALUE);
                    }
                }


                // offset topic group partition offset opaque\r\n
                private Object decodeOffset(final String[] tmps) {
                    this.assertCommand(tmps[0], "offset");
                    return new OffsetCommand(tmps[1], tmps[2], Integer.parseInt(tmps[3]), Long.parseLong(tmps[4]),
                        Integer.parseInt(tmps[5]));
                }


                // stats item opaque\r\n
                // opaque可以为空
                private Object decodeStats(final String[] tmps) {
                    this.assertCommand(tmps[0], "stats");
                    int opaque = Integer.MAX_VALUE;
                    if (tmps.length >= 3) {
                        opaque = Integer.parseInt(tmps[2]);
                    }
                    String item = null;
                    if (tmps.length >= 2) {
                        item = tmps[1];
                    }
                    return new StatsCommand(opaque, item);
                }


                // value totalLen opaque\r\n data
                private Object decodeData(final IoBuffer buff, final String[] tmps) {
                    this.assertCommand(tmps[0], "value");
                    final int valueLen = Integer.parseInt(tmps[1]);
                    // 防止意外情况创建超大数组
                    // if (valueLen >
                    // com.taobao.notify.remoting.core.config.Configuration.MAX_READ_BUFFER_SIZE
                    // * 2) {
                    // throw new MetaCodecException("Invalid data length:" +
                    // valueLen);
                    // }
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        return new DataCommand(data, Integer.parseInt(tmps[2]));
                    }
                }


                /**
                 * result code length opaque\r\n message
                 * 
                 * @param buff
                 * @param tmps
                 * @return
                 */
                private Object decodeBoolean(final IoBuffer buff, final String[] tmps) {
                    this.assertCommand(tmps[0], "result");
                    final int valueLen = Integer.parseInt(tmps[2]);
                    if (valueLen == 0) {
                        return new BooleanCommand(Integer.parseInt(tmps[3]), Integer.parseInt(tmps[1]), null);
                    }
                    else {
                        if (buff.remaining() < valueLen) {
                            buff.reset();
                            return null;
                        }
                        else {
                            final byte[] data = new byte[valueLen];
                            buff.get(data);
                            return new BooleanCommand(Integer.parseInt(tmps[3]), Integer.parseInt(tmps[1]),
                                ByteUtils.getString(data));
                        }
                    }
                }


                // get topic group partition offset maxSize opaque\r\n
                private Object decodeGet(final String[] tmps) {
                    this.assertCommand(tmps[0], "get");
                    return new GetCommand(tmps[1], tmps[2], Integer.parseInt(tmps[3]), Long.parseLong(tmps[4]),
                        Integer.parseInt(tmps[5]), Integer.parseInt(tmps[6]));
                }


                // transaction key sessionId type [timeout] opaque\r\n
                private Object decodeTransaction(final String[] tmps) {
                    this.assertCommand(tmps[0], "transaction");
                    final TransactionId transactionId = this.getTransactionId(tmps[1]);
                    final TransactionType type = TransactionType.valueOf(tmps[3]);
                    if (tmps.length == 6) {
                        final int timeout = Integer.valueOf(tmps[4]);
                        final TransactionInfo info = new TransactionInfo(transactionId, tmps[2], type, timeout);
                        return new TransactionCommand(info, Integer.parseInt(tmps[5]));
                    }
                    else if (tmps.length == 5) {
                        final TransactionInfo info = new TransactionInfo(transactionId, tmps[2], type);
                        return new TransactionCommand(info, Integer.parseInt(tmps[4]));
                    }
                    else {
                        throw new MetaCodecException("Invalid transaction command:" + StringUtils.join(tmps));
                    }
                }


                private TransactionId getTransactionId(final String s) {
                    return TransactionId.valueOf(s);
                }


                // sync topic partition value-length flag msgId
                // opaque\r\n
                private Object decodeSync(final IoBuffer buff, final String[] tmps) {
                    this.assertCommand(tmps[0], "sync");
                    final int valueLen = Integer.parseInt(tmps[3]);
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        if (tmps.length == 7) {
                            return new SyncCommand(tmps[1], Integer.parseInt(tmps[2]), data, Long.valueOf(tmps[5]),
                                Integer.parseInt(tmps[4]), Integer.parseInt(tmps[6]));
                        }
                        else {
                            throw new MetaCodecException("Invalid Sync command:" + StringUtils.join(tmps));
                        }
                    }
                }


                // put topic partition value-length flag [transactionKey]
                // opaque\r\n
                private Object decodePut(final IoBuffer buff, final String[] tmps) {
                    this.assertCommand(tmps[0], "put");
                    final int valueLen = Integer.parseInt(tmps[3]);
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        if (tmps.length == 6) {
                            return new PutCommand(tmps[1], Integer.parseInt(tmps[2]), data, null,
                                Integer.parseInt(tmps[4]), Integer.parseInt(tmps[5]));
                        }
                        else if (tmps.length == 7) {
                            // 事务性消息
                            // added by boyan,2011-08-18
                            return new PutCommand(tmps[1], Integer.parseInt(tmps[2]), data,
                                this.getTransactionId(tmps[5]), Integer.parseInt(tmps[4]), Integer.parseInt(tmps[6]));
                        }
                        else {
                            throw new MetaCodecException("Invalid put command:" + StringUtils.join(tmps));
                        }
                    }
                }


                private void assertCommand(final String cmd, final String expect) {
                    if (!expect.equals(cmd)) {
                        throw new MetaCodecException("Expect " + expect + ",but was " + cmd);
                    }
                }

            };
        }


        @Override
        public Encoder getEncoder() {
            return new Encoder() {

                @Override
                public IoBuffer encode(final Object message, final Session session) {
                    return ((MetaEncodeCommand) message).encode();
                }
            };
        }

    }

}