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
            return new BooleanCommand(httpCode, errorMsg, request.getOpaque());
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
                        final String[] sa = SPLITER.split(line);
                        if (sa == null || sa.length == 0) {
                            throw new MetaCodecException("Blank command line.");
                        }
                        final byte op = (byte) sa[0].charAt(0);
                        switch (op) {
                        case 'p':
                            return this.decodePut(buff, sa);
                        case 'g':
                            return this.decodeGet(sa);
                        case 't':
                            return this.decodeTransaction(sa);
                        case 'r':
                            return this.decodeBoolean(buff, sa);
                        case 'v':
                            if (sa[0].equals("value")) {
                                return this.decodeData(buff, sa);
                            }
                            else {
                                return this.decodeVersion(sa);
                            }
                        case 's':
                            if (sa[0].equals("stats")) {
                                return this.decodeStats(sa);
                            }
                            else {
                                return this.decodeSync(buff, sa);
                            }
                        case 'o':
                            return this.decodeOffset(sa);
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


                private Object decodeVersion(final String[] sa) {
                    if (sa.length >= 2) {
                        return new VersionCommand(Integer.parseInt(sa[1]));
                    }
                    else {
                        return new VersionCommand(Integer.MAX_VALUE);
                    }
                }


                // offset topic group partition offset opaque\r\n
                private Object decodeOffset(final String[] sa) {
                    this.assertCommand(sa[0], "offset");
                    return new OffsetCommand(sa[1], sa[2], Integer.parseInt(sa[3]), Long.parseLong(sa[4]),
                        Integer.parseInt(sa[5]));
                }


                // stats item opaque\r\n
                // opaque可以为空
                private Object decodeStats(final String[] sa) {
                    this.assertCommand(sa[0], "stats");
                    int opaque = Integer.MAX_VALUE;
                    if (sa.length >= 3) {
                        opaque = Integer.parseInt(sa[2]);
                    }
                    String item = null;
                    if (sa.length >= 2) {
                        item = sa[1];
                    }
                    return new StatsCommand(opaque, item);
                }


                // value totalLen opaque\r\n data
                private Object decodeData(final IoBuffer buff, final String[] sa) {
                    this.assertCommand(sa[0], "value");
                    final int valueLen = Integer.parseInt(sa[1]);
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        return new DataCommand(data, Integer.parseInt(sa[2]));
                    }
                }


                /**
                 * result code length opaque\r\n message
                 * 
                 * @param buff
                 * @param sa
                 * @return
                 */
                private Object decodeBoolean(final IoBuffer buff, final String[] sa) {
                    this.assertCommand(sa[0], "result");
                    final int valueLen = Integer.parseInt(sa[2]);
                    if (valueLen == 0) {
                        return new BooleanCommand(Integer.parseInt(sa[1]), null, Integer.parseInt(sa[3]));
                    }
                    else {
                        if (buff.remaining() < valueLen) {
                            buff.reset();
                            return null;
                        }
                        else {
                            final byte[] data = new byte[valueLen];
                            buff.get(data);
                            return new BooleanCommand(Integer.parseInt(sa[1]), ByteUtils.getString(data),
                                Integer.parseInt(sa[3]));
                        }
                    }
                }


                // get topic group partition offset maxSize opaque\r\n
                private Object decodeGet(final String[] sa) {
                    this.assertCommand(sa[0], "get");
                    return new GetCommand(sa[1], sa[2], Integer.parseInt(sa[3]), Long.parseLong(sa[4]),
                        Integer.parseInt(sa[5]), Integer.parseInt(sa[6]));
                }


                // transaction key sessionId type [timeout] [unique qualifier]
                // opaque\r\n
                private Object decodeTransaction(final String[] sa) {
                    this.assertCommand(sa[0], "transaction");
                    final TransactionId transactionId = this.getTransactionId(sa[1]);
                    final TransactionType type = TransactionType.valueOf(sa[3]);
                    switch (sa.length) {
                    case 7:
                        // Both include timeout and unique qualifier.
                        int timeout = Integer.valueOf(sa[4]);
                        String uniqueQualifier = sa[5];
                        TransactionInfo info =
                                new TransactionInfo(transactionId, sa[2], type, uniqueQualifier, timeout);
                        return new TransactionCommand(info, Integer.parseInt(sa[6]));
                    case 6:
                        // Maybe timeout or unique qualifier
                        if (StringUtils.isNumeric(sa[4])) {
                            timeout = Integer.valueOf(sa[4]);
                            info = new TransactionInfo(transactionId, sa[2], type, null, timeout);
                            return new TransactionCommand(info, Integer.parseInt(sa[5]));
                        }
                        else {
                            uniqueQualifier = sa[4];
                            info = new TransactionInfo(transactionId, sa[2], type, uniqueQualifier, 0);
                            return new TransactionCommand(info, Integer.parseInt(sa[5]));
                        }
                    case 5:
                        // Without timeout and unique qualifier.
                        info = new TransactionInfo(transactionId, sa[2], type, null);
                        return new TransactionCommand(info, Integer.parseInt(sa[4]));
                    default:
                        throw new MetaCodecException("Invalid transaction command:" + StringUtils.join(sa));
                    }

                }


                private TransactionId getTransactionId(final String s) {
                    return TransactionId.valueOf(s);
                }


                // sync topic partition value-length flag msgId
                // opaque\r\n
                private Object decodeSync(final IoBuffer buff, final String[] sa) {
                    this.assertCommand(sa[0], "sync");
                    final int valueLen = Integer.parseInt(sa[3]);
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        switch (sa.length) {
                        case 7:
                            // old master before 1.4.4
                            return new SyncCommand(sa[1], Integer.parseInt(sa[2]), data, Integer.parseInt(sa[4]),
                                Long.valueOf(sa[5]), -1, Integer.parseInt(sa[6]));
                        case 8:
                            // new master since 1.4.4
                            return new SyncCommand(sa[1], Integer.parseInt(sa[2]), data, Integer.parseInt(sa[4]),
                                Long.valueOf(sa[5]), Integer.parseInt(sa[6]), Integer.parseInt(sa[7]));
                        default:
                            throw new MetaCodecException("Invalid Sync command:" + StringUtils.join(sa));
                        }
                    }
                }


                // put topic partition value-length flag checksum
                // [transactionKey]
                // opaque\r\n
                private Object decodePut(final IoBuffer buff, final String[] sa) {
                    this.assertCommand(sa[0], "put");
                    final int valueLen = Integer.parseInt(sa[3]);
                    if (buff.remaining() < valueLen) {
                        buff.reset();
                        return null;
                    }
                    else {
                        final byte[] data = new byte[valueLen];
                        buff.get(data);
                        switch (sa.length) {
                        case 6:
                            // old clients before 1.4.4
                            return new PutCommand(sa[1], Integer.parseInt(sa[2]), data, null, Integer.parseInt(sa[4]),
                                Integer.parseInt(sa[5]));
                        case 7:
                            // either transaction command or new clients since
                            // 1.4.4
                            String slot = sa[5];
                            char firstChar = slot.charAt(0);
                            if (Character.isDigit(firstChar) || '-' == firstChar) {
                                // slot is checksum.
                                int checkSum = Integer.parseInt(slot);
                                return new PutCommand(sa[1], Integer.parseInt(sa[2]), data, Integer.parseInt(sa[4]),
                                    checkSum, null, Integer.parseInt(sa[6]));
                            }
                            else {
                                // slot is transaction id.
                                return new PutCommand(sa[1], Integer.parseInt(sa[2]), data,
                                    this.getTransactionId(slot), Integer.parseInt(sa[4]), Integer.parseInt(sa[6]));
                            }
                        case 8:
                            // New clients since 1.4.4
                            // A transaction command
                            return new PutCommand(sa[1], Integer.parseInt(sa[2]), data, Integer.parseInt(sa[4]),
                                Integer.parseInt(sa[5]), this.getTransactionId(sa[6]), Integer.parseInt(sa[7]));
                        default:
                            throw new MetaCodecException("Invalid put command:" + StringUtils.join(sa));
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