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

/**
 * 统计名称常量
 * 
 * @author boyan
 * @Date 2011-5-8
 * 
 */
public class StatConstants {
    /**
     * 发送消息时间统计
     */
    public static final String PUT_TIME_STAT = "cli_put_time";
    /**
     * 发送消息查过一定值的统计
     */
    public static final String PUT_TIMEOUT_STAT = "cli_put_timeout";

    /**
     * 接收消息跳过处理的消息数
     */
    public static final String SKIP_MSG_COUNT = "cli_skip_msg_count";

    /**
     * 发送消息重试次数统计
     */
    public static final String PUT_RETRY_STAT = "cli_put_retry";

    /**
     * 获取消息时间统计
     */
    public static final String GET_TIME_STAT = "cli_get_timeout";

    /**
     * 获取消息失败统计
     */
    public static final String GET_FAILED_STAT = "cli_get_failed";

    /**
     * Offset查询统计
     */
    public static final String OFFSET_TIME_STAT = "cli_offset_timeout";
    /**
     * Offset查询失败统计
     */
    public static final String OFFSET_FAILED_STAT = "cli_offset_failed";

    /**
     * 获取消息数量统计
     */
    public static final String GET_MSG_COUNT_STAT = "cli_get_msg_count";

    /**
     * 非法消息统计
     */
    public static final String INVALID_MSG_STAT = "cli_invalid_message";

    /**
     * 服务端put消息失败
     */
    public static final String PUT_FAILED = "put_failed";

    /**
     * 服务端put消息失败
     */
    public static final String GET_FAILED = "get_failed";

    /**
     * 服务端接收到的消息大小
     */
    public static final String MESSAGE_SIZE = "message_size";

    /**
     * 服务端get miss
     */
    public static final String GET_MISS = "get_miss";

    /**
     * 服务端处理get请求
     */
    public static final String CMD_GET = "get";

    /**
     * 服务端处理offset
     */
    public static final String CMD_OFFSET = "offset";

    /**
     * 服务端处理put
     */
    public static final String CMD_PUT = "put";

    /**
     * 开始事务数
     */
    public static final String TX_BEGIN = "txBegin";

    /**
     * end事务数
     */
    public static final String TX_END = "txEnd";

    /**
     * prepare事务数
     */
    public static final String TX_PREPARE = "txPrepare";

    /**
     * 提交事务数
     */
    public static final String TX_COMMIT = "txCommit";

    /**
     * 回滚事务数
     */
    public static final String TX_ROLLBACK = "txRollback";

    /**
     * 事务平均执行时间
     */
    public static final String TX_TIME = "txExecTime";

}