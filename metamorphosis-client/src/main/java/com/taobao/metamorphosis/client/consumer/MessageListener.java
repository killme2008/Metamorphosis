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

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;


/**
 * �첽��Ϣ������
 * 
 * @author boyan
 * @Date 2011-4-23
 * 
 */
public interface MessageListener {
    /**
     * ���յ���Ϣ��ֻ��messages��Ϊ�ղ��Ҳ�Ϊnull������»ᴥ���˷���
     * 
     * @param messages
     *            TODO ƴд����Ӧ���ǵ�������ʱ����ʹ��
     */
    public void recieveMessages(Message message) throws InterruptedException;


    /**
     * ������Ϣ���̳߳�
     * 
     * @return
     */
    public Executor getExecutor();
}