package com.abo.m2;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author Abo
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 新建消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("Consumer2");
        // 设置name server
        consumer.setNamesrvAddr("192.168.64.141:9876");
        // 从哪订阅消息
        /*
        *       *   --  所有标签
        *       Tag1
        *       Tag1 || Tag2 || Tag3    --  接收多种标签的消息
        * */
        consumer.subscribe("Topic2", "*");
        // 设置消息监听器
        /*
        * Orderly -- 按顺序消费
        * 单线程处理消息
        * */
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    String s = new String(msg.getBody());
                    System.out.println("收到：" + s );
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动
        consumer.start();
    }
}
