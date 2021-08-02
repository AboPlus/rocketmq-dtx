package com.abo.m1;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author Abo
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 新建消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("Consumer1");
        // 设置name server
        consumer.setNamesrvAddr("192.168.64.141:9876");
        // 从哪订阅消息
        /*
        *       *   --  所有标签
        *       Tag1
        *       Tag1 || Tag2 || Tag3    --  接收多种标签的消息
        * */
        consumer.subscribe("Topic1", "Tag1");
        // 设置消息监听器
        /*
        * concurrently -- 并发
        * 会启动多个线程，并发的处理消息
        * */
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : msgs) {
                    String s = new String(msg.getBody());
                    System.out.println("收到：" + s );
                }
                // 返回消息处理状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                // 通知服务器，稍后重新投递这条消息
                // return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        // 启动
        consumer.start();

    }
}
