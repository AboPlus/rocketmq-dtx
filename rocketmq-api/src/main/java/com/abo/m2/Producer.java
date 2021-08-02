package com.abo.m2;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 顺序消息
 * @author Abo
 */
public class Producer {

    static String[] msgs = {
            "15103111039,创建",
            "15103111065,创建",
            "15103111039,付款",
            "15103117235,创建",
            "15103111065,付款",
            "15103117235,付款",
            "15103111065,完成",
            "15103111039,推送",
            "15103117235,完成",
            "15103111039,完成"
    };

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("producer2");
        // 设置 name server
        producer.setNamesrvAddr("192.168.64.141:9876");
        // 启动
        producer.start();
        // 发送消息，设置队列选择器用来选择队列
        for (String msg : msgs) {
            Long orderId = Long.valueOf(msg.split(",")[0]);
            Message message = new Message("Topic2", msg.getBytes());
            // producer.send(message, 队列选择器, 选择依据);
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    /*
                    * 参数：
                    *   1.服务器端的队列信息
                    *   2.消息
                    *   3.选择依据
                    * */
                    Long orderId = (Long) arg;
                    int index = (int) (orderId % mqs.size());
                    return mqs.get(index);
                }
            }, orderId);
        }
    }
}
