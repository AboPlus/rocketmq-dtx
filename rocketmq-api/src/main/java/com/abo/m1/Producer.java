package com.abo.m1;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Scanner;

/**
 * @author Abo
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 新建生产者实例
        DefaultMQProducer prducer = new DefaultMQProducer("prducer1");
        // 设置 name server的地址
        prducer.setNamesrvAddr("192.168.64.141:9876");
        // 启动 -- 和服务器建立连接
        prducer.start();

        // 消息数据封装到Message对象
        while (true) {
            System.out.println("输入消息：");
            String s = new Scanner(System.in).nextLine();
            // Topic 相当于是一级分类，Tag 相当于是二级分类
            Message msg = new Message("Topic1", "Tag1", s.getBytes());
            // 发送Message
            SendResult result = prducer.send(msg);
            System.out.println(result);
        }

    }
}
