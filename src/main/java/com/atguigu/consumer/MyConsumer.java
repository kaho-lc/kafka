package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.*;

/**
 * @author lc
 * @create 2021-06-05-14:11
 */
public class MyConsumer {
    public static void main(String[] args) {

        //1.创建消费者配置信息
        Properties properties = new Properties();

        //2.给配置信息赋值
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");//连接的集群
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);//开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//开启自动提交
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");//自动提交的延迟
        //key的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //value的反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG , "lc");

        //以下配置只有两种情况下才生效：更换新的消费者组和保存的offset过期了
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //3.创建消费者
        KafkaConsumer consumer = new KafkaConsumer(properties);

        //4.订阅主题
        consumer.subscribe(Arrays.asList("first" , "third"));

        //5.获取数据
        while (true){

            ConsumerRecords<String, String> records = consumer.poll(100);

            //6.解析并且打印ConsumerRecords
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + "--" + record.value());
            }
        }
    }
}


