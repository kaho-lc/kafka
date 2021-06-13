package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author lc
 * @create 2021-06-04-14:55
 */
public class MyProducer {

  public static void main(String[] args) throws InterruptedException {

    //1.创建kafka生产者的配置信息
    Properties properties = new Properties();

    //2.指定连接的kafka集群
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

    //3.ACK应答级别
    properties.put("acks", "all");

    //4.重试次数
    properties.put("retries", 1);

    //5.批次大小
    properties.put("batch.size", 16384);

    //6.等待时间，默认1毫秒
    properties.put("linger.ms", 1);

    //7.RecordAccumulator 缓冲区大小
    properties.put("buffer.memory", 33554432);//32M

    //8.指定key和value的序列化类
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    //9.创建生产者对象
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //10.发送数据
    for(int i = 0; i < 10000; i++) {
      producer.send(new ProducerRecord<String, String>("first", "atguigu","atguigu--" + i));
    }

//    Thread.sleep(100);//一定要关闭资源，这种方法不靠谱
    //11.关闭资源
    producer.close();

  }
}
