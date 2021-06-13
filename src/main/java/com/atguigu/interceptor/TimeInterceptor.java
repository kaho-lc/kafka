package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author lc
 * @create 2021-06-05-16:16
 */
public class TimeInterceptor implements ProducerInterceptor {

    public void configure(Map<String, ?> map) {

    }

    public ProducerRecord<String , String> onSend(ProducerRecord producerRecord) {
        //1.取出数据
        Object value = producerRecord.value();

        //2.创建一个ProducerRecord对象，并且返回
        return new ProducerRecord(producerRecord.topic() , producerRecord.partition() , producerRecord.key() ,  System.currentTimeMillis() + "," + value);
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

}
