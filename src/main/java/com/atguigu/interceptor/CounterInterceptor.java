package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author lc
 * @create 2021-06-05-16:16
 */

public class CounterInterceptor implements ProducerInterceptor {

    int success;
    int error;

    public void configure(Map<String, ?> map) {

    }

    public ProducerRecord<String , String> onSend(ProducerRecord producerRecord) {

        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null){
            success ++;
        }else {
            error ++;
        }
    }

    public void close() {
        System.out.println("success:" + success);
        System.out.println("error:" + error);
    }

}
