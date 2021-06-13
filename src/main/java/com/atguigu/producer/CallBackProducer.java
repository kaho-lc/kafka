package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author lc
 * @create 2021-06-04-17:05
 */
public class CallBackProducer {

  public static void main(String[] args) {

      //1.创建配置信息
      Properties properties = new Properties();
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer");

      //2.创建生产者对象
      KafkaProducer producer = new KafkaProducer<String , String>(properties);

      //3.发送数据
      for (int i = 0; i < 10;i++) {
          producer.send(new ProducerRecord("first",0,"atguigu","atguigu-" + i), new Callback() {
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                  if (e == null){
                      System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset());
                  }else {
                      e.printStackTrace();
                  }
              }
          });

      }

      //4.关闭资源
      producer.close();
  }
}
