package net.wujunwei.kafka.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka producer 例子
 *
 * @author wujunwei
 * @email  1399952803@qq.com
 * @github https://github.com/wujunwei928
 * @blog   http://www.wujunwei.net
 */
public class ProducerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        // 指定kafka的broker地址
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092,cdh02:9092,cdh03:9092");
        // 指定key-value数据的序列化格式
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        // 指定topic
        // ps: kafka topic不存在时, 竟然报 LEADER_NOT_AVAILABLE
        String topic = "hello-kafka";

        // 向topic中生产数据
        producer.send(new ProducerRecord<String, String>(topic, "Hello Kafka"));
        System.out.println("send finish");

        // 关闭链接
        producer.close();
    }
}
