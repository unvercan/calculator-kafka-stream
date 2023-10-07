package tr.unvercanunlu.calculator_kafka_stream.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value(value = "${spring.kafka.group-id}")
    private String groupId;

    public Map<String, Object> jsonConsumerConfigMap() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configMap.put(JsonDeserializer.TRUSTED_PACKAGES, "tr.unvercanunlu.calculator_kafka_stream.kafka.message, tr.unvercanunlu.calculator_kafka_stream.model.entity");

        return configMap;
    }

    public Map<String, Object> resultConsumerConfigMap() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class);
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return configMap;
    }

    @Bean
    public ConsumerFactory<String, Double> resultConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(this.resultConsumerConfigMap());
    }

    @Bean(name = "resultListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Double> resultListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Double> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.resultConsumerFactory());
        return factory;
    }
}
