package tr.unvercanunlu.calculator_kafka_stream.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import tr.unvercanunlu.calculator_kafka_stream.kafka.message.CalculationMessage;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    public Map<String, Object> jsonProducerConfigMap() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configMap.put(JsonDeserializer.TRUSTED_PACKAGES, "tr.unvercanunlu.calculator_kafka_stream.kafka.message, tr.unvercanunlu.calculator_kafka_stream.model.entity");

        return configMap;
    }

    @Bean
    public ProducerFactory<String, CalculationMessage> calculationMessageProducerFactory() {
        return new DefaultKafkaProducerFactory<>(this.jsonProducerConfigMap());
    }

    @Bean
    public KafkaTemplate<String, CalculationMessage> calculationMessageKafkaTemplate() {
        return new KafkaTemplate<>(this.calculationMessageProducerFactory());
    }
}
