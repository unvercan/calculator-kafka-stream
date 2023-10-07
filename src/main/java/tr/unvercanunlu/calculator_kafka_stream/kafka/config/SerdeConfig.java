package tr.unvercanunlu.calculator_kafka_stream.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import tr.unvercanunlu.calculator_kafka_stream.kafka.message.CalculationMessage;

@Configuration
@RequiredArgsConstructor
public class SerdeConfig {

    private final KafkaProducerConfig kafkaProducerConfig;

    private final KafkaConsumerConfig kafkaConsumerConfig;

    @Bean
    public JsonSerde<CalculationMessage> calculationMessageSerde(ObjectMapper mapper) {
        JsonSerde<CalculationMessage> jsonSerde = new JsonSerde<>(CalculationMessage.class, mapper);
        jsonSerde.deserializer().configure(this.kafkaConsumerConfig.jsonConsumerConfigMap(), false);
        jsonSerde.serializer().configure(this.kafkaProducerConfig.jsonProducerConfigMap(), false);
        return jsonSerde;
    }
}
