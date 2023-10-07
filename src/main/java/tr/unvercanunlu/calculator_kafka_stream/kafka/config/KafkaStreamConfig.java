package tr.unvercanunlu.calculator_kafka_stream.kafka.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import tr.unvercanunlu.calculator_kafka_stream.kafka.message.CalculationMessage;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaStreamConfig {

    private final JsonSerde<CalculationMessage> calculationMessageSerde;

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamConfig() {
        Map<String, Object> configMap = new HashMap<>();

        configMap.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculator_kafka_stream");
        configMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        configMap.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configMap.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, this.calculationMessageSerde.getClass());
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configMap.put(JsonDeserializer.TRUSTED_PACKAGES, "tr.unvercanunlu.calculator_kafka_stream.kafka.message, tr.unvercanunlu.calculator_kafka_stream.model.entity");

        return new KafkaStreamsConfiguration(configMap);
    }
}
