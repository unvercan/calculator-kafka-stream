package tr.unvercanunlu.calculator_kafka_stream.kafka.producer;

import jakarta.validation.constraints.NotNull;
import org.springframework.validation.annotation.Validated;

@Validated
public interface IKafkaProducer<K, V> {

    void send(
            @NotNull(message = "Key should not be null.") K key,
            @NotNull(message = "Value should not be null.") V value);

}
