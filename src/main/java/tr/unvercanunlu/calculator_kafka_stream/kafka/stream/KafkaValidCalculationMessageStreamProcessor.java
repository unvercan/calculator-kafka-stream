package tr.unvercanunlu.calculator_kafka_stream.kafka.stream;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import tr.unvercanunlu.calculator_kafka_stream.kafka.message.CalculationMessage;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Calculation;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Operation;
import tr.unvercanunlu.calculator_kafka_stream.repository.ICalculationRepository;
import tr.unvercanunlu.calculator_kafka_stream.repository.IOperationRepository;

import java.util.Optional;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
public class KafkaValidCalculationMessageStreamProcessor {

    private final Logger logger = LoggerFactory.getLogger(KafkaValidCalculationMessageStreamProcessor.class);

    private final ICalculationRepository calculationRepository;

    private final IOperationRepository operationRepository;

    private final JsonSerde<CalculationMessage> calculationMessageSerde;

    @Value(value = "${spring.kafka.topic.calculation}")
    private String calculationTopic;

    @Value(value = "${spring.kafka.topic.valid-calculation}")
    private String validCalculationTopic;

    @Bean
    public KStream<String, CalculationMessage> validate(StreamsBuilder streamsBuilder) {
        KStream<String, CalculationMessage> calculationMessageStream = streamsBuilder
                .stream(this.calculationTopic, Consumed.with(Serdes.String(), this.calculationMessageSerde))
                .peek((key, value) -> {
                    this.logger.debug("Record from '" + this.calculationTopic + "' Kafka Topic is consumed.");
                    this.logger.debug("Consumed record key: '" + key + "'.");
                    this.logger.debug("Consumed record value: " + value);
                });

        this.logger.info("Consuming records from '" + this.calculationTopic + "' Kafka Topic is done.");

        KStream<String, CalculationMessage> validCalculationMessageStream = calculationMessageStream
                .filter((key, value) -> {
                    Optional<Operation> optionalOperation = this.operationRepository.findById(value.getOperationCode());

                    if (optionalOperation.isEmpty()) {
                        this.logger.info("Operation with '" + value.getOperationCode() + "' code is not found in the database.");

                        return false;
                    }

                    Operation operation = optionalOperation.get();

                    this.logger.debug("Fetched operation: " + operation);

                    this.logger.info("Operation with '" + value.getOperationCode() + "' code  is fetched from the database.");

                    return true;
                });

        this.logger.info("Records from Calculation Message Kafka Stream are validated by operation from the database.");

        validCalculationMessageStream = validCalculationMessageStream
                .filter(((key, value) -> {
                    UUID calculationId = UUID.fromString(key);

                    Optional<Calculation> optionalCalculation = this.calculationRepository.findById(calculationId);

                    if (optionalCalculation.isEmpty()) {
                        this.logger.info("Calculation with '" + key + "' id is not found in the database.");

                        return false;
                    }

                    Calculation calculation = optionalCalculation.get();

                    this.logger.debug("Fetched calculation: " + calculation);

                    this.logger.info("Calculation with '" + key + "' id is fetched from the database.");

                    if (!value.getFirst().equals(calculation.getFirst())
                            || !value.getSecond().equals(calculation.getSecond())
                            || !value.getOperationCode().equals(calculation.getOperationCode())) {

                        this.logger.info("Calculation with '" + key + "' id is different from the message.");

                        return false;
                    }

                    return true;
                }));

        this.logger.info("Records from Calculation Message Kafka Stream are validated by calculation from the database.");

        validCalculationMessageStream.peek((key, value) -> this.logger.debug("Record from Calculation Message Kafka Stream: (Key: '" + key + "' , Value: " + value + ")."))
                .to(this.validCalculationTopic, Produced.with(Serdes.String(), this.calculationMessageSerde));

        this.logger.info("Records from Valid Calculation Message Kafka Stream are sent to '" + this.validCalculationTopic + "' Kafka Topic.");

        return validCalculationMessageStream;
    }
}
