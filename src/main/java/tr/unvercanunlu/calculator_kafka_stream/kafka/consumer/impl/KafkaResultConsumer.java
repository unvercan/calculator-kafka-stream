package tr.unvercanunlu.calculator_kafka_stream.kafka.consumer.impl;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tr.unvercanunlu.calculator_kafka_stream.kafka.consumer.IKafkaConsumer;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Calculation;
import tr.unvercanunlu.calculator_kafka_stream.repository.ICalculationRepository;

import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class KafkaResultConsumer implements IKafkaConsumer<String, Double> {

    private final Logger logger = LoggerFactory.getLogger(KafkaResultConsumer.class);

    private final ICalculationRepository calculationRepository;

    @Value(value = "${spring.kafka.topic.result}")
    private String resultTopic;

    @Override
    @KafkaListener(topics = "${spring.kafka.topic.result}", containerFactory = "resultListenerFactory", groupId = "${spring.kafka.group-id}")
    public void receive(ConsumerRecord<String, Double> payload) {
        this.logger.info("Kafka Result Consumer is started.");

        this.logger.info("Kafka Result Consumer consumed a new record from '" + this.resultTopic + "' Kafka Topic.");

        String key = payload.key();
        Double value = payload.value();

        this.logger.debug("Consumed record key: '" + key + "'.");
        this.logger.debug("Consumed record value: " + value);

        UUID calculationId = UUID.fromString(key);

        Optional<Calculation> optionalCalculation = this.calculationRepository.findById(calculationId);

        Calculation calculation = optionalCalculation.get();

        this.logger.debug("Fetched calculation: " + calculation);

        calculation.setResult(value);

        this.logger.info("Calculation with '" + calculationId + "' id is updated.");

        this.logger.debug("Updated calculation: " + calculation);

        calculation = this.calculationRepository.save(calculation);

        this.logger.info("Calculation with '" + calculationId + "' id is saved to database.");

        this.logger.debug("Saved calculation: " + calculation);

        this.logger.info("Kafka Result Consumer is end.");
    }
}
