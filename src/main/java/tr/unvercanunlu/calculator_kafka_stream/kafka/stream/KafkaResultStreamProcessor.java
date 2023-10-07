package tr.unvercanunlu.calculator_kafka_stream.kafka.stream;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import tr.unvercanunlu.calculator_kafka_stream.kafka.message.CalculationMessage;

import java.util.Objects;

@Configuration
@RequiredArgsConstructor
public class KafkaResultStreamProcessor {

    private final Logger logger = LoggerFactory.getLogger(KafkaResultStreamProcessor.class);

    private final JsonSerde<CalculationMessage> calculationMessageSerde;

    @Value(value = "${spring.kafka.topic.valid-calculation}")
    private String validCalculationTopic;

    @Value(value = "${spring.kafka.topic.result}")
    private String resultTopic;

    @Bean
    public KStream<String, Double> calculate(StreamsBuilder streamsBuilder) {
        KStream<String, CalculationMessage> calculationMessageStream = streamsBuilder
                .stream(this.validCalculationTopic, Consumed.with(Serdes.String(), this.calculationMessageSerde))
                .peek((key, value) -> {
                    this.logger.debug("Record from '" + this.validCalculationTopic + "' Kafka Topic is consumed.");
                    this.logger.debug("Consumed record key: '" + key + "'.");
                    this.logger.debug("Consumed record value: " + value);
                });

        this.logger.info("Consuming records from '" + this.validCalculationTopic + "' Kafka Topic is done.");

        KTable<String, CalculationMessage> calculationMessageTable = calculationMessageStream.toTable();

        this.logger.info("Calculation Message Kafka Stream is converted to Calculation Message Kafka Table.");

        KTable<String, Double> resultTable = calculationMessageTable
                .mapValues(((key, value) -> {
                    Double result = switch (value.getOperationCode()) {
                        case 0 -> (double) value.getFirst() + (double) value.getSecond();
                        case 1 -> (double) value.getFirst() - (double) value.getSecond();
                        case 2 -> (double) value.getFirst() * (double) value.getSecond();
                        case 3 -> (double) value.getFirst() / (double) value.getSecond();
                        case 4 -> (double) value.getFirst() % (double) value.getSecond();
                        case 5 -> Math.pow(value.getFirst(), value.getSecond());
                        case 6 -> ((double) value.getFirst() + (double) value.getSecond()) / 2;
                        case 7 -> (double) Math.max(value.getFirst(), value.getSecond());
                        case 8 -> (double) Math.min(value.getFirst(), value.getSecond());
                        default -> null;
                    };

                    this.logger.info("Calculation with '" + key + "' id is done.");

                    return result;
                }));

        this.logger.info("Calculation Message Kafka Table is converted to Result Kafka Table.");

        KStream<String, Double> resultStream = resultTable.toStream();

        this.logger.info("Result Kafka Table is converted to Result Kafka Stream.");

        resultStream = resultStream.filter((key, value) ->
                Objects.nonNull(value) && !value.isNaN()
                        && !Objects.equals(value, Double.POSITIVE_INFINITY)
                        && !Objects.equals(value, Double.NEGATIVE_INFINITY));

        this.logger.info("Null, Not A Number, Infinity values are removed from Result Kafka Stream.");

        resultStream.peek((key, value) -> this.logger.debug("Record from Result Kafka Stream: (Key: '" + key + "' , Value: " + value + ")."))
                .to(this.resultTopic, Produced.with(Serdes.String(), Serdes.Double()));

        this.logger.info("Records from Result Kafka Stream are sent to '" + this.resultTopic + "' Kafka Topic.");

        return resultStream;
    }
}
