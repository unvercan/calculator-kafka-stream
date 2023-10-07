package tr.unvercanunlu.calculator_kafka_stream.controller.impl;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import tr.unvercanunlu.calculator_kafka_stream.controller.ApiConfig;
import tr.unvercanunlu.calculator_kafka_stream.controller.ICalculationController;
import tr.unvercanunlu.calculator_kafka_stream.kafka.producer.IKafkaProducer;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Calculation;
import tr.unvercanunlu.calculator_kafka_stream.model.request.CalculationRequest;
import tr.unvercanunlu.calculator_kafka_stream.repository.ICalculationRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = ApiConfig.CALCULATION_API)
public class CalculationController implements ICalculationController {

    private final Logger logger = LoggerFactory.getLogger(CalculationController.class);

    private final ICalculationRepository calculationRepository;

    private final IKafkaProducer<UUID, Calculation> calculationKafkaProducer;

    @Override
    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<List<Calculation>> getAll() {
        this.logger.info("Get all calculations request is received.");

        List<Calculation> calculations = this.calculationRepository.findAll();

        this.logger.debug("All calculations: " + calculations);

        this.logger.info("All calculations are fetched from the database.");

        return ResponseEntity.status(HttpStatus.OK.value())
                .contentType(MediaType.APPLICATION_JSON)
                .body(calculations);
    }

    @Override
    @RequestMapping(path = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<Calculation> get(@PathVariable(name = "id") UUID id) {
        this.logger.info("Get calculation with '" + id + "' id request is received.");

        Optional<Calculation> optionalCalculation = this.calculationRepository.findById(id);

        if (optionalCalculation.isEmpty()) {
            this.logger.info("Calculation with '" + id + "' id is not found in the database.");

            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        this.logger.info("Calculation with '" + id + "' id is fetched from the database.");

        Calculation calculation = optionalCalculation.get();

        this.logger.debug("Fetched calculation: " + calculation);

        return ResponseEntity.status(HttpStatus.OK.value())
                .contentType(MediaType.APPLICATION_JSON)
                .body(calculation);
    }

    @Override
    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<Void> create(@RequestBody CalculationRequest request) {
        this.logger.info("Create calculation request is received.");

        this.logger.debug("Calculation request: " + request);

        Calculation calculation = Calculation.builder()
                .first(request.getFirst())
                .second(request.getSecond())
                .operationCode(request.getOperationCode())
                .build();

        this.logger.info("Calculation is created.");

        this.logger.debug("Created calculation: " + calculation);

        calculation = this.calculationRepository.save(calculation);

        this.logger.info("Calculation is saved to database.");

        this.logger.debug("Saved calculation: " + calculation);

        this.calculationKafkaProducer.send(calculation.getId(), calculation);

        this.logger.info("Sending Calculation to Kafka Topic using Kafka Calculation Producer is done.");

        return ResponseEntity.status(HttpStatus.OK.value())
                .contentType(MediaType.APPLICATION_JSON)
                .build();
    }
}
