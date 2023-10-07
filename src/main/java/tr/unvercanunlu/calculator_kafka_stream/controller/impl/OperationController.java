package tr.unvercanunlu.calculator_kafka_stream.controller.impl;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;
import tr.unvercanunlu.calculator_kafka_stream.controller.ApiConfig;
import tr.unvercanunlu.calculator_kafka_stream.controller.IOperationController;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Operation;
import tr.unvercanunlu.calculator_kafka_stream.repository.IOperationRepository;

import java.util.List;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = ApiConfig.OPERATION_API)
public class OperationController implements IOperationController {

    private final Logger logger = LoggerFactory.getLogger(OperationController.class);

    private final IOperationRepository operationRepository;

    @Override
    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<List<Operation>> getAll() {
        this.logger.info("Get all operations request is received.");

        List<Operation> operations = this.operationRepository.findAll();

        this.logger.debug("All operations: " + operations);

        this.logger.info("All operations are fetched from the database.");

        return ResponseEntity.status(HttpStatus.OK.value())
                .contentType(MediaType.APPLICATION_JSON)
                .body(operations);
    }

    @Override
    @RequestMapping(path = "/{code}", method = RequestMethod.GET)
    public ResponseEntity<Operation> get(@PathVariable(name = "code") Integer code) {
        this.logger.info("Get operation with '" + code + "' code request is received.");

        Optional<Operation> optionalOperation = this.operationRepository.findById(code);

        if (optionalOperation.isEmpty()) {
            this.logger.info("Operation with '" + code + "' code is not found in the database.");

            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        this.logger.info("Operation with '" + code + "' code  is fetched from the database.");

        Operation operation = optionalOperation.get();

        this.logger.debug("Fetched operation: " + operation);

        return ResponseEntity.status(HttpStatus.OK.value())
                .contentType(MediaType.APPLICATION_JSON)
                .body(operation);
    }
}
