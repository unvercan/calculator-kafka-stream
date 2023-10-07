package tr.unvercanunlu.calculator_kafka_stream.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import tr.unvercanunlu.calculator_kafka_stream.model.entity.Calculation;
import tr.unvercanunlu.calculator_kafka_stream.model.request.CalculationRequest;

import java.util.List;
import java.util.UUID;

@Validated
public interface ICalculationController {

    ResponseEntity<List<Calculation>> getAll();

    ResponseEntity<Calculation> get(@NotNull(message = "Id should not be null.") UUID id);

    ResponseEntity<Void> create(
            @NotNull(message = "Calculation request should not be null.")
            @Valid
            CalculationRequest request);

}
