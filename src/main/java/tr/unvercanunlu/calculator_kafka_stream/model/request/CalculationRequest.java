package tr.unvercanunlu.calculator_kafka_stream.model.request;

import jakarta.validation.constraints.*;
import lombok.*;

import java.io.Serializable;

@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CalculationRequest implements Serializable {

    @Positive(message = "First operand should be positive integer.")
    @Digits(integer = 2, fraction = 0, message = "First operand should be integer.")
    @NotNull(message = "First operand cannot be null.")
    @Min(value = 1, message = "First operand should be at least one.")
    @Max(value = 10, message = "First operand should be at most ten.")
    private Integer first;

    @Positive(message = "Second operand should be positive integer.")
    @Digits(integer = 2, fraction = 0, message = "Second operand should be integer.")
    @NotNull(message = "Second operand cannot be null.")
    @Min(value = 1, message = "Second operand should be at least one.")
    @Max(value = 10, message = "Second operand should be at most ten.")
    private Integer second;

    @NotNull(message = "Operation code should not be null.")
    @Min(value = 0, message = "Operation code should be at least zero.")
    @Max(value = 8, message = "Operation code should be at most eight.")
    @Digits(integer = 1, fraction = 0, message = "Operation code should be integer and has at most one digit.")
    private Integer operationCode;

}
