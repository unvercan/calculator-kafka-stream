package tr.unvercanunlu.calculator_kafka_stream.kafka.message;

import lombok.*;

import java.io.Serializable;

@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CalculationMessage implements Serializable {

    private Integer first;

    private Integer second;

    private Integer operationCode;

}
