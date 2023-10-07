package tr.unvercanunlu.calculator_kafka_stream.model.entity;

import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
import java.util.UUID;

@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "calculation")
@Table(name = "calculation")
public class Calculation implements Serializable {

    @Id
    @GeneratedValue
    @Column(name = "id", updatable = false, nullable = false, unique = true)
    private UUID id;

    @Column(name = "first", nullable = false)
    private Integer first;

    @Column(name = "second", nullable = false)
    private Integer second;

    @Column(name = "operation_code", nullable = false)
    private Integer operationCode;

    @Column(name = "result")
    private Double result;

}
