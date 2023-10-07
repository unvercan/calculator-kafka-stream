package tr.unvercanunlu.calculator_kafka_stream.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.io.Serializable;

@ToString
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "operation")
@Table(name = "operation")
public class Operation implements Serializable {

    @Id
    @Column(name = "code", updatable = false, nullable = false, unique = true)
    private Integer code;

    @Column(name = "name", nullable = false)
    private String name;

}
