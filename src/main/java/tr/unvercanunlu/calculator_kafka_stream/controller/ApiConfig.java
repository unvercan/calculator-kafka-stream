package tr.unvercanunlu.calculator_kafka_stream.controller;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ApiConfig {

    public static final String VERSION = "v1";

    public static final String BASE = "/" + "api" + "/" + VERSION;

    public static final String OPERATION_API = BASE + "/" + "operation";

    public static final String CALCULATION_API = BASE + "/" + "calculation";

}
