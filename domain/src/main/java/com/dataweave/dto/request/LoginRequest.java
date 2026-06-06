package com.dataweave.dto.request;


import com.dataweave.dto.enums.SerializationType;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;


@Data
public class LoginRequest {

    @Min(value = 1, message = "Number of events to kafka send must be at-least 1")
    private long eventCount;

    @Min(value = 0, message = "Sleep in millisecond cannot be negative")
    private long sleepMs;

    @NotNull(message = "topic is required")
    private String topic;

    @NotNull(message = "topic is required")
    private String batchId;

    @NotNull(message = "serializationType is required")
    private SerializationType serializationType;

    private Object jsonPayload;
}
