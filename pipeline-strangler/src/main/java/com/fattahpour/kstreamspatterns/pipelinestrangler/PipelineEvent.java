package com.fattahpour.kstreamspatterns.pipelinestrangler;

public record PipelineEvent(String id, String payload, String correlationId) {}
