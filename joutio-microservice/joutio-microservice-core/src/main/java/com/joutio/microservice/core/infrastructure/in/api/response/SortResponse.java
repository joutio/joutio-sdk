package com.joutio.microservice.core.infrastructure.in.api.response;

import com.joutio.microservice.core.domain.model.search.SortDirection;

import java.io.Serializable;

public record SortResponse(
        String property,
        SortDirection direction
) implements Serializable {
}
