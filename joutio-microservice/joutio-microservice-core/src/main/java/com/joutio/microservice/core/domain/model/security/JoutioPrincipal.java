package com.joutio.microservice.core.domain.model.security;

import java.security.Principal;

public interface JoutioPrincipal extends Principal {

    /**
     * @return L'identifiant unique de l'utilisateur dans Joutio
     */
    String getUserId();

    /**
     * @return L'email de l'utilisateur
     */
    String getEmail();

    @Override
    default String getName() {
        return getUserId();
    }
}
