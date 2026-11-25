package com.joutio.axon.grpc.security;

/** Vérifie le jeton porteur d'un appel entrant ; lève une exception s'il est refusé. */
@FunctionalInterface
public interface TokenVerifier {

    void verify(String token);
}
