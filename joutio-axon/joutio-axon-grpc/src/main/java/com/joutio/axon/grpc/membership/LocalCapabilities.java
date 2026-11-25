package com.joutio.axon.grpc.membership;

import java.util.Set;

/** Handlers servis par le pod local, annoncés à ses pairs. Lu à chaque appel de {@code Capabilities}. */
public interface LocalCapabilities {

    /** Charge relative du pod dans l'anneau des commandes ({@code load_factor} d'Axon). */
    int loadFactor();

    Set<String> commandNames();

    Set<String> queryNames();
}
