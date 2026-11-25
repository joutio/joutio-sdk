package com.joutio.axon.autoconfigure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.commandhandling.distributed.CommandMessageFilter;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.commandfilter.CommandNameFilter;
import org.axonframework.commandhandling.distributed.commandfilter.DenyAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Commandes traitées par le pod local et sa charge, annoncées à ses pairs par le bus gRPC.
 *
 * <p>Alimenté par le {@code DistributedCommandBus} d'Axon, qui publie à chaque abonnement d'un handler
 * un {@link CommandNameFilter} (union des noms de commandes) et le {@code load_factor}. Les noms sont lus
 * dans la forme JSON du filtre ({@code commandNames}), celle qu'Axon échange déjà entre pods pour la
 * découverte REST des capacités.</p>
 *
 * <p>Transitoire : quand le bus de commandes passera en gRPC ({@code sdk-grpc-commandes}), le connecteur
 * recevra directement chaque nom de commande.</p>
 */
public class LocalCommandCapabilities {

    private static final Logger logger = LoggerFactory.getLogger(LocalCommandCapabilities.class);
    private static final ObjectMapper FILTER_MAPPER = new ObjectMapper();

    private volatile int loadFactor = DistributedCommandBus.INITIAL_LOAD_FACTOR;
    private volatile Set<String> commandNames = Set.of();

    public void update(int loadFactor, CommandMessageFilter filter) {
        this.loadFactor = loadFactor;
        this.commandNames = namesOf(filter);
    }

    public int loadFactor() {
        return loadFactor;
    }

    public Set<String> commandNames() {
        return commandNames;
    }

    static Set<String> namesOf(CommandMessageFilter filter) {
        if (filter instanceof DenyAll) {
            return Set.of();
        }
        if (!(filter instanceof CommandNameFilter)) {
            logger.warn("Unexpected command filter {}, no command announced on the gRPC bus", filter);
            return Set.of();
        }
        JsonNode names = FILTER_MAPPER.valueToTree(filter).path("commandNames");
        Set<String> result = new HashSet<>();
        names.forEach(name -> result.add(name.asText()));
        return Set.copyOf(result);
    }
}
