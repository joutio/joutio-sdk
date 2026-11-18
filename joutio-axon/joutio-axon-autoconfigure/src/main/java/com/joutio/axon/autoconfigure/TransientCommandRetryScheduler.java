package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.NoHandlerForCommandException;
import org.axonframework.commandhandling.distributed.CommandDispatchException;
import org.axonframework.commandhandling.gateway.RetryScheduler;
import org.axonframework.messaging.RemoteHandlingException;
import org.axonframework.modelling.command.ConcurrencyException;

import java.util.List;
import java.util.Set;

/**
 * Nouvel essai d'une commande, limité aux échecs passagers du bus distribué.
 *
 * <ul>
 *   <li>{@link ConcurrencyException} : conflit de séquence, typiquement un agrégat en cache périmé
 *   après un changement de topologie (Axon l'évince au rollback, l'essai suivant le recharge) ;</li>
 *   <li>{@link CommandDispatchException}, {@link NoHandlerForCommandException} : destination
 *   injoignable ou anneau en cours de reconstruction.</li>
 * </ul>
 *
 * <p>Une exception distante n'arrive que sous forme de description ({@link RemoteHandlingException}) :
 * on y cherche le nom de ces classes. Un refus métier n'est jamais réessayé.</p>
 */
public class TransientCommandRetryScheduler implements RetryScheduler {

    private static final Set<Class<? extends Throwable>> TRANSIENT = Set.of(
            ConcurrencyException.class,
            CommandDispatchException.class,
            NoHandlerForCommandException.class
    );

    private final RetryScheduler delegate;

    /** @param delegate planificateur qui considère tout échec comme passager (intervalles, nombre d'essais). */
    public TransientCommandRetryScheduler(RetryScheduler delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean scheduleRetry(CommandMessage commandMessage,
                                 RuntimeException lastFailure,
                                 List<Class<? extends Throwable>[]> failures,
                                 Runnable commandDispatch) {
        return isTransient(lastFailure) && delegate.scheduleRetry(commandMessage, lastFailure, failures, commandDispatch);
    }

    static boolean isTransient(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            Throwable candidate = current;
            if (TRANSIENT.stream().anyMatch(type -> type.isInstance(candidate))) {
                return true;
            }
            if (current instanceof RemoteHandlingException remote && remote.getExceptionDescriptions().stream()
                    .anyMatch(description -> TRANSIENT.stream()
                            .anyMatch(type -> description.startsWith(type.getName())))) {
                return true;
            }
        }
        return false;
    }
}
