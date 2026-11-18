package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.distributed.CommandMessageFilter;
import org.axonframework.extensions.springcloud.commandhandling.mode.DefaultMemberCapabilities;
import org.axonframework.extensions.springcloud.commandhandling.mode.MemberCapabilities;
import org.axonframework.extensions.springcloud.commandhandling.mode.CapabilityDiscoveryMode;
import org.axonframework.extensions.springcloud.commandhandling.mode.ServiceInstanceClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Capacités des membres du bus de commandes, stables malgré un échec HTTP passager.
 *
 * <p>Chaque émetteur reconstruit son anneau de hachage cohérent à chaque heartbeat en lisant les
 * capacités de toutes les instances découvertes. Avec l'{@code IgnoreListingDiscoveryMode} d'Axon,
 * une seule lecture en échec (pod qui démarre, identity qui redémarre et ne valide plus le JWT de
 * l'appel) exclut l'instance pendant 1 min, chez cet émetteur seulement : les émetteurs n'ont plus
 * le même anneau, deux pods traitent le même agrégat, et leurs caches d'agrégats divergent.</p>
 *
 * <p>Ici, une instance encore découverte garde ses dernières capacités connues tant que la dernière
 * lecture réussie date de moins de {@code retention} : erreur client (4xx) comme erreur réseau ou 5xx
 * (que le mode REST rend en {@code INCAPABLE_MEMBER}). Une instance jamais lue garde le comportement
 * d'Axon.</p>
 */
public class LastKnownCapabilitiesDiscoveryMode implements CapabilityDiscoveryMode {

    private static final Logger logger = LoggerFactory.getLogger(LastKnownCapabilitiesDiscoveryMode.class);

    private final CapabilityDiscoveryMode delegate;
    private final Duration retention;
    private final Clock clock;
    private final Map<InstanceKey, Known> known = new ConcurrentHashMap<>();

    public LastKnownCapabilitiesDiscoveryMode(CapabilityDiscoveryMode delegate, Duration retention) {
        this(delegate, retention, Clock.systemUTC());
    }

    LastKnownCapabilitiesDiscoveryMode(CapabilityDiscoveryMode delegate, Duration retention, Clock clock) {
        this.delegate = delegate;
        this.retention = retention;
        this.clock = clock;
    }

    @Override
    public void updateLocalCapabilities(ServiceInstance localInstance,
                                        int loadFactor,
                                        CommandMessageFilter commandFilter) {
        delegate.updateLocalCapabilities(localInstance, loadFactor, commandFilter);
    }

    @Override
    public Optional<MemberCapabilities> capabilities(ServiceInstance serviceInstance) {
        InstanceKey key = InstanceKey.of(serviceInstance);
        Instant now = clock.instant();
        Optional<MemberCapabilities> capabilities;
        try {
            capabilities = delegate.capabilities(serviceInstance);
        } catch (ServiceInstanceClientException e) {
            return lastKnown(key, now, e.getMessage()).or(Optional::empty);
        }
        // Le mode REST rend INCAPABLE_MEMBER (aucune commande acceptée) sur une erreur réseau ou 5xx.
        if (capabilities.isPresent() && capabilities.get() == DefaultMemberCapabilities.INCAPABLE_MEMBER) {
            return lastKnown(key, now, "instance injoignable").or(() -> capabilities);
        }
        capabilities.ifPresentOrElse(value -> known.put(key, new Known(value, now)), () -> known.remove(key));
        return capabilities;
    }

    private Optional<MemberCapabilities> lastKnown(InstanceKey key, Instant now, String reason) {
        Known last = known.get(key);
        if (last != null && now.isBefore(last.fetchedAt().plus(retention))) {
            logger.debug("Capacités de {} illisibles, dernières connues conservées ({})", key, reason);
            return Optional.of(last.capabilities());
        }
        if (known.remove(key) != null) {
            logger.info("Capacités de {} illisibles depuis plus de {}, instance retirée de l'anneau ({})",
                    key, retention, reason);
        }
        return Optional.empty();
    }

    private record Known(MemberCapabilities capabilities, Instant fetchedAt) {
    }

    private record InstanceKey(String serviceId, String host, int port) {

        static InstanceKey of(ServiceInstance instance) {
            return new InstanceKey(instance.getServiceId(), instance.getHost(), instance.getPort());
        }
    }
}
