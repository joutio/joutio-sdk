package com.joutio.axon.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Adresse du pod local telle que la découverte l'annonce : {@code server.address}, sinon l'IP du pod
 * (découverte Kubernetes), sinon {@code localhost} (instances simples en local).
 *
 * <p>Elle doit correspondre à l'hôte de l'instance découverte, sinon le pod ne se reconnaît pas parmi
 * ses pairs. Ne pas la déduire de la présence d'un {@code SimpleDiscoveryClient} : Spring Cloud
 * Commons en crée toujours un, même en prod.</p>
 */
final class LocalAddress {

    private static final Logger logger = LoggerFactory.getLogger(LocalAddress.class);

    private LocalAddress() {
    }

    static String resolve(Environment environment) {
        String configuredHost = environment.getProperty("server.address");
        if (StringUtils.hasText(configuredHost)) {
            return configuredHost;
        }
        boolean kubernetesDiscovery = environment.getProperty(
                "spring.cloud.kubernetes.discovery.enabled", Boolean.class, false);
        if (!kubernetesDiscovery) {
            return "localhost";
        }
        String localIp = environment.getProperty("spring.cloud.client.ip-address");
        if (StringUtils.hasText(localIp)) {
            return localIp;
        }
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException exception) {
            logger.warn("Could not resolve local host address, falling back to localhost: {}",
                    exception.getMessage());
            return "localhost";
        }
    }
}
