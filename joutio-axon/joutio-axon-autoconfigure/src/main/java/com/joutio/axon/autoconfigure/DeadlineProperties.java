package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Deadlines courtes en mémoire ({@code joutio.axon.deadline.*}).
 *
 * <p>{@code threads} (4) : threads du {@code SimpleDeadlineManager}. Axon n'en crée qu'un par défaut ; chaque
 * déclenchement charge une saga en transaction et envoie souvent une commande à un autre pod. Sous charge, un seul
 * thread mettait des secondes de retard sur toutes les échéances de partie (réponse du bot perdue, 2026-11-19).
 * L'ordre reste garanti par saga : Axon verrouille chaque instance de saga.</p>
 */
@ConfigurationProperties(prefix = "joutio.axon.deadline")
public record DeadlineProperties(Integer threads) {

    public DeadlineProperties {
        threads = threads == null ? 4 : threads;
        if (threads < 1) {
            throw new IllegalArgumentException("joutio.axon.deadline.threads must be >= 1");
        }
    }
}
