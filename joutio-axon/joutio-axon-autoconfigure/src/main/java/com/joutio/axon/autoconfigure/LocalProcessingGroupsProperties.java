package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Groupes de traitement <b>locaux</b> ({@code joutio.axon.local-groups}) : chaque groupe listé reçoit les événements
 * publiés par <b>ce pod</b>, dans l'unité de travail qui les publie, sans passer par Kafka.
 *
 * <p>Usage : émettre une vue vivante d'un flux d'agrégat ({@code QueryUpdateEmitter}) depuis le pod qui traite la
 * commande. L'émission part au commit de cette unité de travail, quelques millisecondes après l'écriture.</p>
 */
@ConfigurationProperties(prefix = "joutio.axon")
public record LocalProcessingGroupsProperties(List<String> localGroups) {

    public LocalProcessingGroupsProperties {
        if (localGroups == null) {
            localGroups = List.of();
        }
    }
}
