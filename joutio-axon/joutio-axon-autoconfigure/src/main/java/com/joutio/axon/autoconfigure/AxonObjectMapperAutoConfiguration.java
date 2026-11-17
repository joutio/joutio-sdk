package com.joutio.axon.autoconfigure;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.axonframework.springboot.autoconfig.ObjectMapperAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * {@code ObjectMapper} du transport Axon (événements, commandes, requêtes), retenu par Axon sous le nom
 * {@code defaultAxonObjectMapper} : la configuration Jackson de l'application, plus la tolérance aux constantes
 * d'énumération inconnues, lues comme {@code null}. Un producteur peut ainsi publier une nouvelle valeur avant que ses
 * consommateurs ne la connaissent ; le consommateur traite {@code null} comme une valeur inconnue. L'{@code ObjectMapper}
 * principal (API REST) reste strict.
 */
@AutoConfiguration(after = JacksonAutoConfiguration.class, before = {ObjectMapperAutoConfiguration.class, AxonAutoConfiguration.class})
@ConditionalOnClass(Jackson2ObjectMapperBuilder.class)
public class AxonObjectMapperAutoConfiguration {

    public static final String AXON_OBJECT_MAPPER = "defaultAxonObjectMapper";

    @Bean(AXON_OBJECT_MAPPER)
    @ConditionalOnMissingBean(name = AXON_OBJECT_MAPPER)
    public ObjectMapper defaultAxonObjectMapper(Jackson2ObjectMapperBuilder builder) {
        return builder.build()
                .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    }
}
