package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.distributed.commandfilter.CommandNameFilter;
import org.axonframework.commandhandling.distributed.commandfilter.DenyAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LocalCommandCapabilitiesTest {

    @Test
    void reads_the_command_names_announced_by_the_distributed_command_bus() {
        // DistributedCommandBus.subscribe : DenyAll.or(nom) puis CommandNameFilter.or(nom) → union des noms.
        var filter = DenyAll.INSTANCE.or(new CommandNameFilter("StartGameCommand"))
                .or(new CommandNameFilter("AnswerQuestionCommand"));

        assertThat(LocalCommandCapabilities.namesOf(filter))
                .isEqualTo(Set.of("StartGameCommand", "AnswerQuestionCommand"));
        assertThat(LocalCommandCapabilities.namesOf(DenyAll.INSTANCE)).isEmpty();
    }
}
