package com.joutio.axon.grpc.membership;

import java.util.Comparator;
import java.util.List;

/** Membres connus du bus, triés par adresse : deux pods qui voient les mêmes membres ont la même topologie. */
public record Topology(List<Member> members) {

    public static final Topology EMPTY = new Topology(List.of());

    public Topology {
        members = members.stream()
                .sorted(Comparator.comparing((Member member) -> member.endpoint().host())
                        .thenComparingInt(member -> member.endpoint().port()))
                .toList();
    }

    /** Membres capables de traiter cette commande. */
    public List<Member> commandHandlers(String commandName) {
        return members.stream().filter(member -> member.commandNames().contains(commandName)).toList();
    }

    /** Membres capables de traiter cette requête. */
    public List<Member> queryHandlers(String queryName) {
        return members.stream().filter(member -> member.queryNames().contains(queryName)).toList();
    }

    boolean sameAs(Topology other) {
        if (members.size() != other.members.size()) {
            return false;
        }
        for (int i = 0; i < members.size(); i++) {
            if (!members.get(i).sameCapabilities(other.members.get(i))) {
                return false;
            }
        }
        return true;
    }
}
