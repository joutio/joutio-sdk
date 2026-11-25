package com.joutio.axon.grpc.membership;

import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.membership.proto.CapabilitiesRequest;
import com.joutio.axon.grpc.membership.proto.MemberCapabilities;
import com.joutio.axon.grpc.membership.proto.MembershipServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sert les capacités du pod local à ses pairs ({@code joutio.membership.MembershipService}). */
public class MembershipGrpcService extends MembershipServiceGrpc.MembershipServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(MembershipGrpcService.class);

    private final LocalMember localMember;
    private final LocalCapabilities localCapabilities;
    private volatile boolean leaving;

    public MembershipGrpcService(LocalMember localMember, LocalCapabilities localCapabilities) {
        this.localMember = localMember;
        this.localCapabilities = localCapabilities;
    }

    @Override
    public void capabilities(CapabilitiesRequest request, StreamObserver<MemberCapabilities> responseObserver) {
        logger.trace("Capabilities requested by {} ({})", request.getClientId(), request.getComponentName());
        responseObserver.onNext(capabilities());
        responseObserver.onCompleted();
    }

    /** Annonce le départ du pod : ses pairs le retirent à leur prochaine lecture, sans rétention. */
    public void leave() {
        leaving = true;
    }

    LocalCapabilities localCapabilities() {
        return localCapabilities;
    }

    MemberCapabilities capabilities() {
        MemberCapabilities.Builder builder = MemberCapabilities.newBuilder()
                .setClientId(localMember.clientId())
                .setComponentName(localMember.componentName());
        if (leaving) {
            return builder.setLeaving(true).build();
        }
        int loadFactor = localCapabilities.loadFactor();
        localCapabilities.commandNames().stream().sorted().forEach(command -> builder.addCommands(
                CommandSubscription.newBuilder()
                        .setCommand(command)
                        .setClientId(localMember.clientId())
                        .setComponentName(localMember.componentName())
                        .setLoadFactor(loadFactor)));
        localCapabilities.queryNames().stream().sorted().forEach(query -> builder.addQueries(
                QuerySubscription.newBuilder()
                        .setQuery(query)
                        .setClientId(localMember.clientId())
                        .setComponentName(localMember.componentName())));
        return builder.build();
    }
}
