package com.joutio.axon.grpc.security;

import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Ajoute le jeton M2M du pod ({@code authorization: Bearer …}) à chaque appel sortant.
 *
 * <p>Le jeton est obtenu sur l'exécuteur applicatif de gRPC, jamais sur le thread de l'appelant : son
 * renouvellement (appel HTTP au serveur d'autorisation) ne bloque ni un processor ni une saga.</p>
 */
public class BearerTokenClientInterceptor implements ClientInterceptor {

    static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    static final String BEARER = "Bearer ";

    private final CallCredentials credentials;

    public BearerTokenClientInterceptor(Supplier<String> tokenSupplier) {
        this.credentials = new BearerTokenCredentials(tokenSupplier);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
        return next.newCall(method, callOptions.withCallCredentials(credentials));
    }

    private static final class BearerTokenCredentials extends CallCredentials {

        private final Supplier<String> tokenSupplier;

        private BearerTokenCredentials(Supplier<String> tokenSupplier) {
            this.tokenSupplier = tokenSupplier;
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            appExecutor.execute(() -> {
                try {
                    Metadata headers = new Metadata();
                    headers.put(AUTHORIZATION, BEARER + tokenSupplier.get());
                    applier.apply(headers);
                } catch (RuntimeException e) {
                    applier.fail(Status.UNAUTHENTICATED.withDescription("M2M token unavailable").withCause(e));
                }
            });
        }
    }
}
