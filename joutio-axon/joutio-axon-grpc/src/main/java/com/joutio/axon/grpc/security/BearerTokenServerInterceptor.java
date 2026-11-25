package com.joutio.axon.grpc.security;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Refuse en {@code UNAUTHENTICATED} tout appel entrant sans jeton M2M valide. */
public class BearerTokenServerInterceptor implements ServerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(BearerTokenServerInterceptor.class);

    private final TokenVerifier verifier;

    public BearerTokenServerInterceptor(TokenVerifier verifier) {
        this.verifier = verifier;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        String authorization = headers.get(BearerTokenClientInterceptor.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith(BearerTokenClientInterceptor.BEARER)) {
            return refuse(call, "missing bearer token");
        }
        try {
            verifier.verify(authorization.substring(BearerTokenClientInterceptor.BEARER.length()));
        } catch (RuntimeException e) {
            logger.debug("gRPC call {} refused: {}", call.getMethodDescriptor().getFullMethodName(), e.getMessage());
            return refuse(call, "invalid bearer token");
        }
        return next.startCall(call, headers);
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> refuse(ServerCall<ReqT, RespT> call, String reason) {
        call.close(Status.UNAUTHENTICATED.withDescription(reason), new Metadata());
        return new ServerCall.Listener<>() {
        };
    }
}
