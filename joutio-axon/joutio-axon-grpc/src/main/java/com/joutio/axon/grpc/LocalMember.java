package com.joutio.axon.grpc;

/**
 * Identité du pod local sur le bus.
 *
 * <p>Le pod se nomme lui-même comme ses pairs le nomment : ses pairs apprennent {@code clientId} et
 * {@code componentName} par {@code MembershipService.Capabilities}, et le reconnaissent par son
 * adresse découverte ({@code endpoint}).</p>
 *
 * @param clientId      nom du pod ({@code HOSTNAME}), unique dans le cluster
 * @param componentName {@code spring.application.name}
 * @param endpoint      adresse annoncée par la découverte : {@code server.address}, sinon IP du pod,
 *                      sinon {@code localhost}, et port gRPC
 */
public record LocalMember(String clientId, String componentName, GrpcEndpoint endpoint) {
}
