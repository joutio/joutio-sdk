package com.joutio.axon.grpc.membership;

/** Reçoit la topologie du bus à chaque changement de membres ou de capacités. */
@FunctionalInterface
public interface TopologyListener {

    void onTopologyChanged(Topology topology);
}
