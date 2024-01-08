package xyz.mattring.crystan.service;

import java.net.InetAddress;

public interface SvcNode {
    default String getNodeId() {
        // class name - hashcode of hostname
        return this.getClass().getSimpleName()
                + "-" + InetAddress.getLoopbackAddress().getHostName().hashCode();
    }
}
