package com.example.raft.model;

import java.util.ArrayList;
import java.util.List;

public class ClusterConfig {
    private List<String> oldServers = new ArrayList<>();
    private List<String> newServers = new ArrayList<>();
    private boolean inJointConsensus = false;

    public ClusterConfig() {}

    public List<String> getOldServers() { return oldServers; }
    public void setOldServers(List<String> oldServers) { this.oldServers = oldServers; }

    public List<String> getNewServers() { return newServers; }
    public void setNewServers(List<String> newServers) { this.newServers = newServers; }

    public boolean isInJointConsensus() { return inJointConsensus; }
    public void setInJointConsensus(boolean inJointConsensus) { this.inJointConsensus = inJointConsensus; }
}