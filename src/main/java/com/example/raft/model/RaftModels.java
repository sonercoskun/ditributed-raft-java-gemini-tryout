package com.example.raft.model;

import java.util.List;

public class RaftModels {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public record LogEntry(int index, int term, String fileName, String content) {}

    public record HeartbeatRequest(String leaderId, int term, int leaderCommitIndex) {}

    public record VoteRequest(String candidateId, int term) {}

    public record JoinRequest(String nodeId, String nodeUrl) {}

    public record StatusResponse(
            String nodeId,
            Role role,
            String leader,
            int currentTerm,
            int activePeersCount,
            int totalLogsCount,
            List<LogEntry> logs
    ) {}
}