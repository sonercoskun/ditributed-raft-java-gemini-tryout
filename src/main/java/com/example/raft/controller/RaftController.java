package com.example.raft.controller;

import com.example.raft.model.LogEntry;
import com.example.raft.model.NodeRole;
import com.example.raft.service.RaftNodeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNodeService raftNodeService;

    public RaftController(RaftNodeService raftNodeService) {
        this.raftNodeService = raftNodeService;
    }

    @GetMapping("/request-vote")
    public ResponseEntity<Boolean> requestVote(@RequestParam long term, @RequestParam String candidateId) {
        boolean voteGranted = raftNodeService.handleVoteRequest(term, candidateId);
        return ResponseEntity.ok(voteGranted);
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> heartbeat(@RequestParam String leaderId, @RequestParam long term) {
        raftNodeService.handleHeartbeat(leaderId, term);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/command")
    public ResponseEntity<String> executeCommand(@RequestBody Map<String, String> body) {
        String command = body.get("command");
        boolean success = raftNodeService.executeCommand(command);

        if (success) {
            return ResponseEntity.ok("Komut başarıyla loglandı.");
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("İstek reddedildi. Bu düğüm LEADER değil. Aktif Leader: " + raftNodeService.getCurrentLeaderId());
        }
    }

    @PostMapping("/cluster/add-node")
    public ResponseEntity<String> addNode(@RequestParam String nodeId) {
        raftNodeService.addNodeToCluster(nodeId);
        return ResponseEntity.ok("Düğüm ekleme isteği işleme alındı: " + nodeId);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(Map.of(
                "nodeId", raftNodeService.getNodeId(),
                "role", raftNodeService.getCurrentRole(),
                "term", raftNodeService.getCurrentTerm(),
                "leaderId", String.valueOf(raftNodeService.getCurrentLeaderId()),
                "logsCount", raftNodeService.getLogEntries().size()
        ));
    }
}