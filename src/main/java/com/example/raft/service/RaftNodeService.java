package com.example.raft.service;

import com.example.raft.model.LogEntry;
import com.example.raft.model.NodeRole;
import com.example.raft.model.ClusterConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RaftNodeService {

    @Value("${DISCOVERY_SERVICE_NAME:raft-node}")
    private String discoveryServiceName;

    @Value("${wal.file.path:wal.log}")
    private String walFilePath;

    private String nodeId;
    private volatile NodeRole currentRole = NodeRole.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile String currentLeaderId = null;

    private final List<LogEntry> logEntries = new CopyOnWriteArrayList<>();
    private final Map<String, String> clusterAddressMap = new ConcurrentHashMap<>();
    private final ClusterConfig currentConfig = new ClusterConfig();

    private volatile long lastHeartbeatReceivedTime;
    private long electionTimeoutMs;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;

    private final RestTemplate restTemplate = new RestTemplate();
    private final FileLogger fileLogger = new FileLogger();

    @PostConstruct
    public void init() {
        try {
            this.nodeId = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.nodeId = "node-" + UUID.randomUUID().toString().substring(0, 8);
        }

        resetElectionTimeout();
        this.lastHeartbeatReceivedTime = System.currentTimeMillis();

        // 1. Dinamik Küme Keşfi
        discoverAndJoinCluster();

        // 2. Election Timeout Zamanlayıcısını Başlat
        startElectionTimeoutTimer();

        fileLogger.log("INFO", "RaftNodeService başlatıldı. Node ID: " + nodeId + ", Election Timeout: " + electionTimeoutMs + "ms");
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }

    /* ========================================================================
     * KÜME KEŞFİ VE OTOMATİK KAYIT (CLUSTER DISCOVERY & SELF-REGISTRATION)
     * ======================================================================== */

    private void discoverAndJoinCluster() {
        try {
            InetAddress[] addresses = InetAddress.getAllByName(discoveryServiceName);
            List<String> discoveredNodes = new ArrayList<>();

            for (InetAddress addr : addresses) {
                String hostNameOrIp = addr.getHostAddress();
                discoveredNodes.add(hostNameOrIp);
                clusterAddressMap.put(hostNameOrIp, "http://" + hostNameOrIp + ":8080");
            }

            this.currentConfig.setOldServers(discoveredNodes);
            registerSelfToCluster(discoveredNodes);

        } catch (Exception e) {
            fileLogger.log("WARN", "DNS keşfi sırasında küme düğümleri çözülemedi: " + e.getMessage());
        }
    }

    private void registerSelfToCluster(List<String> nodes) {
        for (String node : nodes) {
            if (node.equals(this.nodeId)) continue;
            try {
                String url = "http://" + node + ":8080/raft/cluster/add-node?nodeId=" + this.nodeId;
                restTemplate.postForEntity(url, null, String.class);
                fileLogger.log("INFO", "Kümeye otomatik kayıt isteği gönderildi -> " + node);
                break;
            } catch (Exception ignored) {
                // Lider olmayan veya henüz hazır olmayan düğümler es geçilir
            }
        }
    }

    /* ========================================================================
     * SEÇİM VE ZAMANLAYICILAR (ELECTION & TIMERS)
     * ======================================================================== */

    private void resetElectionTimeout() {
        this.electionTimeoutMs = 150 + ThreadLocalRandom.current().nextInt(150);
    }

    private synchronized void startElectionTimeoutTimer() {
        if (electionTimeoutTask != null && !electionTimeoutTask.isCancelled()) {
            electionTimeoutTask.cancel(true);
        }

        electionTimeoutTask = scheduler.scheduleAtFixedRate(() -> {
            if (currentRole != NodeRole.LEADER) {
                long elapsed = System.currentTimeMillis() - lastHeartbeatReceivedTime;
                if (elapsed >= electionTimeoutMs) {
                    startElection();
                }
            }
        }, electionTimeoutMs, electionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void startElection() {
        this.currentRole = NodeRole.CANDIDATE;
        this.currentTerm++;
        this.votedFor = this.nodeId;
        this.lastHeartbeatReceivedTime = System.currentTimeMillis();
        resetElectionTimeout();

        fileLogger.log("INFO", "Election timeout doldu. Seçim başlatılıyor... Term: " + currentTerm + ", Node: " + nodeId);

        List<String> activeNodes = getActiveClusterNodes();
        if (activeNodes.isEmpty() || activeNodes.size() == 1) {
            becomeLeader();
            return;
        }

        AtomicInteger votesGranted = new AtomicInteger(1);
        int majorityNeeded = (activeNodes.size() / 2) + 1;

        for (String targetNode : activeNodes) {
            if (targetNode.equals(this.nodeId)) continue;

            CompletableFuture.runAsync(() -> {
                try {
                    String targetUrl = clusterAddressMap.getOrDefault(targetNode, "http://" + targetNode + ":8080");
                    String requestUrl = targetUrl + "/raft/request-vote?term=" + currentTerm + "&candidateId=" + nodeId;

                    Boolean vote = restTemplate.getForObject(requestUrl, Boolean.class);
                    if (Boolean.TRUE.equals(vote)) {
                        int currentVotes = votesGranted.incrementAndGet();
                        if (currentVotes >= majorityNeeded && currentRole == NodeRole.CANDIDATE) {
                            becomeLeader();
                        }
                    }
                } catch (Exception e) {
                    fileLogger.log("WARN", "Oy isteği gönderilemedi -> " + targetNode + ": " + e.getMessage());
                }
            });
        }
    }

    private synchronized void becomeLeader() {
        this.currentRole = NodeRole.LEADER;
        this.currentLeaderId = this.nodeId;
        fileLogger.log("INFO", ">>> LİDER SEÇİLDİ <<< Node: " + nodeId + ", Term: " + currentTerm);

        if (electionTimeoutTask != null) electionTimeoutTask.cancel(true);
        startHeartbeatTimer();
    }

    private synchronized void startHeartbeatTimer() {
        if (heartbeatTask != null && !heartbeatTask.isCancelled()) {
            heartbeatTask.cancel(true);
        }

        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        if (currentRole != NodeRole.LEADER) return;

        List<String> activeNodes = getActiveClusterNodes();
        for (String targetNode : activeNodes) {
            if (targetNode.equals(this.nodeId)) continue;

            CompletableFuture.runAsync(() -> {
                try {
                    String targetUrl = clusterAddressMap.getOrDefault(targetNode, "http://" + targetNode + ":8080");
                    String requestUrl = targetUrl + "/raft/heartbeat?leaderId=" + nodeId + "&term=" + currentTerm;
                    restTemplate.postForEntity(requestUrl, null, Void.class);
                } catch (Exception e) {
                    fileLogger.log("WARN", "Heartbeat gönderilemedi -> " + targetNode);
                }
            });
        }
    }

    /* ========================================================================
     * RPC HANDLER METOTLARI
     * ======================================================================== */

    public synchronized boolean handleVoteRequest(long candidateTerm, String candidateId) {
        if (candidateTerm > this.currentTerm) {
            stepDown(candidateTerm);
        }

        if (candidateTerm == this.currentTerm && (this.votedFor == null || this.votedFor.equals(candidateId))) {
            this.votedFor = candidateId;
            this.lastHeartbeatReceivedTime = System.currentTimeMillis();
            fileLogger.log("INFO", "Oy verildi -> Candidate: " + candidateId + ", Term: " + candidateTerm);
            return true;
        }

        return false;
    }

    public synchronized void handleHeartbeat(String leaderId, long leaderTerm) {
        if (leaderTerm >= this.currentTerm) {
            if (leaderTerm > this.currentTerm || currentRole != NodeRole.FOLLOWER) {
                stepDown(leaderTerm);
            }
            this.currentLeaderId = leaderId;
            this.lastHeartbeatReceivedTime = System.currentTimeMillis();
        }
    }

    private synchronized void stepDown(long newTerm) {
        this.currentTerm = newTerm;
        this.currentRole = NodeRole.FOLLOWER;
        this.votedFor = null;
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        startElectionTimeoutTimer();
        fileLogger.log("INFO", "Follower moduna geçildi. Yeni Term: " + newTerm);
    }

    /* ========================================================================
     * LOG YAZMA (WAL) VE JOINT CONSENSUS İŞLEMLERİ
     * ======================================================================== */

    public synchronized boolean executeCommand(String command) {
        if (currentRole != NodeRole.LEADER) {
            fileLogger.log("WARN", "Yazma komutu reddedildi. Bu düğüm Leader değil! Aktif Leader: " + currentLeaderId);
            return false;
        }

        LogEntry entry = new LogEntry(logEntries.size() + 1, currentTerm, command, false);
        logEntries.add(entry);
        writeToWal(entry);

        fileLogger.log("INFO", "Yeni log lider tarafından kabul edildi: " + command);
        return true;
    }

    public synchronized void addNodeToCluster(String newNodeId) {
        if (currentRole != NodeRole.LEADER) {
            fileLogger.log("WARN", "Düğüm ekleme isteği reddedildi. Leader olunması gerekiyor.");
            return;
        }

        List<String> currentNodes = new ArrayList<>(currentConfig.getOldServers());
        if (!currentNodes.contains(newNodeId)) {
            List<String> newNodes = new ArrayList<>(currentNodes);
            newNodes.add(newNodeId);

            currentConfig.setNewServers(newNodes);
            currentConfig.setInJointConsensus(true);

            LogEntry jointLog = new LogEntry(logEntries.size() + 1, currentTerm, "JOINT_CONFIG:" + newNodes, true);
            logEntries.add(jointLog);
            writeToWal(jointLog);

            clusterAddressMap.put(newNodeId, "http://" + newNodeId + ":8080");
            currentConfig.setOldServers(newNodes);
            currentConfig.setInJointConsensus(false);

            fileLogger.log("INFO", "Yeni düğüm Joint Consensus ile kümeye eklendi: " + newNodeId);
        }
    }

    private void writeToWal(LogEntry entry) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(walFilePath, true))) {
            writer.write(String.format("%d,%d,%s,%b\n",
                    entry.getIndex(),
                    entry.getTerm(),
                    entry.getCommand(),
                    entry.isConfiguration()));
        } catch (IOException e) {
            fileLogger.log("ERROR", "WAL dosyasına yazılırken hata oluştu: " + e.getMessage());
        }
    }

    /* ========================================================================
     * GETTER'LAR VE LOG YARDIMCISI
     * ======================================================================== */

    private List<String> getActiveClusterNodes() {
        return currentConfig.getOldServers() != null && !currentConfig.getOldServers().isEmpty()
                ? currentConfig.getOldServers()
                : new ArrayList<>(clusterAddressMap.keySet());
    }

    public String getNodeId() { return nodeId; }
    public NodeRole getCurrentRole() { return currentRole; }
    public long getCurrentTerm() { return currentTerm; }
    public String getCurrentLeaderId() { return currentLeaderId; }
    public List<LogEntry> getLogEntries() { return logEntries; }

    private static class FileLogger {
        public void log(String level, String message) {
            System.out.printf("[%s] [%s] %s%n", level, System.currentTimeMillis(), message);
        }
    }
}