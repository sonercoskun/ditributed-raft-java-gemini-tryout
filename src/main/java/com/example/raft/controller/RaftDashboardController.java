package com.example.raft.controller;

import com.example.raft.service.RaftNodeService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class RaftDashboardController {

    private final RaftNodeService raftNodeService;

    public RaftDashboardController(RaftNodeService raftNodeService) {
        this.raftNodeService = raftNodeService;
    }

    @GetMapping("/dashboard")
    public String dashboard(Model model) {
        model.addAttribute("nodeId", raftNodeService.getNodeId());
        model.addAttribute("role", raftNodeService.getCurrentRole());
        model.addAttribute("term", raftNodeService.getCurrentTerm());
        model.addAttribute("leaderId", raftNodeService.getCurrentLeaderId());
        model.addAttribute("logs", raftNodeService.getLogEntries());
        return "dashboard";
    }
}