package com.example.raft.service;

import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class RaftFileLogger {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private final List<String> systemLogs = Collections.synchronizedList(new ArrayList<>());

    public void log(String level, String message) {
        String now = LocalTime.now().format(TIME_FORMATTER);
        String formatted = String.format("[%s] [%s] %s", now, level, message);
        systemLogs.add(formatted);
        if (systemLogs.size() > 500) {
            systemLogs.remove(0);
        }
        System.out.println(formatted);
    }

    public List<String> getRecentSystemLogs(int limit) {
        synchronized (systemLogs) {
            int size = systemLogs.size();
            if (size <= limit) {
                return new ArrayList<>(systemLogs);
            }
            return new ArrayList<>(systemLogs.subList(size - limit, size));
        }
    }
}