package com.example.raft.model;

public class LogEntry {
    private long index;
    private long term;
    private String command;
    private boolean isConfiguration;

    public LogEntry() {}

    public LogEntry(long index, long term, String command, boolean isConfiguration) {
        this.index = index;
        this.term = term;
        this.command = command;
        this.isConfiguration = isConfiguration;
    }

    public long getIndex() { return index; }
    public void setIndex(long index) { this.index = index; }

    public long getTerm() { return term; }
    public void setTerm(long term) { this.term = term; }

    public String getCommand() { return command; }
    public void setCommand(String command) { this.command = command; }

    public boolean isConfiguration() { return isConfiguration; }
    public void setConfiguration(boolean configuration) { isConfiguration = configuration; }
}