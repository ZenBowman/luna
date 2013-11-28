package com.hulu.metrics.luna;


public class LunaReport {
    private String path;
    private boolean updated;

    public LunaReport() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }
}
