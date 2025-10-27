package io.kanal.runner.engine.entities;

import lombok.Getter;

@Getter
public abstract class SourceStage extends Stage {
    protected CacheSupport cacheSupport;
    protected boolean isCacheSource = false;

    public SourceStage(String name, CacheSupport cacheSupport) {
        super(name);
        this.cacheSupport = cacheSupport;
    }

    public void setCacheSource() {
        if (cacheSupport == CacheSupport.SUPPORTED)
            this.isCacheSource = true;
        else
            throw new UnsupportedOperationException("Stage [" + name + "] can't be used as cache source");
    }

    public abstract void poll();

    public abstract boolean allCaughtUp();

    public enum CacheSupport {
        SUPPORTED,
        NOT_SUPPORTED
    }
}
