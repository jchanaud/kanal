package io.kanal.runner.engine.entities;

import lombok.Getter;

@Getter
public abstract class SourceStage extends Stage {
    protected CacheSupport cacheSupport;
    protected boolean isCacheSource;

    public SourceStage(String name, CacheSupport cacheSupport) {
        super(name);
        this.cacheSupport = cacheSupport;
        this.isCacheSource = false;
    }

    /**
     * Marks this source stage as a cache source. If the stage does not support being a cache source,
     * an UnsupportedOperationException is thrown.
     */
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
