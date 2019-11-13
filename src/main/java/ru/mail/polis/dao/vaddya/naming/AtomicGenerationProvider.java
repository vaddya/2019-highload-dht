package ru.mail.polis.dao.vaddya.naming;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public final class AtomicGenerationProvider implements GenerationProvider {
    private final AtomicInteger currentGeneration;

    public AtomicGenerationProvider() {
        this.currentGeneration = new AtomicInteger();
    }

    @Override
    public int nextGeneration() {
        return currentGeneration.getAndIncrement();
    }

    @Override
    public void setNextGeneration(final int generation) {
        currentGeneration.set(generation);
    }
}
