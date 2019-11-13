package ru.mail.polis.dao.vaddya.naming;

public interface GenerationProvider {
    int nextGeneration();

    void setNextGeneration(int generation);
}
