package ru.mail.polis.dao.vaddya.naming;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public interface TableNaming {
    @NotNull
    Path finalPathTo(int generation);

    @NotNull
    Path tempPathTo(int generation);

    int generationFromPath(@NotNull Path path);

    @NotNull
    List<Path> tables();
}
