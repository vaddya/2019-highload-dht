package ru.mail.polis.dao.vaddya.naming;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public interface FileManager {
    @NotNull
    Path tempPathTo(int generation);

    @NotNull
    Path finalPathTo(int generation);

    int generationFromPath(@NotNull Path path);

    @NotNull
    List<Path> listTables();
}
