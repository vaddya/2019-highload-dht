package ru.mail.polis.dao.vaddya.naming;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

public interface LeveledFileManager extends FileManager {
    @NotNull
    Path finalPathTo(
            int generation,
            int level);

    @NotNull
    Path tempPathTo(
            int generation,
            int level);

    int levelFromPath(@NotNull Path path);
}
