package ru.mail.polis.dao.vaddya.naming;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class BasicFileManager implements FileManager {
    private static final String FINAL_SUFFIX = ".db";
    private static final String TEMP_SUFFIX = ".tmp";

    private final File root;

    public BasicFileManager(@NotNull final File root) {
        this.root = root;
    }

    @Override
    @NotNull
    public Path tempPathTo(final int generation) {
        return pathTo(generation + TEMP_SUFFIX);
    }
    
    @Override
    @NotNull
    public Path finalPathTo(final int generation) {
        return pathTo(generation + FINAL_SUFFIX);
    }

    @Override
    public int generationFromPath(@NotNull final Path path) {
        final var name = path.getFileName().toString();
        if (name.length() <= FINAL_SUFFIX.length()) {
            throw new IllegalArgumentException("File name is too short");
        }
        final var substring = name.substring(0, name.length() - FINAL_SUFFIX.length());
        return Integer.parseInt(substring);
    }

    @Override
    @NotNull
    public List<Path> listTables() {
        return Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .filter(s -> s.endsWith(FINAL_SUFFIX))
                .map(s -> Path.of(root.getAbsolutePath(), s))
                .collect(toList());
    }

    @NotNull
    private Path pathTo(@NotNull final String name) {
        return Path.of(root.getAbsolutePath(), name);
    }
}
