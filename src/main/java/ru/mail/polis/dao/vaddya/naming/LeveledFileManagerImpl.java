package ru.mail.polis.dao.vaddya.naming;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class LeveledFileManagerImpl implements LeveledFileManager {
    private static final String FINAL_SUFFIX = ".db";
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String ZERO_LEVEL = "_0";
    private static final Pattern pattern = Pattern.compile("(\\d+)_(\\d+)\\.(\\w+)");
    
    private final File root;

    public LeveledFileManagerImpl(@NotNull final File root) {
        this.root = root;
    }

    @Override
    @NotNull
    public Path tempPathTo(final int generation) {
        return pathTo(generation + ZERO_LEVEL + TEMP_SUFFIX);
    }

    @NotNull
    @Override
    public Path tempPathTo(
            final int generation,
            final int level) {
        return pathTo(generation + "_" + level + TEMP_SUFFIX);
    }

    @Override
    @NotNull
    public Path finalPathTo(final int generation) {
        return pathTo(generation + ZERO_LEVEL + FINAL_SUFFIX);
    }

    @NotNull
    @Override
    public Path finalPathTo(
            final int generation,
            final int level) {
        return pathTo(generation + "_" + level + FINAL_SUFFIX);
    }

    @Override
    public int generationFromPath(@NotNull final Path path) {
        String name = path.getFileName().toString();
        Matcher matcher = pattern.matcher(name);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException();
    }

    @Override
    public int levelFromPath(@NotNull final Path path) {
        String name = path.getFileName().toString();
        Matcher matcher = pattern.matcher(name);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        throw new IllegalArgumentException();
    }

    @NotNull
    @Override
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
