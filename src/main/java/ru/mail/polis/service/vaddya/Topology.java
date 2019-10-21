package ru.mail.polis.service.vaddya;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.jetbrains.annotations.NotNull;

/**
 * Calculates the data owner for the key
 */
public interface Topology<T> {

    @NotNull
    T primaryFor(@NotNull ByteBuffer key);

    boolean isMe(@NotNull T node);

    @NotNull
    Collection<T> all();
}
