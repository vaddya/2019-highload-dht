package ru.mail.polis.service.vaddya;

import com.google.common.base.Charsets;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

final class ByteBufferUtils {
    private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

    private ByteBufferUtils() {
    }

    @NotNull
    static ByteBuffer wrapString(@NotNull final String str) {
        return wrapString(str, DEFAULT_CHARSET);
    }

    @NotNull
    static ByteBuffer wrapString(
            @NotNull final String str,
            @NotNull final Charset charset) {
        return ByteBuffer.wrap(str.getBytes(charset));
    }

    @NotNull
    static byte[] unwrapBytes(@NotNull final ByteBuffer buffer) {
        final var duplicate = buffer.duplicate();
        final var bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
