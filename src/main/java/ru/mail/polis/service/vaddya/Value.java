package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.vaddya.TableEntry;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Comparator;

@ThreadSafe
final class Value {
    private static final Value ABSENT = new Value(State.ABSENT, -1, null);

    private final State state;
    private final long ts;
    @Nullable
    private final byte[] data;

    @NotNull
    static Value present(
            @NotNull final byte[] data,
            final long ts) {
        return new Value(State.PRESENT, ts, data);
    }

    @NotNull
    static Value removed(final long ts) {
        return new Value(State.REMOVED, ts, null);
    }

    @NotNull
    static Value absent() {
        return ABSENT;
    }

    @NotNull
    static Value fromEntry(@Nullable final TableEntry entry) {
        if (entry == null) {
            return Value.absent();
        } else if (entry.hasTombstone()) {
            return Value.removed(entry.ts());
        }
        return Value.present(ByteBufferUtils.unwrapBytes(entry.getValue()), entry.ts());
    }

    @NotNull
    static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.state() != Value.State.ABSENT)
                .max(Comparator.comparing(Value::ts))
                .orElseGet(Value::absent);
    }

    private Value(
            @NotNull final State state,
            final long ts,
            @Nullable final byte[] data) {
        this.state = state;
        this.ts = ts;
        this.data = data == null ? null : data.clone();
    }

    State state() {
        return state;
    }

    long ts() {
        return ts;
    }

    @NotNull
    public byte[] data() {
        if (data == null) {
            throw new IllegalStateException("Trying to get null data");
        }
        return data.clone();
    }

    @Override
    public String toString() {
        return state.toString() + ", ts=" + ts;
    }

    enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
