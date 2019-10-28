package ru.mail.polis.service.vaddya;

import java.util.Collection;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.vaddya.TableEntry;

final class Value {
    private static final Value ABSENT = new Value(null, -1, State.ABSENT);

    private final byte[] data;
    private final long ts;
    private final State state;

    static Value present(byte[] data, long ts) {
        return new Value(data, ts, State.PRESENT);
    }

    static Value removed(long ts) {
        return new Value(null, ts, State.REMOVED);
    }

    static Value absent() {
        return ABSENT;
    }

    static Value fromEntry(@Nullable final TableEntry entry) {
        if (entry == null) {
            return Value.absent();
        } else if (entry.hasTombstone()) {
            return Value.removed(entry.ts());
        }
        return Value.present(ByteBufferUtils.unwrapBytes(entry.getValue()), entry.ts());
    }

    @NotNull
    static Value mergeValues(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.state() != Value.State.ABSENT)
                .max(Comparator.comparing(Value::ts))
                .orElseGet(Value::absent);
    }

    private Value(
            @Nullable final byte[] data,
            final long ts,
            @NotNull State state) {
        this.data = data;
        this.ts = ts;
        this.state = state;
    }

    public byte[] data() {
        return data;
    }

    long ts() {
        return ts;
    }

    State state() {
        return state;
    }

    enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
