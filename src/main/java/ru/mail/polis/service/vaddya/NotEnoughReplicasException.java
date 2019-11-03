package ru.mail.polis.service.vaddya;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

final class NotEnoughReplicasException extends RuntimeException {
    private static final long serialVersionUID = -5969658434925957525L;

    private final Collection<Throwable> causes;

    NotEnoughReplicasException(@NotNull final Collection<Throwable> causes) {
        this.causes = causes;
    }

    @Override
    @NotNull
    public String getMessage() {
        return "NotEnoughReplicasException{" + "causes=" + causes + '}';
    }
}
