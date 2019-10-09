package ru.mail.polis.dao.vaddya;

import java.util.NoSuchElementException;

final class NoSuchEntityException extends NoSuchElementException {
    NoSuchEntityException(final String s) {
        super(s);
    }
    
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
