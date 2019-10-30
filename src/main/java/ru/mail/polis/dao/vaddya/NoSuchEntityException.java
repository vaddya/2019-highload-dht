package ru.mail.polis.dao.vaddya;

import java.util.NoSuchElementException;

final class NoSuchEntityException extends NoSuchElementException {
    private static final long serialVersionUID = -2879302788125098643L;

    NoSuchEntityException(final String s) {
        super(s);
    }
    
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
