package ru.mail.polis.dao.vaddya;

import java.util.NoSuchElementException;

final class NoSuchEntityException extends NoSuchElementException {
    NoSuchEntityException(String s) {
        super(s);
    }
    
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
