package net.tcs.exceptions;

public class TCSConfigReadException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TCSConfigReadException(String message) {
        super(message);
    }

    public TCSConfigReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
