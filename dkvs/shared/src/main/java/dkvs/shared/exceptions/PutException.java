package dkvs.shared.exceptions;

public class PutException extends Exception {
    public PutException(Exception e) {
        super(e.getMessage());
    }
}