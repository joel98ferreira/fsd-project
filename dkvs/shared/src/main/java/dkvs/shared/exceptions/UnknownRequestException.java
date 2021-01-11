package dkvs.shared.exceptions;

public class UnknownRequestException extends Exception {
    public UnknownRequestException(Exception e) {
        super(e.getMessage());
    }
}