package dkvs.shared.exceptions;

public class GetException extends Exception{
    public GetException(Exception e){
        super(e.getMessage());
    }
}
