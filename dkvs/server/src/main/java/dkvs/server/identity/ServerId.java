package dkvs.server.identity;

import java.util.Objects;

/**
 * Class that uses an integer to be a unique representation of a Server.
 */
public class ServerId implements Comparable<ServerId> {

    private final int value;

    public ServerId(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        return
                obj != null &&
                        this.getClass() == obj.getClass() &&
                        this.getValue() == ((ServerId)obj).getValue();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getValue());
    }

    @Override
    public int compareTo(ServerId other) {
        return Integer.compare(this.getValue(), other.getValue());
    }
}
