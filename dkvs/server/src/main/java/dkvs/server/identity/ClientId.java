package dkvs.server.identity;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Class that uses a string uuid to be a unique representation of a Client.
 */
public class ClientId implements Comparable<ClientId>, Serializable {

    private final String clientUUID;

    public ClientId(){
        this.clientUUID = UUID.randomUUID().toString();
    }

    public ClientId(String clientId){
        this.clientUUID = clientId;
    }

    public String getClientId() {
        return clientUUID;
    }

    @Override
    public boolean equals(Object obj) {
        return
                obj != null &&
                        this.getClass() == obj.getClass() &&
                        this.getClientId().equals(((ClientId) obj).getClientId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClientId());
    }

    @Override
    public int compareTo(ClientId other) {
        return this.getClientId().compareTo(other.getClientId());
    }

    @Override
    public String toString() {
        return clientUUID;
    }
}
