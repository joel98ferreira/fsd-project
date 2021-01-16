package dkvs.server.network;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class that represents a scalar logical clock and provides an API
 * to manipulate the clock based on the events.
 */
public class ScalarLogicalClock implements Comparable<ScalarLogicalClock>{

    private int counter;

    public ScalarLogicalClock() {
        this.counter = 0;
    }

    public int getCounter(){
        return counter;
    }

    public void localEvent(){
        this.counter++;
    }

    public void sendEvent(){
        this.counter++;
    }

    public void receiveEvent(ScalarLogicalClock logicalClock){
        this.counter = Math.max(logicalClock.counter, this.counter) + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScalarLogicalClock that = (ScalarLogicalClock) o;
        return this.counter == that.counter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(counter);
    }


    @Override
    public int compareTo(ScalarLogicalClock logicalClock) {

        //  0 -> The clocks are equal
        // -1 -> The time from the received clock is less than mine
        //  1 -> The time from the received clock is higher than mine
        return Integer.compare(logicalClock.counter, this.counter);
    }

    @Override
    public String toString() {
        return "" + counter;
    }

    /**
     * Represents the possible scalar logical clocks events.
     */
    public enum Event {

        SEND(1),    // Send Event
        RECEIVE(2), // Receive event
        LOCAL(3);   // Local event


        private final int type;

        Event(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        private final static Map<Integer, Event> map = new HashMap<>();

        static {
            for (Event type : Event.values()) {
                map.put(type.getType(), type);
            }
        }

        static Event getEvent(int type) {
            return map.get(type);
        }
    }
}
