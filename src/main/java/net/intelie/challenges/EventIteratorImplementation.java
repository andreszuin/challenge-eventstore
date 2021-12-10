package net.intelie.challenges;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of EventIterator
 *
 * In this implementation of EventIterator I chose to create a new Map containing
 * only the Events from the Query, this way it is possible to iterate concurrently
 * on the Events contained in the store, if an Iterator removes an Event from
 * the store, another Iterator that was created before this removal will still be
 * able to see the Event with its current() method, this helps to keep it thread-safe,
 * allowing a thread to access all the Events contained in the Iterator without
 * throwing an Exception or Error.
 *
 * I also made it so that when remove() is called there is a need to call moveNext()
 * once again before calling remove() or current(), I made this decision so that this
 * Iterator would be in compliance with Javas documentation of its own Iterators.
 */
public class EventIteratorImplementation implements EventIterator{
    private final Map<Integer, Event> events;
    private final List<Integer> keys;
    private int currentKeysIndex; //the index of the List keys, not the values of the keys inside the List.
    private int currentEventKey; //the key of the event the Iterator is currently referencing.
    private boolean moveNextReturnedFalse;
    private boolean hasCalledMoveNext;
    private final EventStoreImplementation store;


    public EventIteratorImplementation(Map<Integer, Event> events, EventStoreImplementation store) {
        this.events = events;
        this.keys = new ArrayList<>(events.keySet());
        this.store = store;
    }

    @Override
    public boolean moveNext() {
        this.hasCalledMoveNext = true;
        try{
            this.currentEventKey = keys.get(this.currentKeysIndex++);
            return true;
        } catch (IndexOutOfBoundsException ex){
            this.moveNextReturnedFalse = true;
            return false;
        }
    }

    @Override
    public Event current() {
        if (this.moveNextReturnedFalse || !this.hasCalledMoveNext){
            throw new IllegalStateException();
        }
        return this.events.get(this.currentKeysIndex);
    }

    @Override
    public void remove() {
        if (this.moveNextReturnedFalse || !this.hasCalledMoveNext){
            throw new IllegalStateException();
        }
        this.store.removeEvent(this.currentEventKey);
        this.hasCalledMoveNext = false;
    }

    @Override
    public void close(){
        this.keys.clear();
        this.events.clear();
    }
}
