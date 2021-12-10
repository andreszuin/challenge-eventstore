package net.intelie.challenges;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implementation of EventStore
 *
 * I chose a ConcurrentHashMap to store the events since it was defined by the
 * challenge that this would be a system hotspot and concurrency would happen
 * frequently.
 *
 * There were other options, like using a synchronized collection to achieve thread-safety,
 * but I thought that since there were going to be a lot of threads trying to access the same
 * collection it was better to allow multiple threads to access it at the same time.
 *
 * While there are other collections that allow concurrency, I chose to use
 * ConcurrentHashMap because it allows for the removal of an Event in any part of the map
 * as long as you have its key, and since there was no need to keep the map ordered
 * I saw no benefit in using a ConcurrentSkipListMap or Set, another collection I could have
 * chosen is CopyOnWriteArrayList, but it has a heavy cost to add elements to the collection.
 *
 * For the insert method I decided to make it synchronized so that only one thread can use it
 * at a time, I did this to keep the method thread-safe not allowing inserts to be overwritten,
 * another way of guaranteeing thread-safety was to make the key variable an AtomicInteger, but
 * with too many threads trying to use it at the same time it start to become inefficient, since
 * it needs to retry more times due to the amount of threads changing its value.
 */
public class EventStoreImplementation implements EventStore{
    private final ConcurrentHashMap<Integer, Event> eventMap = new ConcurrentHashMap<>();
    private int key;

    @Override
    public synchronized void insert(Event event){
        eventMap.put(key++, event);
    }

    @Override
    public void removeAll(String type) {
        List<Integer> keys = eventMap.entrySet().stream().filter(entry -> entry.getValue().type().equals(type)).map(Map.Entry::getKey).collect(Collectors.toList());
        for (Integer key: keys){
            eventMap.remove(key);
        }
    }

    /**
     * Returns the number of events in this store.
     *
     * @return the size of the collection eventMap.
     */
    public Integer numberOfEvents(){
        return this.eventMap.size();
    }

    /**
     * Removes an Event from the store by its key.
     *
     * @param key The key of the Event to be removed.
     */
    public void removeEvent(Integer key){
        this.eventMap.remove(key);
    }

    /**
     * Retrieves an Event cointained in the store by its key.
     *
     * @param key The key of the Event to be retrieved.
     * @return the event in the collection by its key.
     */
    public Event getEvent(Integer key){
        return this.eventMap.get(key);
    }

    /**
     * Verifies if a key is contained in the eventMap collection.
     *
     * @param key The key to be verified.
     * @return true or false based on if the key is contained in the map.
     */
    public boolean containsKey(Integer key){
        return this.eventMap.containsKey(key);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        Map<Integer, Event> eventsInQuery = eventMap.entrySet().stream().filter(entry -> entry.getValue().type().equals(type) && (entry.getValue().timestamp() >= startTime && entry.getValue().timestamp() < endTime)).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
        return new EventIteratorImplementation(eventsInQuery, this);
    }
}
