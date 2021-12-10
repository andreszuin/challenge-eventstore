package net.intelie.challenges;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void addEvents() { //tests the insert() method of the store
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        assertEquals(2, (int) store.numberOfEvents());
    }

    @Test
    public void removeAllOfType() { //tests the removeAll() method of the store
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 111L));
        store.removeAll("some_type");
        assertEquals(1, (int) store.numberOfEvents());
        assertEquals("other_type", store.getEvent(1).type());
    }

    @Test
    public void iterate() { //tests the creation of an Iterator and its moveNext() method.
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            iterator.moveNext();
            assertEquals("some_type", iterator.current().type());
            assertEquals(111L,iterator.current().timestamp());
            iterator.moveNext();
            assertEquals("some_type", iterator.current().type());
            assertEquals(113L, iterator.current().timestamp());
        }catch (Exception ex){
            System.out.println("An exception has occurred: " + ex);
        }
    }

    @Test
    public void removeWithIterator() { //tests the remove() method of an Iterator
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            iterator.moveNext();
            iterator.remove();
        }catch (Exception ex){
            System.out.println("An exception has occurred: " + ex);
        }
        assertEquals(2, (int) store.numberOfEvents());
        assertFalse(store.containsKey(0));
    }

    @Test
    public void callCurrentNoNextCalled() { //forces the Exception caused by calling current() without calling moveNext() first
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            Event event = iterator.current();
        }catch (Exception ex){
            //System.out.println("An exception has occurred: " + ex);
            assertEquals(IllegalStateException.class, ex.getClass());
        }
    }

    @Test
    public void callCurrentNextReturnedFalse() { //forces the Exception caused by calling current() with moveNext() having returned false
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            iterator.moveNext();
            iterator.moveNext();
            iterator.moveNext();
            Event event = iterator.current();
        }catch (Exception ex){
            //System.out.println("An exception has occurred: " + ex);
            assertEquals(IllegalStateException.class, ex.getClass());
        }
    }

    @Test
    public void removeWithIteratorNoNextCalled() { //forces the Exception caused by calling remove() without calling moveNext() first
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            iterator.remove();
        }catch (Exception ex){
            //System.out.println("An exception has occurred: " + ex);
            assertEquals(IllegalStateException.class, ex.getClass());
        }
    }

    @Test
    public void removeWithIteratorNextReturnedFalse() { //forces the Exception caused by calling remove() with moveNext() having returned false
        EventStoreImplementation store = new EventStoreImplementation();
        store.insert(new Event("some_type", 111L));
        store.insert(new Event("other_type", 112L));
        store.insert(new Event("some_type", 113L));
        try(EventIterator iterator = store.query("some_type",110L, 114L)){
            iterator.moveNext();
            iterator.moveNext();
            iterator.moveNext();
            iterator.remove();
        }catch (Exception ex){
            //System.out.println("An exception has occurred: " + ex);
            assertEquals(IllegalStateException.class, ex.getClass());
        }
    }


    @Test
    public void addEventsMultiThreaded() throws Exception { //tests the stores insert() method with multithreading
        int numberofThreads = 16;
        ExecutorService service = Executors.newFixedThreadPool(numberofThreads);
        CountDownLatch latch = new CountDownLatch(numberofThreads);
        EventStoreImplementation store = new EventStoreImplementation();
        for (int i = 0; i < numberofThreads; i++){
            service.submit(() -> {
                        try{
                            for (int j = 0; j < 100; j++){
                                if (j%2 == 0){
                                    store.insert(new Event("some_type", 111L));
                                }
                                else {

                                    store.insert(new Event("other_type", 111L));
                                }
                            }
                        } catch (Exception ex){
                            System.out.println("An exception has occurred: " + ex);
                        }
                        latch.countDown();
                    }
            );
        }
        latch.await();
        assertEquals(1600, (int) store.numberOfEvents());
    }

    @Test
    public void threadsInputThenRemoveType() throws Exception { //tests the removeAll() method from the store with multithreading
        int numberofThreads = 16;
        ExecutorService service = Executors.newFixedThreadPool(numberofThreads);
        CountDownLatch latch = new CountDownLatch(numberofThreads);
        EventStoreImplementation store = new EventStoreImplementation();
        for (int i = 0; i < numberofThreads; i++){
            service.submit(() -> {
                        try{
                            for (int j = 0; j < 100; j++){
                                if (j%2 == 0){
                                    store.insert(new Event("some_type", 111L));
                                }
                                else {

                                    store.insert(new Event("other_type", 111L));
                                }
                            }
                        } catch (Exception ex){
                            System.out.println("An exception has occurred: " + ex);
                        }
                        latch.countDown();
                    }
            );
        }
        latch.await();
        assertEquals(1600, (int) store.numberOfEvents());


        CountDownLatch latch2 = new CountDownLatch(numberofThreads);
        for (int i = 0; i < numberofThreads; i++){
            service.submit(() -> {
                        try{
                            store.removeAll("other_type");
                        } catch (Exception ex){
                            System.out.println("An exception has occurred: " + ex);
                        }
                        latch2.countDown();
                    }
            );
        }
        latch2.await();
        assertEquals(800, (int) store.numberOfEvents());
    }

    @Test
    public void threadsInputQueryThenRemoveFirst() throws Exception { //tests the remove() method from an Iterator with multithreading
        int numberofThreads = 16;
        ExecutorService service = Executors.newFixedThreadPool(numberofThreads);
        CountDownLatch latch = new CountDownLatch(numberofThreads);
        EventStoreImplementation store = new EventStoreImplementation();
        for (int i = 0; i < numberofThreads; i++){
            service.submit(() -> {
                        try{
                            long even = 110L;
                            long odd = 110L;
                            for (int j = 0; j < 6; j++){
                                if (j%2 == 0){
                                    store.insert(new Event("some_type", even++));
                                }
                                else {

                                    store.insert(new Event("other_type", odd++));
                                }
                            }
                        } catch (Exception ex){
                            System.out.println("An exception has occurred: " + ex);
                        }
                        latch.countDown();
                    }
            );
        }
        latch.await();
        int items = (int) store.numberOfEvents();
        assertEquals(96, items);

        CountDownLatch latch2 = new CountDownLatch(numberofThreads);
        for (int i = 0; i < numberofThreads; i++){
            service.submit(() -> {
                        try(EventIterator iterator = store.query("other_type",110L, 113L)) {
                            iterator.moveNext();
                            iterator.remove();
                        } catch (Exception ex){
                            System.out.println("An exception has occurred: " + ex);
                        }
                        latch2.countDown();
                    }
            );
        }
        latch2.await();
        assertTrue(items > (int) store.numberOfEvents());
    }
}