package ru.mail.polis.igor;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class InMemory implements DAO {

    private ByteBuffer REMOVED  = null; // what if client try to save null?
    private SortedMap<ByteBuffer, ByteBuffer> storage = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return configureIterator(storage.tailMap(from).entrySet().iterator());
    }

    private Iterator<Record> configureIterator(Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator) {
        return new Iterator<Record>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Record next() {
                Map.Entry<ByteBuffer, ByteBuffer> nextEntry = iterator.next();
                if(Objects.equals(nextEntry.getValue(), REMOVED)) {
                    return next();
                }
                return Record.of(nextEntry.getKey(), nextEntry.getValue());
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        storage.put(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        storage.replace(key, REMOVED);
    }

    @Override
    public void close() throws IOException {

    }
}
