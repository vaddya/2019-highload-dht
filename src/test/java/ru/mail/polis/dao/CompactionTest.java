/*
 * Copyright 2019 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.dao;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.Files;
import ru.mail.polis.TestBase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compaction tests for {@link DAO} implementations.
 *
 * @author Vadim Tsesko
 */
class CompactionTest extends TestBase {
    @Test
    void overwriteAndCompactExplicitly(@TempDir File data) throws IOException {
        overwrite(data, true);
    }

    @Test
    void overwriteAndCompactImplicitly(@TempDir File data) throws IOException {
        overwrite(data, false);
    }

    private void overwrite(File data, boolean compactExplicitly) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;
        final int overwrites = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKeyBuffer());
        }

        // Overwrite keys several times each time closing DAO
        for (int round = 0; round < overwrites; round++) {
            try (DAO dao = DAOFactory.create(data)) {
                for (final ByteBuffer key : keys) {
                    dao.upsert(key, join(key, value));
                }
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            // Compact
            if (compactExplicitly) {
                dao.compact();
            } else {
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Check the contents
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }
        }

        // Check store size
        final long size = Files.directorySize(data);
        final long minSize = keyCount * (KEY_LENGTH + KEY_LENGTH + valueSize);

        // Heuristic
        assertTrue(size > minSize);
        assertTrue(size < 2 * minSize);
    }

    @Test
    void clear(@TempDir File data) throws IOException {
        // Reference value
        final int valueSize = 1024 * 1024;
        final int keyCount = 10;

        final ByteBuffer value = randomBuffer(valueSize);
        final Collection<ByteBuffer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(randomKeyBuffer());
        }

        // Insert keys
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                dao.upsert(key, join(key, value));
            }
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertEquals(join(key, value), dao.get(key));
            }

            // Remove keys
            for (final ByteBuffer key : keys) {
                dao.remove(key);
            }
        }

        // Compact
        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();
        }

        // Check the contents
        try (DAO dao = DAOFactory.create(data)) {
            for (final ByteBuffer key : keys) {
                assertThrows(NoSuchElementException.class, () -> dao.get(key));
            }
        }

        // Check store size
        final long size = Files.directorySize(data);

        System.out.println("size = " + size);
        // Heuristic
        assertTrue(size < valueSize);
    }
}
