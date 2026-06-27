import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * קובץ הרצה מרכזי להשוואת הביצועים בין KiWi רגיל ל-KiWi משופר (הפרויקט שלכם).
 * הקובץ מכיל את שתי הגרסאות ואת מערכת המדידה (Benchmark).
 */
public class KiWiBenchmark {

    // ממשק משותף לשתי המפות כדי שנוכל להריץ עליהן טסט זהה
    interface IKiWiMap {
        void put(int key, int value);
        void remove(int key);
    }

    // =========================================================
    // גרסה 1: KiWiStandard (KiWi רגיל ללא אופטימיזציה)
    // =========================================================
    static class KiWiStandard implements IKiWiMap {
        private final AtomicLong globalVersion = new AtomicLong(0);
        private final Chunk headChunk = new Chunk();

        static class Entry {
            int key, value;
            long version;
            boolean isTombstone;

            Entry(int key, int value, long version, boolean isTombstone) {
                this.key = key; this.value = value; this.version = version; this.isTombstone = isTombstone;
            }
        }

        class Chunk {
            static final int CAPACITY = 100;
            Entry[] entries = new Entry[CAPACITY];
            int size = 0;
            final ReentrantLock lock = new ReentrantLock();

            public void add(int key, int value, boolean isTombstone) {
                lock.lock();
                try {
                    // ב-KiWi רגיל: אם מלא, ישר מפצלים! (אין דחיסה)
                    if (size == CAPACITY) {
                        split();
                    }
                    entries[size++] = new Entry(key, value, globalVersion.incrementAndGet(), isTombstone);
                } finally {
                    lock.unlock();
                }
            }

            public void split() {
                try {
                    Thread.sleep(5); // עונש: פעולת הפיצול היא איטית מאוד
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                
                // זורקים חצי מהמידע כדי לדמות העברה לגוש חדש
                int newSize = size / 2;
                for (int i = 0; i < newSize; i++) {
                    entries[i] = entries[i + newSize];
                }
                size = newSize;
            }
        }

        public void put(int key, int value) { headChunk.add(key, value, false); }
        public void remove(int key) { headChunk.add(key, 0, true); }
    }


    // =========================================================
    // גרסה 2: KiWiOptimized (הפרויקט שלכם - עם מחיקה עצלה)
    // =========================================================
    static class KiWiOptimized implements IKiWiMap {
        private final AtomicLong globalVersion = new AtomicLong(0);
        // נניח שזה הזמן של הסורק הכי ישן. לצורך הדוגמה אנחנו מניחים שאפשר לנקות הכל כי אין סריקות כרגע
        private final AtomicLong oldestActiveScanVersion = new AtomicLong(Long.MAX_VALUE);
        private final Chunk headChunk = new Chunk();

        static class Entry {
            int key, value;
            long version;
            boolean isTombstone;

            Entry(int key, int value, long version, boolean isTombstone) {
                this.key = key; this.value = value; this.version = version; this.isTombstone = isTombstone;
            }
        }

        class Chunk {
            static final int CAPACITY = 100;
            Entry[] entries = new Entry[CAPACITY];
            int size = 0;
            final ReentrantLock lock = new ReentrantLock();

            public void add(int key, int value, boolean isTombstone) {
                lock.lock();
                try {
                    if (size == CAPACITY) {
                        // הפיתוח שלכם: קודם כל מנסים לדחוס ולנקות זבל!
                        int freedSpace = compact(oldestActiveScanVersion.get());
                        
                        // רק אם הדחיסה לא עזרה והגוש באמת מלא במידע אמיתי, נעשה פיצול איטי
                        if (freedSpace == 0) {
                            split();
                        }
                    }
                    entries[size++] = new Entry(key, value, globalVersion.incrementAndGet(), isTombstone);
                } finally {
                    lock.unlock();
                }
            }

            // פונקציית הניקוי שלכם
            public int compact(long minActiveVersion) {
                int writeIndex = 0;
                int originalSize = size;

                for (int readIndex = 0; readIndex < size; readIndex++) {
                    Entry current = entries[readIndex];
                    // אם זו מצבה ישנה, מדלגים עליה (היא נמחקת פיזית)
                    if (current.isTombstone && current.version < minActiveVersion) {
                        continue; 
                    }
                    entries[writeIndex++] = current;
                }
                size = writeIndex;
                for (int i = size; i < originalSize; i++) entries[i] = null; // לעזור ל-GC
                
                return originalSize - size; 
            }

            public void split() {
                try {
                    Thread.sleep(5); // עונש
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                
                int newSize = size / 2;
                for (int i = 0; i < newSize; i++) entries[i] = entries[i + newSize];
                size = newSize;
            }
        }

        public void put(int key, int value) { headChunk.add(key, value, false); }
        public void remove(int key) { headChunk.add(key, 0, true); }
    }


    // =========================================================
    // ה-Benchmark (הרצת המבחנים)
    // =========================================================
    public static void main(String[] args) throws Exception {
        int threads = 8;
        int durationMs = 2000; // 2 שניות לכל מבחן
        int addRatio = 50; // 50% הוספות, 50% מחיקות

        System.out.println("==================================================");
        System.out.println("   KiWi Deletion Optimization Benchmark");
        System.out.println("   Threads: " + threads + " | Duration: " + durationMs + "ms");
        System.out.println("   Workload: " + addRatio + "% Puts, " + (100 - addRatio) + "% Removes");
        System.out.println("==================================================\n");

        // הרצת KiWi רגיל
        System.out.println("Running Test 1: Standard KiWi (No Compaction)...");
        double stdThroughput = runTest(new KiWiStandard(), threads, durationMs, addRatio);
        System.out.printf("-> Standard KiWi Throughput: %,.0f ops/sec\n\n", stdThroughput);

        // הרצת KiWi משופר
        System.out.println("Running Test 2: Optimized KiWi (With Lazy Deletion)...");
        double optThroughput = runTest(new KiWiOptimized(), threads, durationMs, addRatio);
        System.out.printf("-> Optimized KiWi Throughput: %,.0f ops/sec\n\n", optThroughput);

        // סיכום תוצאות
        System.out.println("==================================================");
        System.out.println("   RESULTS SUMMARY");
        System.out.println("==================================================");
        double improvement = (optThroughput / stdThroughput) * 100 - 100;
        System.out.printf("Optimized version is %.0f%% faster under heavy deletion workload!\n", improvement);
    }

    private static double runTest(IKiWiMap map, int threads, int durationMs, int addRatio) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(threads + 1);
        LongAdder done = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean(false);
        int BATCH = 100;
        
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                try {
                    barrier.await();
                } catch (Exception e) { return; }
                
                long local = 0;
                while (!stop.get()) {
                    for (int k = 0; k < BATCH; k++) {
                        int key = rnd.nextInt(1000); // טווח מפתחות
                        if (rnd.nextInt(100) < addRatio) {
                            map.put(key, rnd.nextInt(1000));
                        } else {
                            map.remove(key);
                        }
                    }
                    local += BATCH;
                }
                done.add(local);
            });
            ts[i].start();
        }
        
        barrier.await(); 
        long start = System.nanoTime();
        Thread.sleep(durationMs); 
        stop.set(true); 
        
        for (Thread t : ts) {
            t.join();
        }
        
        long elapsed = System.nanoTime() - start;
        long ops = done.sum();
        return ops / (elapsed / 1e9); 
    }
}