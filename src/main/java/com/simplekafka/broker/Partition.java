package com.simplekafka.broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition {
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int MEGABYTE = 1024 * 1024;

    private final int id;                     // Unique partition identifier
    private int leader;                       // Leader broker ID
    private List<Integer> followers;          // Follower broker IDs for replication
    private final String baseDir;             // Directory for log storage
    private final AtomicLong nextOffset;      // Next available message offset
    private final ReadWriteLock lock;         // Concurrency control mechanism
    private RandomAccessFile activeLogFile;   // Currently active log file
    private FileChannel activeLogChannel;     // Channel for file operations
    private final List<SegmentInfo> segments; // List of segments in the partition

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();

        initialize();
    }
        
    private void initialize() {
        // Create directory if needed
        try {
            File dir = new File(baseDir);
            if (!dir.exists()){
                dir.mkdirs();
            }

            // Load existing segments
            //scan the directory
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) { //gotta extract base offset
                    String name = file.getName();
                    String segmentName = name.substring(0, name.lastIndexOf("."));
                    long baseOffset = Long.parseLong(segmentName);

                    File indexFile = new File(baseOffset + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo newSegment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(newSegment);
                    }
                }

                // Sort segments by offset
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));

                // Determine next available offset
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }

            // Create a new segment if none exists
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }

            LOGGER.info("Initialized partition " + id + " with " + segments.size() + " segments, next offset: " + nextOffset.get());

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
        
    
        // Open the last segment for appending
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        /*
        Resource management: Properly closes any previously active file handles
        File access mode: Opens files in “rw” (read-write) mode using RandomAccessFile
        Append positioning: Positions the file pointer at the end to ensure append-only behavior
        Channel-based I/O: Uses FileChannel for efficient I/O operations
         */

        //concurrency block? we have to "close" somehow the activeLogFile
        //there is also activeLogChannel, probably useful
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        
        if (activeLogFile != null) {
            activeLogFile.close();
        }

        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");

        //appending positioning
        activeLogChannel = activeLogFile.getChannel();
        activeLogChannel.position(activeLogChannel.size()); //to the end of file
    }

    private void createNewSegment(long baseOffset) throws IOException {
        /*
        File naming convention: Uses a standardized 20-digit format for base offsets, ensuring proper sorting and consistency
        Dual file creation: Creates both log (.log) and index (.index) files for each segment
        Segment tracking: Adds the new segment to the in-memory segments list
        Immediate activation: Opens the new segment for immediate use
         */

        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;

        File logFile = new File(logPath);
        logFile.createNewFile();

        File indexFile = new File(indexPath);
        indexFile.createNewFile();

        SegmentInfo startSegment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(startSegment);

        openSegmentForAppend(startSegment);

        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }

    /**
     * to count messages in segment file
     * 
     * @param lastSegment
     * @return
     * @throws IOException
     */
    public long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long messagesCount = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r")) {
            FileChannel logChannel = logFile.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(4); //size of a message
            while (logChannel.position() < logChannel.size()) {
                buffer.clear();
                int reading = logChannel.read(buffer);
                if (reading < 4) break;
                buffer.flip();
                int messageSize = buffer.getInt();
                logChannel.position(logChannel.position() + messageSize);
                messagesCount++;
            }
        }
        return messagesCount;
    }

    public long append(byte[] message) {
        /*
        Acquires a write lock to prevent concurrent modifications
        Checks if current segment is full (exceeds 1MB) and creates a new one if needed
        Formats message with a 4-byte length prefix
        Persists data using FileChannel for efficient I/O
        Forces data to disk to ensure durability
        Updates the index file with the new offset-position mapping
        Increments and returns the message offset
         */
        lock.writeLock().lock();
        //checking if segment full, create a new one if it is 
        try {
            long currentOffset = nextOffset.get(); // gets the current offset position 
            if (activeLogChannel.position() >= MEGABYTE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset); 
            }

            //we need to add 4 bytes as a prefix to message
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length);
            buffer.put(message);
            buffer.flip();

            //persisting data using FileChannel
            long position = activeLogChannel.position();
            activeLogChannel.write(buffer);

            //forcing data to disk (force(true))
            activeLogChannel.force(true);

            //updating index file with new offset positioning
            //.getIndexPath
            updateIndex(currentOffset, position);

            nextOffset.incrementAndGet();

            return currentOffset;

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndex(long offset, long position) {
        /*
        Index record format: Stores pairs of 8-byte values (offset + position = 16 bytes per entry)
        Append-only index: Always positions at the end of the index file, maintaining chronological order
        Durability: Forces the index to disk with force(true) to ensure persistence
        Current segment focus: Always updates the index of the most recent segment
         */

        //find current segment
        try {
            if (segments.isEmpty()) return;

            SegmentInfo currentSegment = segments.get(segments.size() - 1);

            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw")) {
                FileChannel indexChannel = indexFile.getChannel();

                indexChannel.position(indexChannel.size());

                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.flip();

                indexChannel.write(buffer);
                indexChannel.force(true);
            }     

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }

    }

    public List<byte[]> readMessages(long offset, int maxBytes) {
        /*
        Acquires a read lock (allowing concurrent reads)
        Locates the segment containing the requested offset
        Uses the index to find the exact file position for efficient access
        Reads messages sequentially up to the byte limit
        Handles crossing segment boundaries seamlessly
        Returns messages as byte arrays
         */

        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;

        try {
            //locating segment with requested offset
            //the offset is in the name of the file i think
            //probably check if directory exists
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                return messages;
            }
            //finding exact file position with index

            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0) {
                return messages;
            }
        
            try (RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r")) {
                FileChannel logChannel = logFile.getChannel();
                //we are reading from position until we reach maxBytes
                logChannel.position(position);

                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                long currentOffset = offset;
                
                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    sizeBuffer.clear();
                    int read = logChannel.read(sizeBuffer);
                    if (read < 4) break;
                    sizeBuffer.flip();

                    int messageSize = sizeBuffer.getInt(); //the length of the actual message

                    if (bytesRead + messageSize > maxBytes) {
                        break;
                    }

                    //read message 
                    ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize); //allocating the size of the actual message
                    int messageRead = logChannel.read(messageBuffer);

                    if (messageRead < messageSize) { //checks if there is an incongruency in message size and actual size
                        LOGGER.warning("Incomplete message read at offset " + currentOffset);
                        break;
                    }

                    messageBuffer.flip();

                    //adding message
                    byte[] message = new byte[messageSize];
                    //add content to message[] 
                    messageBuffer.get(message);
                    messages.add(message);

                    //update bytesread and positon
                    bytesRead += messageSize + 4; //4 are the bytes containing size
                    currentOffset++;

                    //handling crossing segment boundaries.
                    if (logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()) { //the offset check is if we are out of message
                        //we can safely continue
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        if (nextSegmentIndex < segments.size()) { //checks if it exists
                            logChannel.close();
                            logFile.close();
                            targetSegment = findSegmentForOffset(currentOffset);
                            RandomAccessFile nextLogFile = new RandomAccessFile(targetSegment.getLogPath(), "r"); //resource leak?
                            FileChannel nextLogChannel = nextLogFile.getChannel();

                            position = 0; //beginning of next segment
                            nextLogChannel.position(position);
                        }
                    }
                }              
            }
        
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read messages from partition: " + id, e);
        } finally {
            lock.readLock().unlock();
        }

        return messages;
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        /*
        Range validation: Checks if the offset is within valid bounds
        Binary search algorithm: Uses O(log n) search to efficiently find the correct segment
        Segment boundary logic: Determines if an offset falls between the current segment’s base offset and the next segment’s base offset
        Special case handling: Has specific logic for the last segment (which doesn’t have an upper bound)
        */

        //which bounds?
        if (segments.isEmpty() && offset >= nextOffset.get()) {
            return null;
        } 

        int low = 0;
        int high = segments.size() - 1;

        //binary search alg
        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);

            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }

            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return null;

    }

    private long findPositionForOffset(SegmentInfo segment, long offset) {
        /*
        Relative offset calculation: Translates global offsets to segment-relative offsets
        Direct index lookup: Uses the relative offset to calculate the exact byte position in the index file
        Special case handling: Handles empty indexes and out-of-range offsets
        Efficient random access: Uses direct file positioning rather than sequential scanning
         */
        //global offset to segment-relative offsets?
        //nextOffset into offset?
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r")) {
            FileChannel indexChannel = indexFile.getChannel();
            
            if (indexChannel.size() == 0) {
                return 0;
            }

            //relative offset within file

            long relativeOffset = offset - segment.getBaseOffset(); //exact index position

            // an index entry is 16 bytes
            long entryCount = indexChannel.size() / 16;

            if (relativeOffset >= entryCount) {
                //its more than the count therefore it doesnt exists yet
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                //once it reads buffer which is size 16 (en entry) 
                buffer.flip();

                buffer.getLong(); //one skip so next getLong() lands in entry position
                return buffer.getLong(); 
            } 

            //what if relative offset is in indexChannel?
            //Read specific index entry

            indexChannel.position(relativeOffset * 16); //16 for evry entry
            ByteBuffer buffer = ByteBuffer.allocate(16);
            indexChannel.read(buffer);
            buffer.flip();

            buffer.getLong();
            return buffer.getLong();

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to find position for offset" + offset, e);
            return -1;
        }

    }

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }
    
    public long getLogEndOffset() {
        return nextOffset.get();
    }

    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }

            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to close partition resources", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;

        public SegmentInfo (long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public String getLogPath() {
            return logPath;
        }

        public String getIndexPath() {
            return indexPath;
        }
        
    }
}
