package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public class Partition {
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final int id;                     // Unique partition identifier
    private int leader;                       // Leader broker ID
    private List<Integer> followers;          // Follower broker IDs for replication
    private final String baseDir;             // Directory for log storage
    private final AtomicLong nextOffset;      // Next available message offset
    private final ReadWriteLock lock;         // Concurrency control mechanism
    private RandomAccessFile activeLogFile;   // Currently active log file
    private FileChannel activeLogChannel;     // Channel for file operations
    private final List<SegmentInfo> segments; // List of segments in the partition
        
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
            }


        } catch () {

        }
        
        
        
        
        // Open the last segment for appending
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

        //open segment for use
        
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
