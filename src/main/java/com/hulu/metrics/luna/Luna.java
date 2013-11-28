package com.hulu.metrics.luna;


import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.channels.FileLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Luna {
    private static final String LUNA_PATH_SEPARATOR = "__";
    private static final String LUNA_DIR = ".luna";
    private static final String LOCK_EXT = ".luna_lock";

    public static File lunaHome() {
        File homeDirectory = new File(System.getProperty("user.home"));
        return new File(homeDirectory, LUNA_DIR);
    }

    public static byte[] shaFromInputStream(InputStream is) throws IOException, NoSuchAlgorithmException {
        final MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] dataBytes = new byte[1024];
        while (is.read(dataBytes) != -1) {
            md.update(dataBytes);
        }
        return md.digest();
    }

    public static byte[] shaFromRemoteFile(FileSystem fs, Path hdfsPath) throws NoSuchAlgorithmException, IOException {
        final FSDataInputStream dataInputStream = fs.open(hdfsPath);
        final byte[] dataBytes = shaFromInputStream(dataInputStream);
        dataInputStream.close();
        return dataBytes;
    }

    public static byte[] shaFromLocalFile(File file) throws NoSuchAlgorithmException, IOException {
        final FileInputStream fis = new FileInputStream(file);
        final byte[] dataBytes = shaFromInputStream(fis);
        fis.close();
        return dataBytes;
    }

    /*
     * Inputs: Path to a tarball on HDFS
     * Outputs: The path, as a string, to the local directory to which the tarball has been extracted
     */
    public static String getTarballAsDirectory(String filePath) throws IOException, NoSuchAlgorithmException {
        return lgetTarballAsDirectory(null, filePath).getPath();
    }

    /*
     * Inputs: Path to a file on HDFS
     * Outputs: The path, as a string, to the local file that the HDFS file has been copied to
     */
    public static String getFile(String filePath) throws IOException, NoSuchAlgorithmException {
        return lgetFile(null, filePath).getPath();
    }

    /*
     * Inputs: Path to the hadoop filesystem namenode, path to a tarball on HDFS
     * Outputs: The path, as a string, to the local directory to which the tarball has been extracted
     */
    public static String getTarballAsDirectory(String hadoopFileSystemPath, String filePath) throws IOException, NoSuchAlgorithmException {
        return lgetTarballAsDirectory(hadoopFileSystemPath, filePath).getPath();
    }

    /*
     * Inputs: Path to the hadoop filesystem namenode, path to a file on HDFS
     * Outputs: The path, as a string, to the local file that the HDFS file has been copied to
     */
    public static String getFile(String hadoopFileSystemPath, String filePath) throws IOException, NoSuchAlgorithmException {
        return lgetFile(hadoopFileSystemPath, filePath).getPath();
    }

    @VisibleForTesting
    public static File convertToLunaPath(String hdfsPath) {
        return new File(lunaHome(), hdfsPath.replaceAll("\\/", LUNA_PATH_SEPARATOR));
    }

    private static LunaReport lgetTarballAsDirectory(String hadoopFileSystemPath, String filePath) throws IOException, NoSuchAlgorithmException {
        final LunaReport fileReport = lgetFile(hadoopFileSystemPath, filePath);
        final String dirPath = fileReport.getPath() + ".dir";
        final File directory = new File(dirPath);

        if (fileReport.isUpdated() || (!directory.exists())) {
            if (directory.exists()) {
                boolean couldDelete = directory.delete();
                if (!couldDelete) {
                    throw new IOException("Unable to delete old directory: " + dirPath);
                }
            }

            boolean directoryMade = directory.mkdir();
            if (!directoryMade) {
                throw new IOException("Unable to create directory: " + dirPath);
            }


            Runtime.getRuntime().exec(String.format("tar -xf %s --directory=%s", fileReport.getPath(), dirPath));
        }

        fileReport.setPath(dirPath);
        return fileReport;
    }


    private static LunaReport lgetFile(String hadoopFileSystemPath, String filePath) throws IOException, NoSuchAlgorithmException {
        final Configuration conf = new Configuration();
        if (hadoopFileSystemPath != null) {
            conf.set("fs.defaultFS", hadoopFileSystemPath);
        }
        final FileSystem fs = FileSystem.get(conf);
        final LunaReport report = new LunaReport();

        final Path hadoopFilePath = new Path(filePath);
        if (!fs.exists(hadoopFilePath)) {
            throw new FileNotFoundException(filePath);
        }

        makeLunaDirectoryIfNotExists();
        final String localFilePath = Luna.convertToLunaPath(filePath).getAbsolutePath();
        report.setPath(localFilePath);
        final File localFile = new File(localFilePath);
        final RandomAccessFile lockFilePath = new RandomAccessFile(localFilePath + LOCK_EXT, "rw");

        // This prevents simultaneous mapper processes from trying to update the same file at the same time
        final FileLock lock = lockFilePath.getChannel().lock();

        if (!localFile.exists()) {
            fs.copyToLocalFile(hadoopFilePath, new Path(localFilePath));
            System.out.println(String.format("Copied %s to %s", hadoopFilePath, localFilePath));
            report.setUpdated(true);
        } else {
            final byte[] remoteSha = shaFromRemoteFile(fs, hadoopFilePath);
            final byte[] localSha = shaFromLocalFile(localFile);
            if (Arrays.equals(remoteSha, localSha)) {
                System.out.println(String.format("Local file %s and remote file %s are identical, update not required", localFilePath, hadoopFilePath));
            } else {
                fs.copyToLocalFile(hadoopFilePath, new Path(localFilePath));
                System.out.println(String.format("Copied %s to %s", hadoopFilePath, localFilePath));
                report.setUpdated(true);
            }
        }

        lock.release();

        return report;
    }

    private static void makeLunaDirectoryIfNotExists() throws IOException {
        final File lunaDir = lunaHome();
        if (!lunaDir.exists()) {
            if (!lunaDir.mkdir()) {
                throw new IOException("Could not create luna home directory");
            }
        }
    }

}
