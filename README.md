luna
====

Luna is a local filesystem cache for HDFS that was originally developed at Hulu. It was designed to allow the elimination of local filesystem dependencies within a cluster.

If a datanode on the cluster relies on a local file, that file can simply be stored on HDFS, and Luna can be used to facilitate the transfer of the file to the local filesystem when it gets out of date.

There are two methods in the Luna API:

```
 public static String getFile(String hadoopFileSystemPath, String filePath)
```

This method looks for a file in HDFS, and then pulls it into the local filesystem if it is out of date, returning the path to the local file.

```
 public static String getTarballAsDirectory(String hadoopFileSystemPath, String filePath)
```

This method looks for a tarball in HDFS, pulls it into the local filesystem if it is out of date, untars it, and returns the path to the local directory.

Versioning is done by taking a SHA-1 hash of the remote file and comparing it to the hash of the local file (I should probably change this to MD5)
