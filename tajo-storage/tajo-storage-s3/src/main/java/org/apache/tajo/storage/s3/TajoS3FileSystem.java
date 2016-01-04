/*
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
package org.apache.tajo.storage.s3;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.util.Progressable;
import org.apache.tajo.conf.TajoConf;
import org.weakref.jmx.internal.guava.collect.AbstractSequentialIterator;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.toArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.http.HttpStatus.*;
import static org.apache.tajo.storage.s3.RetryDriver.retry;

/**
 * Borrow from com.facebook.presto.hive.PrestoS3FileSystem
 */
public class TajoS3FileSystem extends FileSystem {
  private static final Log log = LogFactory.getLog(TajoS3FileSystem.class);

  private static final TajoS3FileSystemStats STATS = new TajoS3FileSystemStats();
  private static final TajoS3FileSystemMetricCollector METRIC_COLLECTOR
    = new TajoS3FileSystemMetricCollector(STATS);

  public static TajoS3FileSystemStats getFileSystemStats() {
    return STATS;
  }

  private static final String DIRECTORY_SUFFIX = "_$folder$";

  private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
  private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);

  private final TransferManagerConfiguration transferConfig = new TransferManagerConfiguration();

  private URI uri;
  private Path workingDirectory;
  private AmazonS3 s3;
  private File stagingDirectory;
  private int maxAttempts;
  private Duration maxBackoffTime;
  private Duration maxRetryTime;
  private boolean useInstanceCredentials;

  private CannedAccessControlList cannedACL;
  private String serverSideEncryptionAlgorithm;

  private long partSize;
  private int partSizeThreshold;

  private static final String FOLDER_SUFFIX = "_$folder$";

  private TajoConf conf;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    requireNonNull(uri, "uri is null");
    requireNonNull(conf, "conf is null");
    super.initialize(uri, conf);
    setConf(conf);

    this.conf = new TajoConf(conf);

    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDirectory = new Path("/").makeQualified(this.uri, new Path("/"));

    this.stagingDirectory = new File(this.conf.getVar(TajoConf.ConfVars.STAGING_ROOT_DIR));

    this.maxAttempts = this.conf.getIntVar(TajoConf.ConfVars.S3_MAX_CLIENT_RETRIES);
    this.maxBackoffTime = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_MAX_BACKOFF_TIME));
    this.maxRetryTime = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_MAX_RETRY_TIME));

    int maxErrorRetries = this.conf.getIntVar(TajoConf.ConfVars.S3_MAX_ERROR_RETRIES);
    boolean sslEnabled = this.conf.getBoolVar(TajoConf.ConfVars.S3_SSL_ENABLED);

    Duration connectTimeout = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_CONNECT_TIMEOUT));
    Duration socketTimeout = Duration.valueOf(this.conf.getVar(TajoConf.ConfVars.S3_SOCKET_TIMEOUT));
    int maxConnections = this.conf.getIntVar(TajoConf.ConfVars.S3_MAX_CONNECTIONS);

    long minFileSize = this.conf.getLongVar(TajoConf.ConfVars.S3_MULTIPART_MIN_FILE_SIZE);
    long minPartSize = this.conf.getLongVar(TajoConf.ConfVars.S3_MULTIPART_MIN_PART_SIZE);

    this.useInstanceCredentials = this.conf.getBoolVar(TajoConf.ConfVars.S3_USE_INSTANCE_CREDENTIALS);

    serverSideEncryptionAlgorithm = conf.get(SERVER_SIDE_ENCRYPTION_ALGORITHM);

    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
    if (!cannedACLName.isEmpty()) {
      cannedACL = CannedAccessControlList.valueOf(cannedACLName);
    } else {
      cannedACL = null;
    }

    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    partSizeThreshold = conf.getInt(MIN_MULTIPART_THRESHOLD,
      DEFAULT_MIN_MULTIPART_THRESHOLD);

    if (partSize < 5 * 1024 * 1024) {
      log.error(MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    if (partSizeThreshold < 5 * 1024 * 1024) {
      log.error(MIN_MULTIPART_THRESHOLD + " must be at least 5 MB");
      partSizeThreshold = 5 * 1024 * 1024;
    }

    ClientConfiguration configuration = new ClientConfiguration()
      .withMaxErrorRetry(maxErrorRetries)
      .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
      .withConnectionTimeout(Ints.checkedCast(connectTimeout.toMillis()))
      .withSocketTimeout(Ints.checkedCast(socketTimeout.toMillis()))
      .withMaxConnections(maxConnections);

    this.s3 = createAmazonS3Client(uri, conf, configuration);

    transferConfig.setMultipartUploadThreshold(minFileSize);
    transferConfig.setMinimumUploadPartSize(minPartSize);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    workingDirectory = path;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    STATS.newListStatusCall();
    List<LocatedFileStatus> list = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
    while (iterator.hasNext()) {
        list.add(iterator.next());
    }
    return toArray(list, LocatedFileStatus.class);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path) {
    STATS.newListLocatedStatusCall();
    return new RemoteIterator<LocatedFileStatus>() {
      private final Iterator<LocatedFileStatus> iterator = listPrefix(path);

      @Override
      public boolean hasNext() throws IOException {
        try {
          return iterator.hasNext();
        }
        catch (AmazonClientException e) {
          throw new IOException(e);
        }
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        try {
          return iterator.next();
        }
        catch (AmazonClientException e) {
          throw new IOException(e);
        }
      }
    };
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (path.getName().isEmpty()) {
      // the bucket root requires special handling
      if (getS3ObjectMetadata(path) != null) {
        return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
      }
      throw new FileNotFoundException("File does not exist: " + path);
    }

    ObjectMetadata metadata = getS3ObjectMetadata(path);

    if (metadata == null) {
      // check if this path is a directory
      Iterator<LocatedFileStatus> iterator = listPrefix(path);
      if (iterator.hasNext()) {
        return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
      }
      throw new FileNotFoundException("File does not exist: " + path);
    }

    return new FileStatus(metadata.getContentLength(), false, 1, BLOCK_SIZE.toBytes(), lastModifiedTime(metadata),
      qualifiedPath(path));
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(
      new BufferedFSInputStream(
        new TajoS3InputStream(s3, uri.getHost(), path, maxAttempts, maxBackoffTime, maxRetryTime),
          bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
    short replication, long blockSize, Progressable progress) throws IOException {
    if ((!overwrite) && exists(path)) {
      throw new IOException("File already exists:" + path);
    }

    createDirectories(stagingDirectory.toPath());
    File tempFile = createTempFile(stagingDirectory.toPath(), "tajo-s3-", ".tmp").toFile();

    String key = keyFromPath(qualifiedPath(path));
    return new FSDataOutputStream(
      new TajoS3OutputStream(s3, transferConfig, uri.getHost(), key, tempFile),
        statistics);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
    throw new UnsupportedOperationException("append");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean srcDirectory;
    try {
      srcDirectory = directory(src);
    } catch (FileNotFoundException e) {
      return false;
    }

    try {
      if (!directory(dst)) {
        // cannot copy a file to an existing file
        return keysEqual(src, dst);
      }
      // move source under destination directory
      dst = new Path(dst, src.getName());
    } catch (FileNotFoundException e) {
      // destination does not exist
    }

    if (keysEqual(src, dst)) {
      return true;
    }

    if (srcDirectory) {
      for (FileStatus file : listStatus(src)) {
        rename(file.getPath(), new Path(dst, file.getPath().getName()));
      }
      deleteObject(keyFromPath(src) + DIRECTORY_SUFFIX);
    } else {
      s3.copyObject(uri.getHost(), keyFromPath(src), uri.getHost(), keyFromPath(dst));
      delete(src, true);
    }

    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      if (!directory(path)) {
        return deleteObject(keyFromPath(path));
      }
    } catch (FileNotFoundException e) {
      return false;
    }

    if (!recursive) {
      throw new IOException("Directory " + path + " is not empty");
    }

    for (FileStatus file : listStatus(path)) {
      delete(file.getPath(), true);
    }
    deleteObject(keyFromPath(path) + DIRECTORY_SUFFIX);

    return true;
  }

  private boolean directory(Path path) throws IOException {
    return HadoopFileStatus.isDirectory(getFileStatus(path));
  }

  private boolean deleteObject(String key) {
    try {
      s3.deleteObject(uri.getHost(), key);
      return true;
    } catch (AmazonClientException e) {
      return false;
    }
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   *
   * This version doesn't need to create a temporary file to calculate the md5.
   * Sadly this doesn't seem to be used by the shell cp :(
   *
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
                                Path dst) throws IOException {
    String key = keyFromPath(src);
    if (!key.isEmpty()) {
      key += "/";
    }
    if (!overwrite && exists(dst)) {
      throw new IOException(dst + " already exists");
    }

    log.info("Copying local file from " + src + " to " + dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    File srcfile = local.pathToFile(src);

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(partSizeThreshold);

    TransferManager transfers = new TransferManager(s3);
    transfers.setConfiguration(transferConfiguration);

    final ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(uri.getHost(), key, srcfile);
    putObjectRequest.setCannedAcl(cannedACL);
    putObjectRequest.setMetadata(om);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventCode()) {
          case ProgressEvent.PART_COMPLETED_EVENT_CODE:
            statistics.incrementWriteOps(1);
            break;
        }
      }
    };

    Upload up = transfers.upload(putObjectRequest);
    up.addProgressListener(progressListener);
    try {
      up.waitForUploadResult();
      statistics.incrementWriteOps(1);
    } catch (InterruptedException e) {
      throw new IOException("Got interrupted, cancelling");
    } finally {
      transfers.shutdownNow(false);
    }

    // This will delete unnecessary fake parent directories
    finishedWrite(key);

    if (delSrc) {
      local.delete(src, false);
    }
  }

  public void finishedWrite(String key) throws IOException {
    deleteUnnecessaryFakeDirectories(new Path("/" + key).getParent());
  }

  private void deleteUnnecessaryFakeDirectories(Path f) throws IOException {
    while (true) {
      try {
        String key = keyFromPath(f);
        if (key.isEmpty()) {
          break;
        }

        FileStatus status = getFileStatus(f);

        if (status.isDirectory() && status.getBlockSize() == 0 && status.getModificationTime() == 0) {
          if (log.isDebugEnabled()) {
            log.debug("Deleting fake directory " + key + "/");
          }
          s3.deleteObject(uri.getHost(), key + "/");
          statistics.incrementWriteOps(1);
        }
      } catch (FileNotFoundException e) {
      } catch (AmazonServiceException e) {}

      if (f.isRoot()) {
        break;
      }

      f = f.getParent();
    }
  }


  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    log.info("Making directory: " + path);

    try {
      FileStatus fileStatus = getFileStatus(path);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + path);
      }
    } catch (FileNotFoundException e) {
      Path fPart = path;
      do {
        try {
          FileStatus fileStatus = getFileStatus(fPart);
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(String.format(
              "Can't make directory for path '%s' since it is a file.",
              fPart));
          }
        } catch (FileNotFoundException fnfe) {
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = keyFromPath(path);
      if (!key.isEmpty()) {
        key += "/";
      }
      createFakeDirectory(uri.getHost(), key, path.getName());
      return true;
    }
  }

  private void createFakeDirectory(final String bucketName, final String objectName, final String pathName)
    throws AmazonClientException, IOException {
    if (!objectName.endsWith("/")) {
      createEmptyObject(bucketName, objectName + "/", pathName);
    } else {
      createEmptyObject(bucketName, objectName, pathName);
    }
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String bucketName, final String objectName, final String pathName)
    throws AmazonClientException, IOException {

    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };

    final ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(0L);
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setServerSideEncryption(serverSideEncryptionAlgorithm);
    }

    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, im, om);
    putObjectRequest.setCannedAcl(cannedACL);
    s3.putObject(putObjectRequest);
    statistics.incrementWriteOps(1);

    putObjectRequest = new PutObjectRequest(bucketName, objectName + "/" + pathName + FOLDER_SUFFIX, im, om);
    putObjectRequest.setCannedAcl(cannedACL);
    s3.putObject(putObjectRequest);
    statistics.incrementWriteOps(1);
  }

  private Iterator<LocatedFileStatus> listPrefix(Path path) {
    String key = keyFromPath(path);
    if (!key.isEmpty()) {
      key += "/";
    }

    ListObjectsRequest request = new ListObjectsRequest()
      .withBucketName(uri.getHost())
      .withPrefix(key)
      .withDelimiter("/");

    STATS.newListObjectsCall();
    Iterator<ObjectListing> listings = new AbstractSequentialIterator<ObjectListing>(s3.listObjects(request)) {
      @Override
      protected ObjectListing computeNext(ObjectListing previous) {
        if (!previous.isTruncated()) {
          return null;
        }
        return s3.listNextBatchOfObjects(previous);
      }
    };

    return Iterators.concat(Iterators.transform(listings, this::statusFromListing));
  }

  private Iterator<LocatedFileStatus> statusFromListing(ObjectListing listing) {
    return Iterators.concat(
      statusFromPrefixes(listing.getCommonPrefixes()),
      statusFromObjects(listing.getObjectSummaries()));
  }

  private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes) {
    List<LocatedFileStatus> list = new ArrayList<>();
    for (String prefix : prefixes) {
      Path path = qualifiedPath(new Path("/" + prefix));
      FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
      list.add(createLocatedFileStatus(status));
    }
    return list.iterator();
  }

  private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects) {
    return objects.stream()
      .filter(object -> !object.getKey().endsWith("/"))
      .map(object -> new FileStatus(
      object.getSize(),
      false,
      1,
      BLOCK_SIZE.toBytes(),
      object.getLastModified().getTime(),
      qualifiedPath(new Path("/" + object.getKey()))))
      .map(this::createLocatedFileStatus)
      .iterator();
  }

  /**
   * This exception is for stopping retries for S3 calls that shouldn't be retried.
   * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception:
   * Forbidden (Service: Amazon S3; Status Code: 403 ..."
   */
  private static class UnrecoverableS3OperationException extends Exception {
    public UnrecoverableS3OperationException(Throwable cause) {
      super(cause);
    }
  }

  @VisibleForTesting
  ObjectMetadata getS3ObjectMetadata(Path path) throws IOException {
    try {
      return retry()
              .maxAttempts(maxAttempts)
              .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
              .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
              .onRetry(STATS::newGetMetadataRetry)
              .run("getS3ObjectMetadata", () -> {
                try {
                  STATS.newMetadataCall();
                  return s3.getObjectMetadata(uri.getHost(), keyFromPath(path));
                } catch (RuntimeException e) {
                  STATS.newGetMetadataError();
                  if (e instanceof AmazonS3Exception) {
                    switch (((AmazonS3Exception) e).getStatusCode()) {
                      case SC_NOT_FOUND:
                        return null;
                      case SC_FORBIDDEN:
                        throw new UnrecoverableS3OperationException(e);
                    }
                  }
                  throw Throwables.propagate(e);
                }
              });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  private Path qualifiedPath(Path path) {
    return path.makeQualified(this.uri, getWorkingDirectory());
  }

  private LocatedFileStatus createLocatedFileStatus(FileStatus status) {
    try {
      BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
      return new LocatedFileStatus(status, fakeLocation);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static long lastModifiedTime(ObjectMetadata metadata) {
    Date date = metadata.getLastModified();
    return (date != null) ? date.getTime() : 0;
  }

  private static boolean keysEqual(Path p1, Path p2) {
    return keyFromPath(p1).equals(keyFromPath(p2));
  }

  private static String keyFromPath(Path path) {
    checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
    String key = nullToEmpty(path.toUri().getPath());
    if (key.startsWith("/")) {
        key = key.substring(1);
    }
    if (key.endsWith("/")) {
      key = key.substring(0, key.length() - 1);
    }
    return key;
  }

  /* Turns a path (relative or otherwise) into an S3 key
 */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDirectory, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }


  private AmazonS3Client createAmazonS3Client(URI uri, Configuration hadoopConfig, ClientConfiguration clientConfig) {
    AWSCredentialsProvider credentials = getAwsCredentialsProvider(uri, hadoopConfig);
    AmazonS3Client client = new AmazonS3Client(credentials, clientConfig, METRIC_COLLECTOR);

    // use local region when running inside of EC2
    Region region = Regions.getCurrentRegion();
    if (region != null) {
      client.setRegion(region);
    }
    return client;
  }

  private AWSCredentialsProvider getAwsCredentialsProvider(URI uri, Configuration conf) {
    // first try credentials from URI or static properties
    try {
      return new StaticCredentialsProvider(getAwsCredentials(uri, conf));
    } catch (IllegalArgumentException ignored) {
    }

    if (useInstanceCredentials) {
      return new InstanceProfileCredentialsProvider();
    }

    throw new RuntimeException("S3 credentials not configured");
  }

  private static AWSCredentials getAwsCredentials(URI uri, Configuration conf) {
    S3Credentials credentials = new S3Credentials();
    credentials.initialize(uri, conf);
    return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
  }

  private static class TajoS3InputStream extends FSInputStream {
    private final AmazonS3 s3;
    private final String host;
    private final Path path;
    private final int maxAttempts;
    private final Duration maxBackoffTime;
    private final Duration maxRetryTime;

    private boolean closed;
    private InputStream in;
    private long streamPosition;
    private long nextReadPosition;

    public TajoS3InputStream(AmazonS3 s3, String host, Path path, int maxAttempts, Duration maxBackoffTime,
                               Duration maxRetryTime) {
      this.s3 = requireNonNull(s3, "s3 is null");
      this.host = requireNonNull(host, "host is null");
      this.path = requireNonNull(path, "path is null");

      checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
      this.maxAttempts = maxAttempts;
      this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
      this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
    }

    @Override
    public void close() {
      closed = true;
      closeStream();
    }

    @Override
    public void seek(long pos) {
      checkState(!closed, "already closed");
      checkArgument(pos >= 0, "position is negative: %s", pos);

      // this allows a seek beyond the end of the stream but the next read will fail
      nextReadPosition = pos;
    }

    @Override
    public long getPos() {
      return nextReadPosition;
    }

    @Override
    public int read() {
      // This stream is wrapped with BufferedInputStream, so this method should never be called
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      try {
        int bytesRead = retry()
          .maxAttempts(maxAttempts)
          .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
          .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
          .onRetry(STATS::newReadRetry)
          .run("readStream", () -> {
            seekStream();
            try {
              return in.read(buffer, offset, length);
            } catch (Exception e) {
              STATS.newReadError(e);
              closeStream();
              throw e;
            }
          });

        if (bytesRead != -1) {
          streamPosition += bytesRead;
          nextReadPosition += bytesRead;
        }
        return bytesRead;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        throw Throwables.propagate(e);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    private void seekStream() throws IOException, UnrecoverableS3OperationException {
      if ((in != null) && (nextReadPosition == streamPosition)) {
        // already at specified position
        return;
      }

      if ((in != null) && (nextReadPosition > streamPosition)) {
        // seeking forwards
        long skip = nextReadPosition - streamPosition;
        if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
          // already buffered or seek is small enough
          try {
            if (in.skip(skip) == skip) {
              streamPosition = nextReadPosition;
              return;
            }
          } catch (IOException ignored) {
              // will retry by re-opening the stream
          }
        }
      }

      // close the stream and open at desired position
      streamPosition = nextReadPosition;
      closeStream();
      openStream();
    }

    private void openStream() throws IOException, UnrecoverableS3OperationException {
      if (in == null) {
        in = openStream(path, nextReadPosition);
        streamPosition = nextReadPosition;
        STATS.connectionOpened();
      }
    }

    private InputStream openStream(Path path, long start) throws IOException, UnrecoverableS3OperationException {
      try {
          return retry()
            .maxAttempts(maxAttempts)
            .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
            .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
            .onRetry(STATS::newGetObjectRetry)
            .run("getS3Object", () -> {
              try {
                GetObjectRequest request = new GetObjectRequest(host, keyFromPath(path))
                  .withRange(start, Long.MAX_VALUE);
                return s3.getObject(request).getObjectContent();
              } catch (RuntimeException e) {
                STATS.newGetObjectError();
                if (e instanceof AmazonS3Exception) {
                  switch (((AmazonS3Exception) e).getStatusCode()) {
                    case SC_REQUESTED_RANGE_NOT_SATISFIABLE:
                      // ignore request for start past end of object
                      return new ByteArrayInputStream(new byte[0]);
                    case SC_FORBIDDEN:
                    case SC_NOT_FOUND:
                      throw new UnrecoverableS3OperationException(e);
                  }
                }
                throw Throwables.propagate(e);
              }
            });
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      } catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        Throwables.propagateIfInstanceOf(e, UnrecoverableS3OperationException.class);
        throw Throwables.propagate(e);
      }
    }

    private void closeStream() {
      if (in != null) {
        try {
          if (in instanceof S3ObjectInputStream) {
            ((S3ObjectInputStream) in).abort();
          } else {
            in.close();
          }
        } catch (IOException | AbortedException ignored) {
          // thrown if the current thread is in the interrupted state
        }
        in = null;
        STATS.connectionReleased();
      }
    }
  }

  private static class TajoS3OutputStream extends FilterOutputStream {
    private final TransferManager transferManager;
    private final String host;
    private final String key;
    private final File tempFile;

    private boolean closed;

    public TajoS3OutputStream(AmazonS3 s3, TransferManagerConfiguration config, String host, String key,
                                File tempFile) throws IOException {
      super(new BufferedOutputStream(new FileOutputStream(requireNonNull(tempFile, "tempFile is null"))));

      transferManager = new TransferManager(requireNonNull(s3, "s3 is null"));
      transferManager.setConfiguration(requireNonNull(config, "config is null"));

      this.host = requireNonNull(host, "host is null");
      this.key = requireNonNull(key, "key is null");
      this.tempFile = tempFile;

      log.debug("OutputStream for key '" + key +"' using file: " + tempFile);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;

      try {
        super.close();
        uploadObject();
      } finally {
        if (!tempFile.delete()) {
          log.warn("Could not delete temporary file: " + tempFile);
        }
        // close transfer manager but keep underlying S3 client open
        transferManager.shutdownNow(false);
      }
    }

    private void uploadObject() throws IOException {
      try {
        log.debug("Starting upload for host: " + host + ", key: " + key +
            ", file: " + tempFile + ", size: " + tempFile.length());
        STATS.uploadStarted();
        Upload upload = transferManager.upload(host, key, tempFile);

        if (log.isDebugEnabled()) {
          upload.addProgressListener(createProgressListener(upload));
        }

        upload.waitForCompletion();
        STATS.uploadSuccessful();
        log.debug("Completed upload for host: " + host + ", key: " + key);
      } catch (AmazonClientException e) {
        STATS.uploadFailed();
        throw new IOException(e);
      } catch (InterruptedException e) {
        STATS.uploadFailed();
        Thread.currentThread().interrupt();
        throw new InterruptedIOException();
      }
    }

    private ProgressListener createProgressListener(Transfer transfer) {
      return new ProgressListener() {
        private ProgressEventType previousType;
        private double previousTransferred;

        @Override
        public synchronized void progressChanged(ProgressEvent progressEvent) {
          ProgressEventType eventType = progressEvent.getEventType();
          if (previousType != eventType) {
            log.debug("Upload progress event ("+host+"/"+key+"): " + eventType);
            previousType = eventType;
          }

          double transferred = transfer.getProgress().getPercentTransferred();
          if (transferred >= (previousTransferred + 10.0)) {
            log.debug("Upload percentage ("+host+"/"+key+"): " +transferred);
            previousTransferred = transferred;
          }
        }
      };
    }
  }

  @VisibleForTesting
  AmazonS3 getS3Client() {
    return s3;
  }

  @VisibleForTesting
  void setS3Client(AmazonS3 client) {
    s3 = client;
  }
}