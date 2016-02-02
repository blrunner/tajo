/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.FileTablespace;

import net.minidev.json.JSONObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;

public class S3TableSpace extends FileTablespace {
  private final Log LOG = LogFactory.getLog(S3TableSpace.class);

  private AmazonS3 s3;
  private boolean useInstanceCredentials;

  public S3TableSpace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public void init(TajoConf tajoConf) throws IOException {
    super.init(tajoConf);

    int maxErrorRetries = conf.getIntVar(TajoConf.ConfVars.S3_MAX_ERROR_RETRIES);
    boolean sslEnabled = conf.getBoolVar(TajoConf.ConfVars.S3_SSL_ENABLED);

    Duration connectTimeout = Duration.valueOf(conf.getVar(TajoConf.ConfVars.S3_CONNECT_TIMEOUT));
    Duration socketTimeout = Duration.valueOf(conf.getVar(TajoConf.ConfVars.S3_SOCKET_TIMEOUT));
    int maxConnections = conf.getIntVar(TajoConf.ConfVars.S3_MAX_CONNECTIONS);

    this.useInstanceCredentials = conf.getBoolVar(TajoConf.ConfVars.S3_USE_INSTANCE_CREDENTIALS);

    ClientConfiguration configuration = new ClientConfiguration()
      .withMaxErrorRetry(maxErrorRetries)
      .withProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP)
      .withConnectionTimeout(Ints.checkedCast(connectTimeout.toMillis()))
      .withSocketTimeout(Ints.checkedCast(socketTimeout.toMillis()))
      .withMaxConnections(maxConnections);

    Path tajoRootPath = TajoConf.getTajoRootDir(conf);
    FileSystem defaultFS = tajoRootPath.getFileSystem(conf);
    this.s3 = createAmazonS3Client(defaultFS.getUri(), conf, configuration);

    if (s3 != null) {
      String endPoint = conf.getTrimmed(ENDPOINT,"");
      try {
        if (!endPoint.isEmpty()) {
          s3.setEndpoint(endPoint);
        }
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }

      LOG.info("Amazon3Client is initialized.");
    }
  }

  private AmazonS3Client createAmazonS3Client(URI uri, Configuration hadoopConfig, ClientConfiguration clientConfig) {
    AWSCredentialsProvider credentials = getAwsCredentialsProvider(uri, hadoopConfig);
    AmazonS3Client client = new AmazonS3Client(credentials, clientConfig);
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

  @Override
  protected long getTotalFileSize(Path path) throws IOException {
    String key = keyFromPath(path);
    if (!key.isEmpty()) {
      key += "/";
    }

    Iterable<S3ObjectSummary> objectSummaries = S3Objects.withPrefix(s3, uri.getHost(), key);
    Stream<S3ObjectSummary> objectStream = StreamSupport.stream(objectSummaries.spliterator(), false);
    long totalBucketSize = objectStream.mapToLong(object -> object.getSize()).sum();
    objectStream.close();
    return totalBucketSize;
  }

  private String keyFromPath(Path path)
  {
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

  @VisibleForTesting
  public AmazonS3 getAmazonS3Client() {
    return s3;
  }
}