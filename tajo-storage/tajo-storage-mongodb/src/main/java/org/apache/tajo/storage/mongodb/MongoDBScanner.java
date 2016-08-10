/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.mongodb;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.text.TextLineParsingError;

import java.io.IOException;

/*
* Reads data from a MongoDB table
* Uses MongoDBCollectionReader to read tuples
* */

//Todo Remove FileScanner
public class MongoDBScanner extends FileScanner {

    MongoDBCollectionReader collectionReader;

    public MongoDBScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) {
        super(conf, schema, meta, fragment);

    }

    @Override
    public void init() throws IOException {
        if (targets == null) {
            targets = schema.toArray();
        }

        reset();

        super.init();
    }

    @Override
    public Tuple next() throws IOException {
        try {
            Tuple t = collectionReader.readTuple();
            return t;
        } catch (TextLineParsingError textLineParsingError) {
            textLineParsingError.printStackTrace();
            return null;
        }
    }

    @Override
    public void reset() throws IOException {
        MongoDBDocumentDeserializer deserializer = new MongoDBDocumentDeserializer(schema,meta,targets);
        collectionReader = new MongoDBCollectionReader(ConnectionInfo.fromURI(fragment.getUri()),deserializer,targets.length);

        collectionReader.init();
    }

    @Override
    public void close() throws IOException {
    return;
    }

    @Override
    public boolean isProjectable() {
        return false;
    }

    @Override
    public boolean isSelectable() {
        return false;
    }

    @Override
    public void setFilter(EvalNode filter) {

    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public float getProgress() {
        return collectionReader.getProgress();
    }


}
