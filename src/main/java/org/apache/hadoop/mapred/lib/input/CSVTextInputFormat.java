/**
 * Copyright 2014 Marcelo Elias Del Valle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.lib.input;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 * 
 *
 * @author sivakumar
 *
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable{


    private CompressionCodecFactory compressionCodecs = null;

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        LOG.info("Initiated csv input format");

        reporter.setStatus(genericSplit.toString());

        // This change is required for CombineFileInputFormat to work with .gz files.

        // Check if we should throw away this split
        long start = ((FileSplit)genericSplit).getStart();
        Path file = ((FileSplit)genericSplit).getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec != null && start != 0) {
            // (codec != null) means the file is not splittable.

            // In that case, start should be 0, otherwise this is an extra split created
            // by CombineFileInputFormat. We should ignore all such extra splits.
            //
            // Note that for the first split where start = 0, LineRecordReader will
            // ignore the end pos and read the whole file.
            return new EmptyRecordReader();
        }


        return new CSVRecordReader(job, (FileSplit) genericSplit
                );
	}

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return compressionCodecs.getCodec(file) == null;
    }



    static class EmptyRecordReader implements RecordReader<LongWritable, Text> {
        @Override
        public void close() throws IOException {}
        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }
        @Override
        public Text createValue() {
            return new Text();
        }
        @Override
        public long getPos() throws IOException {
            return 0;
        }
        @Override
        public float getProgress() throws IOException {
            return 0;
        }
        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            return false;
        }
    }
}

