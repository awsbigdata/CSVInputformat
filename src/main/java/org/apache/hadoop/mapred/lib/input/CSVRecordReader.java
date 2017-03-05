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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;

import java.io.*;
import java.util.List;
import java.util.zip.ZipInputStream;

/**
 * Reads a CSV line. CSV files could be multiline, as they may have line breaks
 * inside a column
 * 
 * @author sivakumar
 * 
 */
public class CSVRecordReader implements RecordReader<LongWritable, Text> {
	public static final String LINE_DELIMITER = "csvinputformat.record.delimiter";
	public static final String IS_ZIPFILE = "mapreduce.csvinput.zipfile";
	public static final String DEFAULT_DELIMITER = "\n";
    public static final String DEFAULT_QUOTE = "\"";
	public static final boolean DEFAULT_ZIP = false;
    private static final Log LOG
            = LogFactory.getLog(CSVRecordReader.class.getName());
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
    private int maxLineLength=1000;
	protected Reader in;
	private String delimiter;
	private Boolean isZipFile;
	private InputStream is;

	/**
	 * Default constructor is needed when called by reflection from hadoop
	 *
	 */
	public CSVRecordReader() {
	}

	/**
	 * Constructor to be called from FileInputFormat.createRecordReader
	 *
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public CSVRecordReader(InputStream is, Configuration conf) throws IOException {
		//init(is, conf);
	}

    public CSVRecordReader(Configuration job, FileSplit split
                            ) throws IOException {
        LOG.info("Initiated csv reader");
        this.delimiter = job.get(LINE_DELIMITER, DEFAULT_DELIMITER);

        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new BufferedReader(new InputStreamReader(codec.createInputStream(fileIn)));
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new BufferedReader(new InputStreamReader(fileIn));

            LOG.info("Initiated csv reader");
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            start += readLine(new Text());
        }
        this.pos = start;
    }

    @Override
    public synchronized boolean next(LongWritable longWritable, Text text) throws IOException {

        if (longWritable == null) {
            longWritable = new LongWritable();
        }
        longWritable.set(pos);

        if (text == null) {
            text = new Text();
        }
        while (pos<end) {
            long newSize = readLine(text);
            if(newSize>Integer.MAX_VALUE) {
             LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
            }
            pos += newSize;
            if (newSize == 0) {
                return false;
            } else {
                return true;
            }

        }
            // line too long. try again
    return false;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public synchronized long getPos() throws IOException {
        return pos;
    }

    /**
	 * Skip if delimiter exist in between column
	 */
	protected long readLine(Text values) throws IOException {
        LOG.info("Initiated csv reader");

        values.clear();// Empty value columns list
		char c;
		long numRead = 0;
		boolean insideQuote = false;
		StringBuffer sb = new StringBuffer();
		int i;
		int quoteOffset = 0, delimiterOffset = 0;
		// Reads each char from input stream unless eof was reached
		while ((i = in.read()) != -1) {
			c = (char) i;
			numRead++;
			sb.append(c);
			// Check quotes, as delimiter inside quotes don't count
			if (c == DEFAULT_QUOTE.charAt(quoteOffset)) {
                insideQuote = !insideQuote;
            }
			// Check delimiters, but only those outside of quotes
			if (!insideQuote) {
				// A new line outside of a quote is a real csv line breaker
				if (c == delimiter.charAt(0)) {
					break;
				}
			}

			if(numRead>Integer.MAX_VALUE)
			    return numRead;
		}
        LOG.info("line Read : "+sb.toString());
        values.set(sb.toString());
		return numRead;
	}




	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
			in = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
	}
}
