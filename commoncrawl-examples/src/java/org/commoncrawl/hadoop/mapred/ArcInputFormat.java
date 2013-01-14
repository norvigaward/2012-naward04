package org.commoncrawl.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.util.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A input format the reads arc files.
 */
public class ArcInputFormat extends FileInputFormat<Text, ArcRecord> {

	/**
	 * Returns the <code>RecordReader</code> for reading the arc file.
	 * 
	 * @param split
	 *            The InputSplit of the arc file to process.
	 * @param job
	 *            The job configuration.
	 * @param reporter
	 *            The progress reporter.
	 */
	public RecordReader<Text, ArcRecord> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		context.setStatus(split.toString());
		return new ArcRecordReader();
	}

	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}
		if (dirs.length > 1) {
			throw new IOException(
					"This format supports only one input directory");
		}

		PathFilter inputFilter = getInputPathFilter(job);

		List<Path> dirsToProcess = new ArrayList<Path>();
		dirsToProcess.add(dirs[0]);
		while (dirsToProcess.size() > 0) {
			Path p = dirsToProcess.remove(dirsToProcess.size() - 1);
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.listStatus(p, inputFilter);
			if (matches == null) {
				throw new IOException("Input path does not exist: " + p);
			} else {
				for (FileStatus child : matches) {
					if (child.isDir()) {
						dirsToProcess.add(child.getPath());
					} else {
						result.add(child);
					}
				}
			}
		}

		return result;
	}

	/**
	 * <p>
	 * Always returns false to indicate that ARC files are not splittable.
	 * </p>
	 * <p>
	 * ARC files are stored in 100MB files, meaning they will be stored in at
	 * most 3 blocks (2 blocks on Hadoop systems with 128MB block size).
	 * </p>
	 */
	protected boolean isSplitable(JobContext context,
			org.apache.hadoop.fs.Path filename) {
		return false;
	}
}
