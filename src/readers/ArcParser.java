package readers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.http.HttpException;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;

public class ArcParser extends LoadFunc {

	private RecordReader<Text, ArcRecord> in;
	private final TupleFactory mTupleFactory = TupleFactory.getInstance();
	private static final Logger LOG = Logger.getLogger(ArcParser.class);

	@Override
	public InputFormat<Text, ArcRecord> getInputFormat() throws IOException {
		return new ArcInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			ArcRecord value = null;
			while (value == null) {
				boolean notDone = in.nextKeyValue();
				if (!notDone) {
					return null;
				}
				value = in.getCurrentValue();
				try {
					value.getHttpResponse();
				} catch (HttpException e) {
					LOG.debug(e.getMessage());
					value = null;
				}
			}
			Tuple t = mTupleFactory.newTuple(7);
			t.set(0, value.getArchiveDate().toString());
			t.set(1, value.getContentLength());
			String type = value.getContentType();
			t.set(2, type);
			try {
				t.set(3, value.getHttpStatusCode());
			} catch (HttpException e) {
				t.set(3, -1);
			}
			t.set(4, value.getIpAddress());
			t.set(5, value.getURL());

			// If the page is a html page then parse it
			if (type != null && type.equals("text/html")) {

			} else {

			}

			return t;
		} catch (InterruptedException e) {
			throw new IOException("Error getting input");
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToRead(RecordReader reader, PigSplit arg1)
			throws IOException {
		in = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
