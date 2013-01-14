package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class MyUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return null;
		try {

			// In input we get a webpage. What should we do?

			String str = (String) tuple.get(0);
			return str.toUpperCase();
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
