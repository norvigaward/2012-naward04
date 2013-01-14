package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CouccurrenceKeywords extends EvalFunc<Integer> {

	public static int countMatches(String str, String sub) {
		int count = 0;
		int idx = 0;
		while ((idx = str.indexOf(sub, idx)) != -1) {
			count++;
			idx += sub.length();
		}
		return count;
	}

	@Override
	public Integer exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return null;
		try {
			String webpage = (String) tuple.get(0);
			String keyword = (String) tuple.get(1);
			int occurrences = countMatches(webpage, keyword);
			return (occurrences == 0) ? null : occurrences;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
