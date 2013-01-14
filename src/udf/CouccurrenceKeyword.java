package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CouccurrenceKeyword extends EvalFunc<DataBag> {

	// public static int countMatches(String str, String sub) {
	// int count = 0;
	// int idx = 0;
	// while ((idx = str.indexOf(sub, idx)) != -1) {
	// count++;
	// idx += sub.length();
	// }
	// return count;
	// }

	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();
	String[] keywords;

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0)
			return null;
		try {
			String webpage = (String) tuple.get(2);

			// Returns a bag with all the words that have appeared at least one
			// in the webpage
			DataBag output = mBagFactory.newDefaultBag();

			for (String keyword : keywords) {
				if (webpage.indexOf(keyword) != -1) {
					output.add(mTupleFactory.newTuple(keyword));
				}
			}

			return output;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
