package org.commoncrawl.pig;

import org.apache.hadoop.fs.*;

public class FilterGzip implements PathFilter {

    public boolean accept(Path path) {
	if (path.getName().indexOf("metadata") != -1 || path.getName().indexOf("textData") != -1) {
	    return false;
	}	else {       
	    return true;
	}
    }
}
