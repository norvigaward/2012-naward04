package org.commoncrawl.pig;

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
import org.apache.pig.data.*;
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;
import org.jsoup.nodes.*;
import org.jsoup.select.*;
import java.net.*;
import org.jsoup.Jsoup;
import org.htmlcleaner.*;

public class ArcLoader extends LoadFunc {

  private RecordReader<Text, ArcRecord> in;
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  private static final Logger LOG = Logger.getLogger(ArcLoader.class);

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

      Tuple t = mTupleFactory.newTuple(9);
      if (value.getArchiveDate() != null)
	  t.set(0, value.getArchiveDate().toString());
      else 
	  t.set(0, "NA");

      int contentLength = value.getContentLength();
      t.set(1, contentLength);
      String contentType = value.getContentType();
      t.set(2, contentType);
      try {
        t.set(3, value.getHttpStatusCode());
      } catch(HttpException e) {
        t.set(3, -1);
      }
      t.set(4, value.getIpAddress());
      t.set(5, value.getURL());

      if (contentType != null && contentType.equals("text/html") && contentLength <= 200000) {
	  try {
	      Document doc = value.getParsedHTML();
	      if (doc != null) {
	       	  // Get title document
	       	  String title = doc.title();
	       	  title = title.replaceAll("\\n","");
	       	  title = title.replaceAll("\\t","");
	       	  title = title.replaceAll("\\r","");
	       	  t.set(6, title);

		  //Get list links out of the page
		  Elements links = doc.select("a[href]");
		  String l = "";
		  for(Element link : links) {
		      String sLink = link.attr("abs:href");
		      //		      sLink = URLEncoder.encode(sLink, "UTF-8");
		      sLink = sLink.replaceAll(",","");
		      l +=  sLink + ",";
		  }
		  if (l.length() > 0)
		      l = l.substring(0, l.length() - 1);
		  t.set(7,l);

	       	  // Get list words
	       	  String text = doc.body().text();
	       	  text = text.replaceAll("\\t","");
	       	  text = text.replaceAll("\\n","");
	       	  text = text.replaceAll("\\r","");
	       	  t.set(8, text);
	      }
	      // TagNode doc = value.getParsedHTML2();
	      // if (doc != null) {
	      // 	  // Get title document
	      // 	  t.set(6,"TODO");
	      // 	  // Get list words
	      // 	  String text = doc.getText().toString();
	      // 	  text = text.replaceAll("\\t","");
	      // 	  text = text.replaceAll("\\n","");
	      // 	  text = text.replaceAll("\\r","");
	      // 	  t.set(7, text);
	      // }
	  } catch (Exception e) {
	      t.set(6, "CONVERSION_ERRORS");
	      t.set(7,null);
	      t.set(8,null);
	  }
      } else {
	  t.set(6, "NOT_PARSED");
	  t.set(7,null);
	      t.set(8,null);
      }

      // if (contentType != null && contentType.equals("text/html")) {
      // 	      if (contentLength > 100000) {
      // 		  t.set(6, "PAGE_TOO_LARGE");
      // 		  if (value.getPayload() != null)
      // 		      t.set(7, new DataByteArray(value.getPayload()));
      // 		  else
      // 		      t.set(7, null);
      // 	      } else {
      // 		  try {
      // 		      Document doc = value.getParsedHTML();
      // 		      if (doc != null) {
      // 			  t.set(6, doc.toString());
      // 			  t.set(7, null);
      // 		      } else {
      // 			  t.set(6,"CONVERSION_ERRORS");
      // 			  if (value.getPayload() != null)
      // 			      t.set(7, new DataByteArray(value.getPayload()));
      // 			  else
      // 			      t.set(7, null);
      // 		      }
      // 		  } catch (Exception e) {
      // 		      t.set(6,"CONVERSION_ERRORS");
      // 		      if (value.getPayload() != null)
      // 			  t.set(7, new DataByteArray(value.getPayload()));
      // 		      else
      // 			  t.set(7, null);
      // 		  }
      // 	      }
      // } else {
      // 	  t.set(6, null);
      // 	  t.set(7, new DataByteArray(value.getPayload()));
      // }

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
