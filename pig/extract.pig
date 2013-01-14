REGISTER ../commoncrawl-examples/lib/guava-12.0.jar
REGISTER ../commoncrawl-examples/lib/gson-2.2.1.jar
REGISTER ../commoncrawl-examples/lib/httpcore-4.2.1.jar
REGISTER ../commoncrawl-examples/lib/jsoup-1.7.2-SNAPSHOT.jar
REGISTER ../commoncrawl-examples/lib/htmlcleaner-2.2.jar 
REGISTER ../commoncrawl-examples/commoncrawl-examples-1.0.1.jar
	
SET mapred.input.pathFilter.class org.commoncrawl.pig.FilterGzip;
SET job.name "CommonCrawlProcessing";
SET default_parallel 300;

IN = LOAD '/data/public/common-crawl/parse-output/' using org.commoncrawl.pig.ArcLoader() as (date, length:int, type:chararray, statuscode:int, ipaddress:chararray, url:chararray, title:chararray, links:chararray, words:chararray);

LIST = LOAD 'list' as (domain:chararray);

WEBSITES = FOREACH IN GENERATE SUBSTRING(url,0,INDEXOF(url,'/',8)+1) as dom, url, statuscode, type, length, title, links, words;
	
J = JOIN WEBSITES by dom, LIST BY domain using 'replicated';

STORE J INTO 'foundations_pages.gz';

