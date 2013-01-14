REGISTER ../commoncrawl-examples/lib/guava-12.0.jar
REGISTER ../commoncrawl-examples/lib/gson-2.2.1.jar
REGISTER ../commoncrawl-examples/lib/httpcore-4.2.1.jar
REGISTER ../commoncrawl-examples/lib/jsoup-1.7.2-SNAPSHOT.jar
REGISTER ../commoncrawl-examples/lib/htmlcleaner-2.2.jar 
REGISTER ../commoncrawl-examples/commoncrawl-examples-1.0.1.jar
	
SET mapred.input.pathFilter.class org.commoncrawl.pig.FilterGzip;
SET job.name "CommonCrawlProcessing";
SET default_parallel 300;

/*Load the entire dataset*/
	/*TEST /data/public/common-crawl/award/testset/*/
	/*/data/public/common-crawl/parse-output/*/

IN = LOAD '/data/public/common-crawl/parse-output/' using org.commoncrawl.pig.ArcLoader() as (date, length:int, type:chararray, statuscode:int, ipaddress:chararray, url:chararray, title:chararray, words:chararray);

A = FOREACH IN GENERATE url, statuscode, type, length, title, words;
B = FILTER A BY statuscode == 200 and words != 'NOT_VALID' and words != 'CONVERSION_ERRORS';
STORE B INTO 'cc2.gz';

--SPLIT A INTO TOO_LARGE IF length >100000, NOT_TEXT IF SUBSTRING(type,0,4)!='text', NOT_200 if statuscode != 200;


-- STORE TOO_LARGE INTO 'cc_too_large.gz';
-- STORE NOT_TEXT INTO 'cc_not_text.gz';
-- STORE NOT_200 INTO 'cc_not_200.gz';

-- /*Filter only good answers*/
-- SPLIT IN INTO TEXT_PAGES if (statuscode == 200 AND SUBSTRING(type,0,4)=='text'), NOT_RESPONSE if statuscode != 200, NOT_TEXT if SUBSTRING(type,0,4) != 'text';

-- SPLIT TEXT_PAGES INTO OK_TO_PARSE IF (length <= 100000), TOO_LARGE IF (length > 100000);

-- /*For the good pages, keep only date, domain, url, content*/
-- OK_TO_PARSE = FOREACH OK_TO_PARSE GENERATE date as DATE, SUBSTRING(url,0,INDEXOF(url,'/',8)) as DOMAIN, url as URL, type as TYPE, raw as RAW;

-- /*For the pages that are too large, keep only the URI, and length, and store the content*/
-- TOO_LARGE = FOREACH TOO_LARGE GENERATE url as URL, length as LENGTH, type as TYPE, raw as RAW;

-- /*For the no HTML pages, stores also the type*/
-- NOT_TEXT = FOREACH NOT_TEXT GENERATE url as URL, length as LENGTH, type as TYPE, raw as RAW;

-- /*For the no good response pages, store also the response*/
-- NOT_RESPONSE = FOREACH NOT_RESPONSE GENERATE url as URL, length as LENGTH, statuscode as STATUS;

-- STORE OK_TO_PARSE INTO 'cc_valid.gz';
-- STORE NOT_TEXT INTO 'cc_not_text.gz';
-- STORE TOO_LARGE INTO 'cc_too_large.gz';
-- STORE NOT_RESPONSE INTO 'cc_not_200.gz';
