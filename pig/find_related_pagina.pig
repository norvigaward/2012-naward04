REGISTER ../commoncrawl-examples/lib/guava-12.0.jar
REGISTER ../commoncrawl-examples/lib/gson-2.2.1.jar
REGISTER ../commoncrawl-examples/lib/httpcore-4.2.1.jar
REGISTER ../commoncrawl-examples/lib/jsoup-1.7.2-SNAPSHOT.jar
REGISTER ../commoncrawl-examples/lib/htmlcleaner-2.2.jar 
REGISTER ../commoncrawl-examples/commoncrawl-examples-1.0.1.jar
	
SET mapred.input.pathFilter.class org.commoncrawl.pig.FilterGzip;
SET job.name "FindRelatedPaginas";
SET default_parallel 256;

IN = LOAD '/data/public/common-crawl/parse-output/' using org.commoncrawl.pig.ArcLoader() as (date, length:int, type:chararray, statuscode:int, ipaddress:chararray, url:chararray, title:chararray, links:chararray, content:chararray);

LIST = LOAD 'list' as (domain:chararray);

--Deconstruct the links
A = FOREACH IN GENERATE url, FLATTEN(TOKENIZE(links,',')) as link:chararray;

--Create domain from the link
B = FOREACH A generate SUBSTRING(url,0,INDEXOF(url,'/',8) + 1) as domain_url, url, SUBSTRING(link,0,INDEXOF(link,'/',8) + 1) as link_domain;
C = FILTER B BY domain_url != link_domain;

-- Join with the list of organizations
D = JOIN C BY link_domain, LIST BY domain using 'replicated';
D = DISTINCT D;

E = FOREACH D GENERATE C::domain_url, C::url, C::link_domain;

STORE E into 'pages_related_orgz.gz';
