REGISTER ../commoncrawl-examples/lib/guava-12.0.jar
REGISTER ../commoncrawl-examples/lib/gson-2.2.1.jar
REGISTER ../commoncrawl-examples/lib/httpcore-4.2.1.jar
REGISTER ../commoncrawl-examples/lib/jsoup-1.7.2-SNAPSHOT.jar
REGISTER ../commoncrawl-examples/lib/htmlcleaner-2.2.jar 
REGISTER ../commoncrawl-examples/commoncrawl-examples-1.0.1.jar
REGISTER ../award.jar

SET job.name "JaccardCoefficient";
SET default_parallel 256;

LIST_FOUNDATIONS = LOAD 'list' as (domain:chararray);

IN = LOAD '/data/public/common-crawl/parse-output/' using org.commoncrawl.pig.ArcLoader() as (date, length:int, type:chararray, statuscode:int, ipaddress:chararray, url:chararray, title:chararray, links:chararray, words:chararray);
IN = FILTER IN BY statuscode == 200 and title != 'NOT_PARSED' and words != 'CONVERSION_ERRORS' and type == 'text/html';
IN2 = FOREACH IN GENERATE SUBSTRING(url,0,INDEXOF(url,'/',8)+1) as domain, url, links, words;

-- Retrieve foundation pages
F = JOIN IN2 by domain, LIST_FOUNDATIONS BY domain using 'replicated';

-- Retrieve other pages that contain the list of foundations
RF = FOREACH IN2 GENERATE url, FLATTEN(TOKENIZE(links,',')) as link:chararray, words;
RF1 = FOREACH RF generate SUBSTRING(url,0,INDEXOF(url,'/',8) + 1) as domain_url, url, SUBSTRING(link,0,INDEXOF(link,'/',8) + 1) as link_domain, words;
RF2 = FILTER RF1 BY domain_url != link_domain;
RF3 = JOIN RF2 BY link_domain, LIST_FOUNDATIONS BY domain using 'replicated';
RF4 = DISTINCT RF3;

-- Union the two sets of webpages
F = FOREACH F GENERATE IN2::url as url, IN2::domain as domain, IN2::words as words;
RF4 = FOREACH RF4 GENERATE RF2::url as url, RF2::link_domain as domain, RF2::words as words;
TF = UNION ONSCHEMA F, RF4;

-- Calculate the Jaccard Index: A
A = FOREACH TF generate domain, FLATTEN(udf.CouccurrenceKeyword()) as keyword:chararray;
A1 = GROUP A BY (domain,keyword);
A2 = FOREACH A1 GENERATE group.domain as domain, group.keyword as keyword, COUNT(A) as count;

-- Calculate the Jaccard Index: B
B = GROUP TF by domain;
B1 = FOREACH B GENERATE group as domain, COUNT(TF) as count;

-- Calculate the Jaccard Index: C
IN3 = FOREACH IN2 GENERATE IN2::url as url, IN2::domain as domain, IN2::words as words;
C = FOREACH IN3 generate FLATTEN(udf.CouccurrenceKeyword()) as keyword:chararray;
C1 = GROUP C BY keyword;
C2 = FOREACH C1 GENERATE group as keyword, COUNT(IN3) as count;

STORE A2 into 'jaccard_a.gz';
STORE B into 'jaccard_b.gz';
STORE C2 into 'jaccard_c.gz';