REGISTER ../commoncrawl-examples/lib/guava-12.0.jar
REGISTER ../commoncrawl-examples/lib/gson-2.2.1.jar
REGISTER ../commoncrawl-examples/lib/httpcore-4.2.1.jar
REGISTER ../commoncrawl-examples/lib/jsoup-1.7.2-SNAPSHOT.jar
REGISTER ../commoncrawl-examples/commoncrawl-examples-1.0.1.jar

A = LOAD '/data/public/common-crawl/award/testset/1346864469604_0.arc.gz' using org.commoncrawl.pig.ArcLoader() as (date, length, type, statuscode, ipaddress, url, html, payload);
B = LIMIT A 10;
DUMP B;

