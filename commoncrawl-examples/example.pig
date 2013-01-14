register /home/participant/git/commoncrawl-examples/lib/*.jar; 

register /home/participant/git/commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1.jar;

a = LOAD '/data/1346864466526_10.arc.gz' USING org.commoncrawl.pig.ArcLoader() as (date, length, type, statuscode, ipaddress, url, html, payload);

words = foreach a generate flatten(statuscode) as codes;

grpd = group words by codes; 

cntd = foreach grpd generate group, COUNT(words); 

dump cntd;
