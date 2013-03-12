REGISTER '../load-grouper/target/load-grouper-1.0-SNAPSHOT.jar';

songs = LOAD 's3n://tbmmsd/A.tsv.*' USING com.mortardata.pig.LoadGrouperFunc() AS (filename:chararray, split_index:chararray, count:int);
 
songs_all = GROUP songs ALL;
 
avg_split_song_count = FOREACH songs_all GENERATE AVG(songs.count) as avg_song_count, $ITERATION_NUM as iter_num;

rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/$ITERATION_NUM/avg_split_song_count;
STORE avg_split_song_count INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/$ITERATION_NUM/avg_split_song_count' USING PigStorage('\t');
