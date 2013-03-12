REGISTER '../../load-grouper/target/load-grouper-1.0-SNAPSHOT.jar';

songs = LOAD 's3n://tbmmsd/A.tsv.*' USING com.mortardata.pig.LoadGrouperFunc() AS (filename:chararray, split_index:chararray, count:int);
 
songs_all = GROUP songs ALL;
 
avg_file_count = FOREACH songs_all GENERATE COUNT(songs) as song_count, $ITERATION_NUM as iter_num;

rmf s3n://hawk-dev-sandbox/$MORTAR_EMAIL_S3_ESCAPED/$ITERATION_NUM/top_density_songs;
STORE avg_file_count INTO 's3n://hawk-dev-sandbox/$MORTAR_EMAIL_S3_ESCAPED/$ITERATION_NUM/top_density_songs' USING PigStorage('\t');
