%declare INPUTMAINPATH '/user/st-turetta/project/input/'
%declare OUTPUTMAINPATH '/user/st-turetta/project/output/'
%declare OUTPUTSUBFOLDERATIOVOLPKT '/ratio_vol_td'

dataset = LOAD '$INPUTMAINPATH$filename' using PigStorage(',') AS (pkt_n:long, time:double, sourceip:chararray, destip:chararray, protocol:chararray, dim:long, info:chararray);

udpdataset = FILTER dataset BY protocol == 'UDP';

ordereddataset = ORDER udpdataset BY pkt_n DESC;


udpgroup = GROUP ordereddataset BY (sourceip, destip);



udpmap = FOREACH udpgroup GENERATE group, ordereddataset.(pkt_n,time,dim), MIN(ordereddataset.time) AS min_ts, MAX(ordereddataset.time) AS max_ts, COUNT(ordereddataset.pkt_n) AS n_packets, SUM(ordereddataset.dim) AS total_volume;

result = FOREACH udpmap GENERATE group, min_ts, max_ts, n_packets,total_volume, (float)(max_ts-min_ts) AS time_difference, (float)(total_volume/(max_ts-min_ts)) AS ratio_vol_td;

resultordered_ratio_vol_pkts = ORDER result BY ratio_vol_td DESC;

STORE resultordered_ratio_vol_pkts INTO '$OUTPUTMAINPATH$filename$OUTPUTSUBFOLDERATIOVOLPKT' using PigStorage(';');