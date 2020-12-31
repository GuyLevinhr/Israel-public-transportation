#Setup
#1.Ope mongoDB Atlas and open cluster

#2.Create kinesis
aws kinesis create-stream \
    --stream-name TranOnline \
    --shard-count 1 \
	--region eu-north-1
	
aws kinesis create-stream \
    --stream-name tranafterspark \
    --shard-count 1 \
	--region eu-north-1

aws kinesis create-stream \
    --stream-name tran.bus.in.station \
    --shard-count 1 \
	--region eu-north-1

#3.Create kinesis Data Analytics applications
#3.Create kinesis Data Firehose

#Drop

#1.Ope mongoDB Atlas and stop cluster

#2.Delete kinesis
aws kinesis delete-stream \
    --stream-name TranOnline \
	--region eu-north-1
	
aws kinesis delete-stream \
    --stream-name tranafterspark \
	--region eu-north-1
	
aws kinesis delete-stream \
    --stream-name tran.bus.in.station \
	--region eu-north-1
		
#3.Drop kinesis Data Analytics applications
#3.Drop kinesis Data Firehose