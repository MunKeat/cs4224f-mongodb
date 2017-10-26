##
##config server
screen -dmS node_cs mongod --configsvr --replSet cs --port 27020 --dbpath=/temp/cs4224f/data-mongo/sr0 
rs.initiate(
  {
    _id: "cs",
    configsvr: true,
    members: [
      { _id : 0, host : "xcnd25.comp.nus.edu.sg:27020" },
      { _id : 1, host : "xcnd26.comp.nus.edu.sg:27020" },
      { _id : 2, host : "xcnd27.comp.nus.edu.sg:27020" },
      { _id : 3, host : "xcnd28.comp.nus.edu.sg:27020" },
      { _id : 4, host : "xcnd29.comp.nus.edu.sg:27020" }
    ]
  }
);

#js to remove replset
use local;
db.dropDatabase();
exit;
#for bash
#kill all screen
screen -ls | grep Detached | cut -d. -f1 | awk '{print $1}' | xargs kill

#### node0
##replica set shard
screen -dmS node0_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs0
screen -dmS node0_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs0
screen -dmS node0_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs0

##for mongo shell
rs.initiate();
rs.add("xcnd25.comp.nus.edu.sg:27018");
rs.add("xcnd25.comp.nus.edu.sg:27019");

screen -dmS node0_s mongos --port 27021 --configdb cs/xcnd25.comp.nus.edu.sg:27020,xcnd26.comp.nus.edu.sg:27020,xcnd27.comp.nus.edu.sg:27020,xcnd28.comp.nus.edu.sg:27020,xcnd29.comp.nus.edu.sg:27020

#### node1
screen -dmS node1_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs1
screen -dmS node1_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs1
screen -dmS node1_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs1

rs.initiate();
rs.add("xcnd26.comp.nus.edu.sg:27018");
rs.add("xcnd26.comp.nus.edu.sg:27019");

screen -dmS node1_s mongos --port 27021 --configdb cs/xcnd25.comp.nus.edu.sg:27020,xcnd26.comp.nus.edu.sg:27020,xcnd27.comp.nus.edu.sg:27020,xcnd28.comp.nus.edu.sg:27020,xcnd29.comp.nus.edu.sg:27020

#### node2
screen -dmS node2_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs2
screen -dmS node2_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs2
screen -dmS node2_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs2

rs.initiate();
rs.add("xcnd27.comp.nus.edu.sg:27018");
rs.add("xcnd27.comp.nus.edu.sg:27019");

screen -dmS node2_s mongos --port 27021 --configdb cs/xcnd25.comp.nus.edu.sg:27020,xcnd26.comp.nus.edu.sg:27020,xcnd27.comp.nus.edu.sg:27020,xcnd28.comp.nus.edu.sg:27020,xcnd29.comp.nus.edu.sg:27020


#### node3
screen -dmS node3_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs3
screen -dmS node3_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs3
screen -dmS node3_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs3

rs.initiate();

rs.add("xcnd28.comp.nus.edu.sg:27018");
rs.add("xcnd28.comp.nus.edu.sg:27019");

screen -dmS node3_s mongos --port 27021 --configdb cs/xcnd25.comp.nus.edu.sg:27020,xcnd26.comp.nus.edu.sg:27020,xcnd27.comp.nus.edu.sg:27020,xcnd28.comp.nus.edu.sg:27020,xcnd29.comp.nus.edu.sg:27020


#### node4
screen -dmS node4_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs4
screen -dmS node4_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs4
screen -dmS node4_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs4

rs.initiate();
rs.add("xcnd29.comp.nus.edu.sg:27018");
rs.add("xcnd29.comp.nus.edu.sg:27019");

screen -dmS node4_s mongos --port 27021 --configdb cs/xcnd25.comp.nus.edu.sg:27020,xcnd26.comp.nus.edu.sg:27020,xcnd27.comp.nus.edu.sg:27020,xcnd28.comp.nus.edu.sg:27020,xcnd29.comp.nus.edu.sg:27020


##on mongos
sh.addShard("rs0/xcnd25.comp.nus.edu.sg:27017");
sh.addShard("rs1/xcnd26.comp.nus.edu.sg:27017");
sh.addShard("rs2/xcnd27.comp.nus.edu.sg:27017");
sh.addShard("rs3/xcnd28.comp.nus.edu.sg:27017");
sh.addShard("rs4/xcnd29.comp.nus.edu.sg:27017");

sh.enableSharding("mongo_warehouse")