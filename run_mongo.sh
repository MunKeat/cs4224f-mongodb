##
##config server
screen -dmS node0_cs0 mongod --configsvr --replSet cs0 --port 27030 --dbpath=/temp/cs4224f/data-mongo/cs0_data --enableMajorityReadConcern
screen -dmS node0_cs1 mongod --configsvr --replSet cs0 --port 27031 --dbpath=/temp/cs4224f/data-mongo/cs1_data --enableMajorityReadConcern
screen -dmS node0_cs2 mongod --configsvr --replSet cs0 --port 27032 --dbpath=/temp/cs4224f/data-mongo/cs2_data --enableMajorityReadConcern

rs.initiate(
  {
    _id: "cs0",
    configsvr: true,
    members: [
      { _id : 0, host : "xcnd25.comp.nus.edu.sg:27030" },
      { _id : 1, host : "xcnd25.comp.nus.edu.sg:27031" },
      { _id : 2, host : "xcnd25.comp.nus.edu.sg:27032" },
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
screen -dmS node0_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs0 --enableMajorityReadConcern
screen -dmS node0_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs0 --enableMajorityReadConcern
screen -dmS node0_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs0 --enableMajorityReadConcern

rs.initiate(
  {
    _id: "rs0",
    members: [
      { _id : 0, host : "xcnd25.comp.nus.edu.sg:27017" },
      { _id : 1, host : "xcnd25.comp.nus.edu.sg:27018" },
      { _id : 2, host : "xcnd25.comp.nus.edu.sg:27019" },
    ]
  }
);


screen -dmS node0_s mongos --port 47017 --configdb cs0/xcnd25.comp.nus.edu.sg:27030,xcnd25.comp.nus.edu.sg:27031,xcnd25.comp.nus.edu.sg:27032

#### node1
screen -dmS node1_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs1 --enableMajorityReadConcern
screen -dmS node1_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs1 --enableMajorityReadConcern
screen -dmS node1_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs1 --enableMajorityReadConcern

rs.initiate(
  {
    _id: "rs1",
    members: [
      { _id : 0, host : "xcnd26.comp.nus.edu.sg:27017" },
      { _id : 1, host : "xcnd26.comp.nus.edu.sg:27018" },
      { _id : 2, host : "xcnd26.comp.nus.edu.sg:27019" },
    ]
  }
);


screen -dmS node1_s mongos --port 47017 --configdb cs0/xcnd25.comp.nus.edu.sg:27030,xcnd25.comp.nus.edu.sg:27031,xcnd25.comp.nus.edu.sg:27032

#### node2
screen -dmS node2_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs2 --enableMajorityReadConcern
screen -dmS node2_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs2 --enableMajorityReadConcern
screen -dmS node2_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs2 --enableMajorityReadConcern

rs.initiate(
  {
    _id: "rs2",
    members: [
      { _id : 0, host : "xcnd27.comp.nus.edu.sg:27017" },
      { _id : 1, host : "xcnd27.comp.nus.edu.sg:27018" },
      { _id : 2, host : "xcnd27.comp.nus.edu.sg:27019" },
    ]
  }
);


screen -dmS node2_s mongos --port 47017 --configdb cs0/xcnd25.comp.nus.edu.sg:27030,xcnd25.comp.nus.edu.sg:27031,xcnd25.comp.nus.edu.sg:27032


#### node3
screen -dmS node3_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs3 --enableMajorityReadConcern
screen -dmS node3_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs3 --enableMajorityReadConcern
screen -dmS node3_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs3 --enableMajorityReadConcern

rs.initiate(
  {
    _id: "rs3",
    members: [
      { _id : 0, host : "xcnd28.comp.nus.edu.sg:27017" },
      { _id : 1, host : "xcnd28.comp.nus.edu.sg:27018" },
      { _id : 2, host : "xcnd28.comp.nus.edu.sg:27019" },
    ]
  }
);


screen -dmS node3_s mongos --port 47017 --configdb cs0/xcnd25.comp.nus.edu.sg:27030,xcnd25.comp.nus.edu.sg:27031,xcnd25.comp.nus.edu.sg:27032

#### node4
screen -dmS node4_r0 mongod --port 27017 --dbpath=/temp/cs4224f/data-mongo/r0 --shardsvr --replSet rs4 --enableMajorityReadConcern
screen -dmS node4_r1 mongod --port 27018 --dbpath=/temp/cs4224f/data-mongo/r1 --shardsvr --replSet rs4 --enableMajorityReadConcern
screen -dmS node4_r2 mongod --port 27019 --dbpath=/temp/cs4224f/data-mongo/r2 --shardsvr --replSet rs4 --enableMajorityReadConcern

rs.initiate(
  {
    _id: "rs4",
    members: [
      { _id : 0, host : "xcnd29.comp.nus.edu.sg:27017" },
      { _id : 1, host : "xcnd29.comp.nus.edu.sg:27018" },
      { _id : 2, host : "xcnd29.comp.nus.edu.sg:27019" },
    ]
  }
);


screen -dmS node4_s mongos --port 47017 --configdb cs0/xcnd25.comp.nus.edu.sg:27030,xcnd25.comp.nus.edu.sg:27031,xcnd25.comp.nus.edu.sg:27032

##on mongos
sh.addShard("rs0/xcnd25.comp.nus.edu.sg:27017");
sh.addShard("rs1/xcnd26.comp.nus.edu.sg:27017");
sh.addShard("rs2/xcnd27.comp.nus.edu.sg:27017");
sh.addShard("rs3/xcnd28.comp.nus.edu.sg:27017");
sh.addShard("rs4/xcnd29.comp.nus.edu.sg:27017");

sh.enableSharding("mongo_warehouse");