# Welcome to CSED-332 Project
We are working on designing and implementing distributed sorting algorithm with Scala.

You can check out the [design docs](https://github.com/seonghyun26/332project/tree/main/docs) and [weekly progress logs](https://github.com/seonghyun26/332project/tree/main/log).

# How to Run

1. First, build the project. The commands below create worker.jar and master.jar.
```sh
sbt "worker/assembly"
sbt "master/assembly"
```

2. Now run the scripts.

For master,
```sh
./master.sh <# of slaves> [rpc port to bind to]
```

For slave,
```sh
./slave.sh <master IP address>:<master rpc port> -I <input directory> [input directory...] -O <output directory> [-P <inter-slave rpc port>]
```
