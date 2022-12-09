# Welcome to CSED-332 Project
We are working on designing and implementing distributed sorting algorithm with Scala.

You can check out the [design docs](https://github.com/seonghyun26/332project/tree/main/docs) and [weekly progress logs](https://github.com/seonghyun26/332project/tree/main/log).

# How to Run

1. First build the project. The command belows make worker.jar, master.jar.
```sh
  chmod +x ./*.sh
  ./build.sh
```

2. Now run the scripts.

- For master,
```sh
  ./master.sh (\# of slaves)
```

- For slave,
```sh
  ./slave (master IP):(master port) -I (input directories) -O (output directory)
```
