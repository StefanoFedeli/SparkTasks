# Spark Assignment

# Install Environment
(You can skip this step if you have already a Spark 2.4.5 environment ready)
## Pre requisites
* Docker installed
* Python3 and Java installed

## Build the images

The first stepvwill be the build of the custom images, these builds can be performed with the *build-images.sh* script. 

```sh
chmod +x build-images.sh
./build-images.sh
```
## Run cluster
```sh
docker-compose up
```

# Run Application
Move to *app* folder and execute the script you prefer.
```sh
./task<n>.sh
```
Make sure to have the right filenames in the rigth folder.

## Requirements
- [X]  Every task should be a separate Apache Spark application.
- [X]  The application server should be implemented in Scala.
- [X]  Clear instruction on how to use them.

## Known Issues
1. Task4 was not implemented
2. Task3 raise a ClassNotFound Exception
