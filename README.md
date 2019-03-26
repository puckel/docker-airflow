
I've basically automated a process that will run on your local machine. The process will:

- Setup helm on your cluster
- Get a version of Airflow running on this cluster that can spawn jobs off to said cluster.

For now, I'm not doing anything company specific, just running test workflows in airflow.

First, just clone the following git: https://github.com/dafrenchyman/docker-airflow.git

Go into the "kubernetes" folder and read the instructions at the top of the "install.sh" bash script to install docker and enable kubernetes on your mac. For your convenience, these are the instructions:

##############################################################
# OSX instructions:
# Install docker and enable kubernetes:
#
# https://docs.docker.com/docker-for-mac/install/
#
# enable kubernetes:
# - Top right go Docker -> "Preferences..." -> "Kubernetes"
#   - Check mark "enable kubernetes"
#   - Select "kubernetes" instead of swarm
##############################################################

Once that is done, just run the "install.sh" bash script.

When it's done running, it'll probably still be setting stuff up, but you'll eventually be able to hit: http://localhost:8080/admin

To test out spawning kubernetes tasks, just turn on the "kubernetes_sample" DAG. 

If you want to shut down airflow, just check out the functions being run at the end of the script and comment out the running lines and uncomment the "uninstall_airflow" function.

There's also a really handy "Factory Reset" option in docker that I've been using all the time to just nuke everything and start over.

```bash
docker build -t docker-airflow .
docker tag docker-airflow dafrenchyman/docker-airflow:latest
docker push dafrenchyman/docker-airflow:latest
```