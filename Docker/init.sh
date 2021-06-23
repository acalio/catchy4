#stop the firewall
systemctl stop firewalld
#restard the docker service
systemctl restart docker 

docker-compose up -d
