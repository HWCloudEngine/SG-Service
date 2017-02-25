# SG-Service
## Installation
1. install sg-service-client
* $git clone https://github.com/Hybrid-Cloud/SG-Service-Client.git
* $cd SG-Service-Client
* $pip install -r requirements.txt
* $python setup.py install

2. install sg-service;
* $git clone https://github.com/Hybrid-Cloud/SG-Service.git
* $cd SG-Service
* $pip install -r requirements.txt
* $python setup.py install

3. create sgservice database;
* $mysql -u root -p
* mysql> create database sgservice;
* mysql> GRANT ALL PRIVILEGES ON sgservice.* TO 'sgservice'@'localhost' \
  IDENTIFIED BY 'SGSERVICE_DBPASS';
* mysql> GRANT ALL PRIVILEGES ON sgservice.* TO 'sgservice'@'%' \
  IDENTIFIED BY 'SGSERVICE_DBPASS';

4. create service entity in keystone;
* $ keystone user-create --name sgservice --pass {password}
* $ keystone user-role-add --user sgservice --role service --tenant service
* $ keystone service-create --type sg-service --name sgservice --description "Huawei Storage Gateway Service"
* $ keystone endpoint-create --region RegionOne --service {service_id} --publicurl "http://127.0.0.1:8975/v1/\$(tenant_id)s" 
  --internalurl "http://127.0.0.1:8975/v1/\$(tenant_id)s" --adminurl "http://127.0.0.1:8975/v1/\$(tenant_id)s"

5. Populate the sgservice database;
* sgservice-manage db sync

6. Start sgservice-api sgservice-controller;
* sgservice-api --config-file /etc/sgservice/sgservice.conf --log-file /var/log/sgservice.log
* sgservice-controller --config-file /etc/sgservice/sgservice.conf --log-file /var/log/sgservice.log

