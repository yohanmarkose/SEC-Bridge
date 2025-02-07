### Airflow - Windows installation :

load docker image for airflow : 
docker compose up -d

for admin user creation:
docker-compose run airflow-worker airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

docker 

### Airflow Cmds :
airflow dags list