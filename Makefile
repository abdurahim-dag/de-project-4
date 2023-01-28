0-start:
	docker compose up -d --no-deps --build

1-copy-files:
	docker cp ./src/check-db.sh de-project-airflow:/lessons/check-db.sh
	docker cp variables.json de-project-airflow:/lessons/variables.json

2-check-db:
	docker exec de-project-airflow bash -c "/lessons/check-db.sh"

3-add-conn-pg-orig:
	docker exec de-project-airflow bash -c "airflow connections add \"PG_ORIGIN_BONUS_SYSTEM_CONNECTION\" --conn-json '{\
    \"conn_type\": \"postgres\",\
    \"login\": \"student\",\
    \"password\": \"${passPG}\",\
    \"host\": \"rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net\",\
    \"port\": \"6432\",\
    \"schema\": \"de-public\",\
    \"extra\": {\"sslmode\": \"require\"}\
}'"

4-add-conn-pg-dwh:
	docker exec de-project-airflow bash -c "airflow connections add \"PG_WAREHOUSE_CONNECTION\" --conn-json '{\
    \"conn_type\": \"postgres\",\
    \"login\": \"jovyan\",\
    \"password\": \"jovyan\",\
    \"host\": \"localhost\",\
    \"port\": \"5432\",\
    \"schema\": \"de\",\
    \"extra\": {\"sslmode\": \"disable\"}\
}'"

5-add-variables:
	docker exec de-project-airflow bash -c "airflow variables import /lessons/variables.json"

6-add-conn-api-delivery:
	docker exec de-project-airflow bash -c "airflow connections add \"API_DELIVERY\" --conn-json '{\
    \"conn_type\": \"http\",\
    \"host\": \"d5d04q7d963eapoepsqr.apigw.yandexcloud.net\",\
    \"schema\": \"https\",\
    \"extra\": {\"X-Nickname\": \"ragimatamov\", \"X-Cohort\": \"6\", \"X-API-KEY\": \"${api-key}\"}\
}'"

1-stop:
	docker compose down -v

start: 0-start 1-copy-files 2-check-db 3-add-conn-pg-orig 4-add-conn-pg-dwh 5-add-variables 6-add-conn-api-delivery
stop: 1-stop
