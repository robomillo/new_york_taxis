FROM postgres
ENV POSTGRES_DB airflow
COPY init_db.sql /docker-entrypoint-initdb.d/
EXPOSE 5432
