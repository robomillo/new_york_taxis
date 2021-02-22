FROM postgres
RUN apt-get update \
    && apt-get install -y unzip curl
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && ./aws/install
ENV POSTGRES_DB airflow
COPY init_db.sql /docker-entrypoint-initdb.d/
EXPOSE 5432