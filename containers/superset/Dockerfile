FROM apache/superset:latest
USER root
RUN apt-get -y update
RUN pip install pip 
RUN pip install sqlalchemy-redshift
RUN pip install psycopg2
# ENV ADMIN_USERNAME $ADMIN_USERNAME
# ENV ADMIN_EMAIL $ADMIN_EMAIL
# ENV ADMIN_PASSWORD $ADMIN_PASSWORDP

COPY ./superset-init.sh /superset-init.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT [ "/superset-init.sh" ]