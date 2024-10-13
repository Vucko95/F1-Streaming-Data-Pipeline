FROM apache/superset:latest
USER root
RUN apt-get -y update
RUN pip install pip 
RUN pip install sqlalchemy-redshift
RUN pip install psycopg2

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
COPY wait-for-superset.sh /app/wait-for-superset.sh
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# Make the scripts executable
RUN chmod +x /superset-init.sh /app/wait-for-superset.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]
# ENTRYPOINT [ "/bin/bash", "-c", "/superset-init.sh && /wait-for-superset.sh" ]

