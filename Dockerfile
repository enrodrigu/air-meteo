FROM apache/airflow:2.10.4

# Passer en root pour l'installation de dépendances système
USER root

# Installer Java et procps pour ps command
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME et l'ajouter au PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Revenir à l'utilisateur airflow
USER airflow

# Installer les dépendances Python pour Spark
RUN pip install --no-cache-dir \
  "apache-airflow==2.10.4" \
  "apache-airflow-providers-apache-spark==5.1.1"
