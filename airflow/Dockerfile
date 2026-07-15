FROM apache/airflow:3.3.0-python3.12

# Muda para o usuário root para instalar pacotes do sistema (Java)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Configura a variável de ambiente do Java para o PySpark encontrar
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Retorna para o usuário seguro do airflow
USER airflow

# Copia o requirements da raiz do projeto e instala
COPY ../requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Instala o PySpark
RUN pip install --no-cache-dir pyspark==3.5.1 pandas pyarrow