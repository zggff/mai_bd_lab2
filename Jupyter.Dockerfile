FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --no-cache-dir neopyter

RUN mkdir -p /home/jovyan/notebooks && chown -R jovyan:users /home/jovyan/notebooks

USER jovyan

CMD ["start-notebook.sh", "--LabApp.token=", "--LabApp.password=", "--allow-root"]
