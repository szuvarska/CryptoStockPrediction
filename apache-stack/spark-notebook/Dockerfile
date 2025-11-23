FROM apache/spark:3.4.1
USER root
# python + pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
	pip install --no-cache-dir jupyter numpy pandas matplotlib
RUN pip install --no-cache-dir pyspark==3.4.1

WORKDIR /opt/notebooks

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]	
