from pyspark.sql import SparkSession
from datetime import date

def read_last_processed_path(spark, meta_path):
    try:
        # Path to the last processed folder
        return spark.read.text(meta_path).first()[0]
    except:
        return None

def list_hdfs_dirs(spark, base_path):
    # base_path is a path to the folder of the data source e.g. crypto-prices
    
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI(base_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
    
    return sorted([
        f.getPath().getName()
        for f in fs.listStatus(path)
        if f.isDirectory()
    ])

def find_new_paths(spark, base_path, meta_path):
    
    last_path = read_last_processed_path(spark, meta_path)
    all_dirs = list_hdfs_dirs(spark, base_path)
    if last_path:
        new_dirs = [d for d in all_dirs if d > last_path]
        print(f"Last path: {last_path}")
    else:
        new_dirs = all_dirs  # pierwsze uruchomienie
        
    # Remove today -> not finished
    now = str(date.today())
    if now in new_dirs:
        print(f"Removing {now} from dirs - day has not finished yet")
        new_dirs.remove(str(now))
    
    if not new_dirs:
        print("No new data.")
    
    paths_to_read = [base_path + d for d in new_dirs]
    return paths_to_read, new_dirs