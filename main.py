import os
from jobs.run_pipeline import run_all_jobs

if __name__ == "__main__":
    jar_path = os.path.abspath("jars/sqlite-jdbc-3.50.2.0.jar")
    db_path = os.path.abspath("data/case_study.db")
    json_path = os.path.abspath("data/readings/")
    run_all_jobs(jar_path, db_path, json_path)