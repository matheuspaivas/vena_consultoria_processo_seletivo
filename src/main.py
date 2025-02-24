from pipeline.pipeline import Pipeline
import yaml
import os

if __name__ == "__main__":
    
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "configs", "config.yaml")

    with open(CONFIG_PATH, "r") as config_file:
        config = yaml.safe_load(config_file)

    PROJECT_ID = config["project_id"]
    BUCKET_NAME = config["bucket_name"]
    DATASET_ID = config["dataset_id"]
    
    pipeline = Pipeline(PROJECT_ID, BUCKET_NAME, DATASET_ID)
    pipeline.run()
