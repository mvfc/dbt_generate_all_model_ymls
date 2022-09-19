import os
import subprocess
import re
import logging

# Initializing variables

models_dir = os.getcwd() + r"\models"

dir_list = os.listdir(models_dir)

# Defining logging options


def set_logging_options():
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)


# returns the list of everything inside that directory


def get_files_in_dir(dir):
    return os.listdir(dir)


# lists all model .sql files in specific directory and its subdirectories recursively


def list_models_in_dir(dir):
    model_list = []
    for file in os.listdir(dir):
        if "." not in file:
            inside_dir = list_models_in_dir(dir + rf"\{file}")
            for inside_file in inside_dir:
                if ".sql" in inside_file:
                    model_list.append(inside_file)
                else:
                    pass
        elif ".sql" in file:
            model_list.append(file)
        else:
            pass
    return model_list


# cleans the list by taking only the model name and ignoring the project name


def clean_model_list(model_list):
    clean_list = []
    for model in model_list:
        clean_list.append(model.split(".")[0])
    return clean_list


# runs the generate_model_yaml macro from dbt-codegen for a set model


def generate_node_yml(model_name):
    try:
        model_yml = subprocess.run(
            """dbt run-operation generate_model_yaml --args \"{\'model_name\':\'"""
            + model_name
            + """\'}\" """,
            stdout=subprocess.PIPE,
        ).stdout.decode("utf-8")
    except ValueError:
        logging.warning(f"Error while running dbt command for model {model_name}")
    return model_yml


# ties everything together and produces the end result, which is a single YML file for each model


def run():
    set_logging_options()
    node_list = clean_model_list(list_models_in_dir(models_dir))
    for node in node_list:
        logging.info(f"Creating YML for model {node}")
        try:
            yml = re.sub(
                r"(\r\n){2,}", "\r\n", generate_node_yml(node).split("version: 2")[1]
            )
            yml = "version: 2\n\n" + yml
            try:
                os.mkdir("generated_ymls")
                logging.warning("Couldnt find folder generated_ymls, creating it...")
                logging.info("Folder generated_ymls created")
            except:
                pass
            try:
                f = open(os.getcwd() + "\\generated_ymls\\" + node + ".yml", "w")
                f.write(yml)
                f.close()
                logging.info(f"YML file for model {node} created successfuly")
            except:
                logging.warning(f"WARNING: Error on saving YML file for node {node}")
        except:
            logging.warning(f"WARNING: Failure generating YML for model {node}")


run()
