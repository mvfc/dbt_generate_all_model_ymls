import os
import subprocess
import re

models_dir = os.getcwd()+'\models'

dir_list = os.listdir(models_dir)

def get_files_in_dir(dir):
    return os.listdir(dir)

def list_models_in_dir(dir):
    model_list = []
    for file in os.listdir(dir):
        if '.' not in file:
            inside_dir = list_models_in_dir(dir+f'\{file}')
            for inside_file in inside_dir:
                if '.sql' in inside_file:
                    model_list.append(inside_file)
                else:
                    pass
        elif '.sql' in file:
            model_list.append(file)
        else:
            pass
    return model_list

def clean_model_list(model_list):
    clean_list = []
    for model in model_list:
        clean_list.append(model.split('.')[0])
    return clean_list

def generate_node_yml(model_name):
    try:
        model_yml = subprocess.run("""dbt run-operation generate_model_yaml --args \"{\'model_name\':\'"""+model_name+"""\'}\" """, stdout=subprocess.PIPE).stdout.decode('utf-8')
    except ValueError:
        print(f'Error while running dbt command for model {model_name}')
    return model_yml

node_list = clean_model_list(list_models_in_dir(models_dir))
for node in node_list:
    print(f'Creating YML for model {node}\n')
    try:
        yml = re.sub(r'(\r\n){2,}', '\r\n', generate_node_yml(node).split('version: 2')[1])
        yml = 'version: 2\n\n'+yml
    except:
        yml = generate_node_yml(node)
        print(f'Be careful with the header on YML for model {node}\n\r\n')
    try:
        os.mkdir('generated_ymls')
        print('Couldnt find folder generated_ymls, creating it...\n')
        print('Folder generated_ymls created.\n')
    except:
        pass
    try:
        f = open(os.getcwd()+'\generated_ymls\\'+node+'.yml', "w")
        f.write(yml)
        f.close()
        print(f'YML file for model {node} created successfuly\n')
    except:
        print(f'Error on generating YML for node {node}')
