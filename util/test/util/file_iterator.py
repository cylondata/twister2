import os


def process_directory(dir_path, config_files=[]):
    for filename in os.listdir(dir_path):
        path = os.path.join(dir_path, filename)
        if os.path.isdir(path):
            process_directory(path, config_files)
        else:
            if filename.endswith(".json"):
                config_files.append(path)
                print("found test definition", path)
