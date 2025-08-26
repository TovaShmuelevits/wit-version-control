import csv
import os
import shutil
import subprocess

from Commit import Commit

def create_new_folder_in_path(path):
    try:
        os.mkdir(path)
        subprocess.run(['attrib','+h',path])
    except FileExistsError as e:
        raise e



def create_new_file_in_folder(file_path, name_file):
    with open(file_path + "/" + name_file, 'w') as file:
        file.write("")


# העתקת הקובץ לניתוב
def copy_file(source_path, dest_path):
    shutil.copy(source_path, dest_path)


# מחיקת קובץ
def delete_file(path):
    os.remove(path)


# בדיקה האם התקיה ריקה
def is_empty_folder(path):
    if not os.listdir(path):
        return True
    return False


# החזרת רשימת הקבצים בתקיה
def list_files_in_folder(path):
    if not os.path.exists(path):
        print(f"The specified path does not exist: {path}")
        return []
    return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]


def is_valid_path(path):
    return os.path.exists(path)


def write_to_data_csv(path, commit):
    data_csv_path = os.path.join(path, ".wit", "data.csv")
    if not os.path.exists(os.path.dirname(data_csv_path)):
        print(f"The directory does not exist: {os.path.dirname(data_csv_path)}")
        return
    with open(data_csv_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(commit.get_list_value())


def read_1_row_from_data_csv(path, hash_code):
    with open(os.path.join(path, ".wit", "data.csv"), mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] == hash_code:
                return Commit(row[1], row[0], row[2])
    return None


def print_all_data_csv(path):
    with open(os.path.join(path, ".wit", "data.csv"), mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            c = Commit(row[1], row[0], row[2])
            print(c)


def is_empty_data_csv(path):
    return os.stat(path).st_size == 0


def last_hash_code_data_csv(path):
    with open(os.path.join(path, ".wit", "data.csv"), mode='r') as file:
        lines = file.readlines()
        last_line = lines[-1]
        last_value = last_line.strip().split(',')[0]
        return last_value
