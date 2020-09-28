import csv
import xml.etree.cElementTree as xml
import secrets
import string
import random
import os
import zipfile
import glob
import time
import multiprocessing

number = 10
count = 100


# сгенерировать xml файл по правилам из задания
def create_xml(file):
    global number
    global count
    root = xml.Element("root")
    var1 = xml.Element("var")
    var1.set('name', 'id')
    var1.set('value', ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                              for i in range(random.randint(0, number) + 1)))
    var2 = xml.Element("var")
    var2.set('name', 'level')
    var2.set('value', str(random.randint(0, count) + 1))
    root.append(var1)
    root.append(var2)
    objects = xml.Element('objects')
    for i in range(random.randint(0, number) + 1):
        obj = xml.Element('object')
        obj.set('name', ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                                for i in range(random.randint(0, number))))
        objects.append(obj)
    root.append(objects)
    tree = xml.ElementTree(root)
    with open(file, "wb") as fh:
        tree.write(fh)


# соханить созданные xml файл
def generate_files(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    for i in range(count):
        file = dir + '/' + str(i + 1) + '_file.xml'
        create_xml(file)


# заархивировать директорию, в которой лежат zip файлы
def zipdir(path, zipf):
    # zipf is zipfile handle
    os.chdir(path)
    for file in os.listdir('.'):
        if not file.endswith('zip'):
            zipf.write(file)
    os.chdir('../..')


# очитсить директорию от xml -файлов
def purge(dir, pattern):
    files = glob.glob(os.path.join(dir, pattern))
    for file in files:
        try:
            os.remove(file)
        except:
            print("Error while deleting file : ", file)


# функция обхода директории result и архивирования поддиректорий, в которых находят xml файлы
def zip_files():
    for i in range(int(count / 2)):
        generate_files('result/{}'.format(str(i + 1)))
        with zipfile.ZipFile('result/{0}/{0}.zip'.format(i + 1), 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipdir('result/{}'.format(i + 1), zipf)
        purge('result/{}'.format(str(i + 1)), '*.xml')


# метод загрузки xml-файла в память
def read_zipfile_xml(archive, file):
    xml_file = archive.read(file)
    tree = xml.fromstring(xml_file.decode('utf-8'))
    return tree


# запись в csv файл данных, требуемых по 2 части задания
def define_data(file):
    global count
    divided = file.split('/')
    file1 = open(os.path.join(divided[0], divided[1], divided[1] + '_levels.csv'), 'w', newline='', encoding='utf-8')
    file2 = open(os.path.join(divided[0], divided[1], divided[1] + '_objects.csv'), 'w', newline='', encoding='utf-8')
    levels = csv.writer(file1)
    obj = csv.writer(file2)
    levels.writerow(['id', 'level'])
    obj.writerow(['id', 'object_name'])
    with zipfile.ZipFile(file, 'r') as archive:
        for i in range(count):
            xml_file = read_zipfile_xml(archive, '{}_file.xml'.format(str(i + 1)))
            root = xml_file.getchildren()
            levels.writerow([root[0].get('value'), root[1].get('value')])
            obj_tree = root[2].getchildren()
            for object in obj_tree:
                obj.writerow([root[1].get('value'), object.get('name')])
    file1.close()
    file2.close()


# функция для обхода директории result и записи данных в csv-файлы
def parse_databox(databox: list):
    for element in databox:
        define_data('result/{0}/{0}.zip'.format(element))


if __name__ == '__main__':
    dir_list = os.listdir('result')
    cores = multiprocessing.cpu_count()
    sub_lists = [[] for i in range(cores)]
    for i in range(len(dir_list)):
        try:
            for element in sub_lists:
                element.append(dir_list.pop())
        except:
            break
    processes = []
    zip_files()
    for i in range(cores):
        p = multiprocessing.Process(target=parse_databox, args=(sub_lists[i], ))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    print('DONE')