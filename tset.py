import base64
import os
import datetime
import pyodbc

# Определяем строку подключения к базе TNG
server = 'localhost'
connectStringShort = 'DSN=OracleODBC;PWD=hrs'
connectString = f'DRIVER=Oracle in instantclient_19_10;DBQ={server}:1521/club;UID=club;PWD=hrs'
# Определение рабочего каталога "D:\MEDIA"
os.chdir("D:\\MEDIA")
# Складываем текущую директорию в переменную
this_dir = os.getcwd()

# Обьявляем глобальную переменную для списка синхронизации
list_to_sync = []
list_to_update = []


# Функция получения версии файла (timestamp атрибута "Изменен")
def modification_date(filename):
    t = os.path.getmtime(filename)
    d = datetime.datetime.fromtimestamp(t).replace(microsecond=0)
    # print(type(d))
    return d


# Функция конвертирования файлов в base64
def converter(x):
    with open(x, 'rb') as img_file:
        img_base64_string = base64.b64encode(img_file.read())
    return img_base64_string


# Функция формирования SQL запроса на INSERT OR UPDATE

def insert_query_builder():
    print('Preparing INSERT SQL TRANSACTION')
    # Подключаемся к базе данных
    connection = pyodbc.connect(connectString)
    print("Connection established!")
    # Создаем обьект курсора
    cursor = connection.cursor()
    try:
        # Определяем формат запроса
        tng_insert_query = "INSERT INTO SIGUR_PHOTO_SYNC(PHOTO_ID, PHOTO_BIN, PHOTO_VERSION) VALUES (?, ?, ?);"
        # Выполняем множественный запрос на Insert, передаем в него список на синхронизацию
        print('Executing SQL QUERY...')
        cursor.executemany(tng_insert_query, list_to_sync)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into TNG database")
        print('SQL Transaction done...\n')
    # Обработка ошибки и откат
    except pyodbc.Error as error:
        connection.rollback()
        print("Failed to insert record into Oracle database {}".format(error))

    finally:
        cursor.close()
        connection.close()
        print('TNG connection is closed')


def update_query_builder():
    print('Preparing INSERT SQL TRANSACTION')
    # Подключаемся к базе данных
    connection = pyodbc.connect(connectString)
    print("Connection established!")
    # Создаем обьект курсора
    cursor = connection.cursor()
    for i in list_to_update:
        try:
            # Определяем формат запроса
            tng_update_query = f"UPDATE SIGUR_PHOTO_SYNC SET PHOTO_BIN = {i[0]}, PHOTO_VERSION = {i[1]} WHERE PHOTO_ID = {i[2]};"
            # Выполняем множественный запрос на Insert, передаем в него список на синхронизацию
            print('Executing SQL QUERY...')
            cursor.execute(tng_update_query)
            connection.commit()
            print(cursor.rowcount, "Record updated successfully into TNG database")
            print('SQL Transaction done...\n')
        # Обработка ошибки и откат
        except pyodbc.Error as error:
            connection.rollback()
            print("Failed to update record into Oracle database {}".format(error))
        finally:
            cursor.close()
            connection.close()
            print('TNG connection is closed')


# Цикл обработки файлов в директории D:\MEDIA
# r=root, d=directory, f=file
# Подключаемся к базе данных
def sync_list_builder():
    print('Start sync list builder')
    connection = pyodbc.connect(connectString)
    # Создаем обьект курсора
    cursor = connection.cursor()
    try:
        for r, d, f in os.walk(this_dir):
            # ignore "docs" folders
            if "docs" not in r:
                for file in f:
                    if (file.endswith(".jpg")) and (len(file) < 10):
                        # Получаем полный путь к файлу
                        photo_path = os.path.join(r, file)
                        # Получаем id
                        photo_id = file.replace(".jpg", "")
                        # Получаем параметр " Изменен "
                        photo_mod_date = modification_date(photo_path)
                        # Get & compare function
                        select_query_string = f'SELECT PHOTO_VERSION FROM SIGUR_PHOTO_SYNC WHERE PHOTO_ID = {photo_id}'
                        # //print(select_query_string)
                        cursor.execute(select_query_string)
                        result = cursor.fetchone()
                        # //print(result[0], type(result[0]), 'result')
                        # //print(photo_mod_date, type(photo_mod_date), 'mod_date')
                        # Если запрос вернулся пустой добавляем файл в список на Insert
                        if result is None:
                            insert_query_unit = int(photo_id)
                            q = open(photo_path, 'rb')
                            photo_bin = q.read()
                            q.close()
                            try:
                                # Определяем формат запроса
                                tng_insert_query = f"INSERT INTO SIGUR_PHOTO_SYNC(PHOTO_ID, PHOTO_BIN, PHOTO_VERSION) VALUES (?, ?, ?);"
                                # Выполняем множественный запрос на Insert, передаем в него список на синхронизацию
                                print('Executing SQL QUERY...')
                                cursor.execute(tng_insert_query, insert_query_unit, photo_bin, photo_mod_date)
                                connection.commit()
                                print('SQL Transaction done...\n')
                            # Обработка ошибки и откат
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to insert record into Oracle database {}".format(error))
                        # Если даты равны пропускаем..
                        elif result[0] == photo_mod_date:
                            print('Photo is actual')
                        # Если не пусто и не равный запросу, значит новый, - добавляем в UPDATE
                        else:
                            # setup update unit
                            update_query_unit = int(photo_id)
                            q = open(photo_path, 'rb')
                            up_photo_bin = q.read()
                            q.close()
                            try:
                                # Определяем формат запроса
                                tng_update_query = f"UPDATE SIGUR_PHOTO_SYNC SET PHOTO_BIN = ?, PHOTO_VERSION = ? WHERE PHOTO_ID = ?;"
                                # Выполняем множественный запрос на Insert, передаем в него список на синхронизацию
                                print('Executing SQL QUERY...')
                                cursor.execute(tng_update_query, update_query_unit, up_photo_bin, photo_mod_date)
                                connection.commit()
                                print('SQL Transaction done...\n')
                            # Обработка ошибки и откат
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to update record into Oracle database {}".format(error))
    # Обработка ошибки и откат
    except pyodbc.Error as error:
        connection.rollback()
        print("Failed to select record from Oracle database {}".format(error))
    finally:
        cursor.close()
        connection.close()


sync_list_builder()
