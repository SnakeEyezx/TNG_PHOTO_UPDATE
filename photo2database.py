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


# Цикл обработки файлов в директории D:\MEDIA
# r=root, d=directory, f=file
# Подключаемся к базе данных
def sync_list_builder():
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
                        cursor.execute(select_query_string)
                        result = cursor.fetchone()
                        # Если запрос пустой, добавляем фото в в базу в бинарном виде(BLOB)
                        if result is None:
                            insert_query_unit = int(photo_id)
                            q = open(photo_path, 'rb')
                            photo_bin = q.read()
                            q.close()
                            try:
                                # Определяем формат запроса
                                tng_insert_query = f"INSERT INTO SIGUR_PHOTO_SYNC(PHOTO_ID, PHOTO_BIN, PHOTO_VERSION) VALUES (?, ?, ?);"
                                # Выполняем множественный запрос на Insert, передаем в него список на синхронизацию
                                cursor.execute(tng_insert_query, insert_query_unit, photo_bin, photo_mod_date)
                                connection.commit()
                            # Обработка ошибки и откат
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to insert record into Oracle database {}".format(error))
                        # Если даты равны пропускаем..
                        elif result[0] == photo_mod_date:
                            pass
                        # Если не пусто и не равный запросу, значит новый, - добавляем в UPDATE
                        else:
                            update_query_unit = int(photo_id)
                            q = open(photo_path, 'rb')
                            up_photo_bin = q.read()
                            q.close()
                            try:
                                # Определяем формат запроса
                                tng_update_query = f"UPDATE SIGUR_PHOTO_SYNC SET PHOTO_BIN = ?, PHOTO_VERSION = ? WHERE PHOTO_ID = ?;"
                                cursor.execute(tng_update_query, up_photo_bin, photo_mod_date, update_query_unit)
                                connection.commit()
                            # Обработка ошибки и откат
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to update record into Oracle database {}".format(error))
    # Обработка ошибки и откат
    except pyodbc.Error as error:
        print("Failed to select record from Oracle database {}".format(error))
    finally:
        cursor.close()
        connection.close()


sync_list_builder()
