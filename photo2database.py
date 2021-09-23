import os
import datetime
import pyodbc

# Определяем строку подключения к базе TNG
server = 'localhost'
connectStringShort = 'DSN=OracleODBC;PWD=hrs'
connectString = f'DRIVER=Oracle in instantclient_19_10;DBQ={server}:1521/club;UID=club;PWD=hrs'
# Определение рабочего каталога "D:\MEDIA"
os.chdir("D:\\MEDIA")
this_dir = os.getcwd()


# Функция получения версии файла (timestamp атрибута "Изменен")
def modification_date(filename):
    t = os.path.getmtime(filename)
    d = datetime.datetime.fromtimestamp(t).replace(microsecond=0)
    return d


# Цикл обработки файлов в директории D:\MEDIA
# r=root, d=directory, f=file
def sync_list_builder():
    # Подключаемся к базе данных
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
                        # Получаем параметр "Изменен"
                        photo_mod_date = modification_date(photo_path)
                        # Get & compare function
                        select_query_string = f'SELECT PHOTO_VERSION FROM SIGUR_PHOTO_SYNC WHERE PHOTO_ID = {photo_id}'
                        cursor.execute(select_query_string)
                        result = cursor.fetchone()
                        # Если запрос пустой, добавляем фото в базу в бинарном виде(BLOB)
                        if result is None:
                            q = open(photo_path, 'rb')
                            photo_bin = q.read()
                            q.close()
                            try:
                                # Определяем формат запроса
                                tng_insert_query = f"INSERT INTO SIGUR_PHOTO_SYNC(PHOTO_ID, PHOTO_BIN, PHOTO_VERSION) VALUES (?, ?, ?);"
                                cursor.execute(tng_insert_query, int(photo_id), photo_bin, photo_mod_date)
                                connection.commit()
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to insert record into Oracle database {}".format(error))
                        # Если даты равны пропускаем..
                        elif result[0] == photo_mod_date:
                            pass
                        # Если дата позже чем в базе, - обновляем
                        else:
                            q = open(photo_path, 'rb')
                            up_photo_bin = q.read()
                            q.close()
                            try:
                                tng_update_query = f"UPDATE SIGUR_PHOTO_SYNC SET PHOTO_BIN = ?, PHOTO_VERSION = ? WHERE PHOTO_ID = ?;"
                                cursor.execute(tng_update_query, int(photo_id), photo_mod_date, update_query_unit)
                                connection.commit()
                            except pyodbc.Error as error:
                                connection.rollback()
                                print("Failed to update record into Oracle database {}".format(error))
    except pyodbc.Error as error:
        print("Failed to select record from Oracle database {}".format(error))
    finally:
        cursor.close()
        connection.close()


sync_list_builder()
