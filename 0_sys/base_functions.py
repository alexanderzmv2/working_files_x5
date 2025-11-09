#библиотеки для работы функций
import os #для работы с папками
import io #для работы по расшифровке книги по паролю
import re
import sys
import pandas as pd
import shutil #для копирования в папку с бэкапами
import duckdb
import psycopg
import msoffcrypto #для работы по расшифровке книги по паролю
from pathlib import Path
from datetime import date, datetime #для использования дат
from getpass import getpass, getuser
import polars as pl
from xlsxwriter import Workbook
import glob
import gc
import logging

def open_file_info():
    print("""
        # Функция для открытия файла и передачи для дальнейшей обработки Excel файлов, CSV и Parquet
        # Переменные на вход:
        #  - file_path - путь к файлу
        #  - file_mask - маска наименовании файла
        #  - file_name_column - указывать ли отдельным столбцом наименование обрабатываемого файла
        #  - sheet_name_ - None если все листы, 0 - первый лист, остальное - если конкретное
        #  - header_ - строка с заголовками
        #  - skiprows_ - пропуск строк сверху
        #  - usecols_ - определенные столбцы к загрузке в Excel и для работы с Parquete файлами
        #  - password_ - пароль к книге Excel при необходимости
        """)
#
def open_file (file_path, file_mask = None, file_name_column = False, sheet_name_ = None, header_ = 0, skiprows_ = None, usecols_ = None, password_ = None, dtype_ = None, converters_ = None, index_col_ = None, delimiter_ = None, prqt_filters = None):
    #определение, файл ли это
    is_file = os.path.isfile(file_path)
    #определить расширение файла
    file_extension = os.path.splitext(file_path)[1].lower()
    #определяем движок в зависимости от типа файла (применимо только для excel файлов)
    if file_extension == '.xls':
        engine_ = 'xlrd'
    elif file_extension == '.xlsb':
        engine_ = 'pyxlsb'
    else:
        engine_ = 'openpyxl'
    #определяем наименование файла
    file_name = os.path.basename(file_path)
    #если маска пустая, то берем первую букву имени файла для выполнения условия
    file_mask = file_mask if file_mask is not None else file_name[0]
    #если это файл, движок определен и название файла попадает под маску - осуществляем перебор, также исключаем файлы блокираторы с началом ~$
    if is_file and file_mask in file_name and "~$" not in file_name:
        #если это файлы Excel
        if file_extension in ('.xls','.xlsx','.xlsb','.xlsm'):
            #открываем файл
            with open(file_path,'rb') as f:
                #если у нас Excel файл с паролем, то перед открытием мы его снимаем, password_ должен быть подан на вход
                if password_ is not None:
                    file_path = io.BytesIO()
                    file = msoffcrypto.OfficeFile(f)
                    file.load_key(password = password_) # Use password
                    file.decrypt(file_path)
                else:
                    file_path = f #если
                #открываем книгу Excel
                try:
                    df = pd.read_excel(file_path, 
                        sheet_name= sheet_name_,
                        engine = engine_, 
                        header= header_,
                        skiprows = skiprows_,
                        usecols = usecols_,
                        index_col = index_col_,
                        dtype = dtype_,
                        converters = converters_)
                    #если требуется указать в отдельном столбце наименование файла, который обрабатывает, ставим True на входе
                    #--если file_name_column = True, то также может быть учтен + дополнительно можно указать текстом из входных данных
                    if isinstance(file_name_column, (int, float)):
                        #если Boolean
                        if file_name_column: df['Filename'] = file_name
                    else:
                        #если String
                        if isinstance(file_name_column, (str, date)):
                            df['Filename'] = file_name_column
                    #отбивка об обработке
                    print(f"Обработан файл - '{file_name}")
                    #возвращаем датафрем
                    return df
                except Exception as e:
                    print(f"Не удалось обработать Excel файл - '{file_name}', с ошибкой - {e}")
        elif file_extension == '.gzip':
            with open(file_path) as f:
                try:
                    df = pd.read_parquet(file_path, 
                        columns = usecols_,
                        filters = prqt_filters,
                        engine='pyarrow')
                    #отбивка об обработке
                    print(f"Обработан файл - '{file_name}")
                    #возвращаем датафрем
                    return df
                except Exception as e:
                    print(f"Не удалось обработать parquet файл - '{file_name}', с ошибкой - {e}")
        elif file_extension == '.csv':
            with open(file_path) as f:
                #открываем книгу Excel
                try:
                    df = pd.read_csv(file_path, 
                        header= header_,
                        skiprows = skiprows_,
                        usecols = usecols_,
                        index_col = index_col_,
                        dtype = dtype_,
                        delimiter = delimiter_,
                        converters = converters_)
                    #если требуется указать в отдельном столбце наименование файла, который обрабатывает, ставим True на входе
                    #--если file_name_column = True, то также может быть учтен + дополнительно можно указать текстом из входных данных
                    if isinstance(file_name_column, (int, float)):
                        #если Boolean
                        if file_name_column: df['Filename'] = file_name
                    else:
                        #если String
                        if isinstance(file_name_column, (str, date)):
                            df['Filename'] = file_name_column
                    #отбивка об обработке
                    print(f"Обработан файл - '{file_name}")
                    #возвращаем датафрем
                    return df
                except Exception as e:
                    print(f"Не удалось обработать csv файл - '{file_name}', с ошибкой - {e}")
        else:
            print('Тип файла не поддерживайте функцией. Поддерживаются следующие форматы: xls, xlsx, xlsb, csv, parquet.')
                                                                      
def err_descr(unit_, exception_text):
    print(f"Ошибка: блок/функция '{unit_}', {exception_text}")
               
def if_success(unit_):
    print(f"Выполнено: блок/функция '{unit_}'")

#создание бэкапа файла
def backup_file(file_path, backup_folder):
    #извлекаем наименование файла и его тип
    file_name = os.path.split(file_path)[1].split(".")[0]
    file_extension = os.path.splitext(file_path)[1].lower()
    #изначальный порядковый номер при повторонее
    i = 0
    #проверяем наличие файла в папке
    while os.path.exists(backup_folder + "\\" + file_name + "_" + str(i) + file_extension):
        i+=1
    #копируем файл в папку с бэкапами, если файла нет, то добавляем, если есть, то добавляем с префиксом    
    try:
        shutil.copy2(file_path, backup_folder + "\\" + file_name + "_" + str(i) + file_extension)
        print(f'Файл {file_name} успешно сохранен в папке бэкапов')
    except Exception as e:
        err_descr(f'Ошибка при сохранении файла {file_name}', e)
        
# функция изменения атрибута файла "Только для чтения"
def access_file_just_read(path, mode):
    #изменения атрибутан на Запись: 1 - чтение, 0 - запись
    import stat
    os.chmod(path, stat.S_IREAD if mode == 1 else stat.S_IWRITE)

# функция для сохранения таблицы в паркетный файл
def save_to_parquet(df, filepath, filename):
    unit = 'Сохранение данных в актуальный файл Parquet'
    try:
        # проверяем есть ли такая папка
        create_folder(filepath)
        # сохранение
        df.to_parquet(filepath + "\\" + filename + ".gzip", compression='gzip', index = False)
        #статус по завершению обработки блока/функции
        if_success(unit)
    except Exception as e:
        err_descr (unit, e)

# функция для чтения sql файлов
def get_sql (path, query_name, params = None):
    with open(path + '\\' + query_name + '.sql', "r") as f:
        sql_qr = f.read()
        if params is not None:
            sql_qr = sql_qr.format(**params)
    return sql_qr

# запрос на получение данных и выгрузке в файл
def create_temp_csv_from_sql_query(connection_str, query_, path_to_save, filename_):
    # входные параметры
    r"""
    connection_str - строка соединения
    query_ - запрос sql
    path_to_save - папка сохранения (пример p_ = r'C:\Users\Aleksan.Zamuruev\Desktop\Work\1_digital\raw_data\raw_v1'), путь папки указывается всегда с буквой r перед апострофом
    filename_ - наименование файла (пример f_ = 'out.csv')
    delimiter_ - разделитель csv, по умолчанию ';'
    """
    try:
        # проверяем есть ли такая папка
        create_folder(path_to_save)
        # выполняем запрос и сохраняем в файл
        with psycopg.connect(connection_str) as conn: # открываем соединение к базе данных
            with conn.cursor() as cur: # открываем курсор
                with open(Path(path_to_save, filename_), 'wb') as csv_file: # открываем файл, куда хотим записать данные
                    with cur.copy("""COPY ({}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ';', ENCODING 'UTF8')""".format(query_)) as copy: # копия данных
                        for data in copy: # перебираем батчи
                            csv_file.write(data) # запись в файл
            # уведомление
            print(f"Запрос успешно выполнен и сохранен в файл {filename_}")
            print("-----------------------------------------------------")
    except Exception as e:
        print(f"Не удалось выполнить запрос и сохранить файл - {e}")
        sys.exit()

# текущее время
def cur_datetime():
    return datetime.now().strftime("%Y%m%d_%H%M")
# выполнение запросов по диапазонам
def execute_range_queries(connection_str, dt_start, dt_end, path_sql, qr_name, path_to_save, params = {}, exec_qr = False):
    try:
        # проходим циклом по всем периодам с начала года
        dt_processing = cur_datetime() # дата обработки файла
        for dt_month in pd.date_range(dt_start, dt_end,freq='ME'):
            date_from = dt_month.replace(day=1).date() # первый день месяца
            date_to = (date_from + pd.offsets.MonthEnd(1)).date() # последний день месяца
            filename = f"{qr_name}_{date_from}_{dt_processing}.csv" # путь сохранения))) 
            print ('Обработка запроса с выходным файлом -', filename)
            # если есть исполнительный запрос
            if exec_qr:
                exec_query(connection_str, path_sql = path_sql, qr_name = qr_name + '_exec', params = {'date_from':date_from, 'date_to':date_to})
            # основной запрос
            create_temp_csv_from_sql_query (connection_str, get_sql(path_sql, qr_name, {'date_from':date_from, 'date_to':date_to} | params), path_to_save, filename)
    except Exception as e:
        print(f"Цикл прерван с ошибкой - {e}")
        
# сохранение в эксель
def save_to_excel(df, path, filename, add_dt = False):
    # проверяем есть ли такая папка
    create_folder(path)
    # если есть время, добавляем в конец наименования файла
    filename = filename + '_' + cur_datetime() if add_dt else filename
    with pd.ExcelWriter(path + '\\' + filename + '.xlsx') as writer:
        df.to_excel(writer, float_format="%.2f", header=True, index=False)

# объединение файлов csv с помощью duckdb
# функция для перебора 
def concat_csv_and_proccesing_by_duckdb (path_folder, mode = 0, path_sql_ = None, qr_name_ = None, add_filename = False, date_pattern_from_filename = False):
    # path_folder format for 0: Path(r'C:\Users\Aleksan.Zamuruev\Desktop\Work\1_digital\raw_data\loyalty_movement_delivery_by_owner_agg\*.csv') - default mode
    # path_folder format for 1: Path(r'C:\Users\Aleksan.Zamuruev\Desktop\Work\1_digital\raw_data\margin_online_wo_agg')
    # modes: 0 = all in folder together, 1 = one by one in folder
    # add_filename - добавляет столбец с наименованием обработанного файла
    if mode == 0:
        path_folder = Path(path_folder + r"\*.csv")
        if qr_name_ is None: #читаем всё
            with duckdb.connect() as con:
                t_total = con.sql(f"select * from read_csv('{path_folder}', filename = {add_filename})").df()
        else:
            with duckdb.connect() as con: # если есть запрос, то делаем запрос, но на основании всей папке без перебора каждого файла
                t_total = con.sql(f"""
                    {get_sql(path_sql_,qr_name_,{'file':path_folder})}
                        """).df()
    else:
        t_total = pd.DataFrame()
        path_folder = Path(path_folder)
        for file in path_folder.glob('*.csv'):
            file_name = file.name.split('.')[0]
            file_path = file
            print('Обработка файла', file_name)
            with duckdb.connect() as con:
                t_df = con.sql(f"""
                    {get_sql(path_sql_,qr_name_,{'file':file_path})}
                       """).df()
            # если надо добавить имя файла
            if add_filename:
                t_df['filename'] = file_path.str.split(r"\\").str.get(-1)
            # объединяем
            t_total = pd.concat([t_total, t_df])
            t_df = None
            print('Обработан')
    # если параметр filename указан, извлекаем только название файла, а не полный путь
    if add_filename:
        t_total['filename'] = t_total.filename.str.split(r"\\").str.get(-1)
    # если в имени файла есть дата в формате "2024-12-31", то можно также извлечь дату вместо имени файла
    if date_pattern_from_filename:
        t_total['filename'] = t_total['filename'].apply(get_dates_from_name)
    # возвращаем результат
    return t_total

# создать папку
def create_folder(path_):
    if not os.path.isdir(path_):
        os.makedirs(path_)

# извлечь дату из текста
def get_dates_from_name(text_value, patttern = r"\d{1,4}-\d{1,2}-\d{1,2}"):
    # по умолчанию паттерн 2024-12-01 ГГГГ-ММ-ДД
    return pd.to_datetime(re.findall(patttern,text_value))[0]

# делаем прозвон соединения
def call_connection(connection_str):
    try:
        with psycopg.connect(connection_str) as conn: # открываем соединение к базе данных
            with conn.cursor() as cur: # открываем курсор
                cur.execute("select 1")
                print('Прозвон соединения выполнен успешно.')
    except Exception as e:
        print(f"Ошибка соединения - {e}")

# исполнение запроса из файла или текста
def exec_query(connection_str, qr_text = None, path_sql = None, qr_name = None, params = {}):
    try:
        with psycopg.connect(connection_str) as conn: # открываем соединение к базе данных
            with conn.cursor() as cur: # открываем курсор
                if qr_text is None:
                    cur.execute(get_sql(path_sql, qr_name, params))
                else:
                    cur.execute(qr_text)
                print('Запрос успешно выполнен.')
    except Exception as e:
        print(f"Ошибка соединения - {e}")
        
# строка подключения к edw
def conn_str(type_conn):
    if type_conn == 'edw':
        conn = f"dbname='ckdata' user='{getuser().lower()}' password='{getpass(prompt='Пароль: ')}' host='adb-edw.x5.ru' port='7433'"
    else:
        conn = 'Такого типа соединения нет в функции'
    return conn

# выгрузка данных из БД через Polars
def pl_read_sql_from_query (connection_str, qr_text) -> pl.LazyFrame:
    with psycopg.connect(connection_str) as conn:
        df = pl.read_database(
            query=qr_text,
            connection=conn,
        ).lazy()
    return df
# открыть excel через Polars
def pl_open_excel(filepath_, sheet_name_ = None) -> pl.LazyFrame:
    df = pl.read_excel(
        source = filepath_,
        table_name = sheet_name_
    ).lazy()
    return df
# читает папку с csv и сохраняет в один файл excel
def pl_concat_csv_n_save_excel(csv_path, path_save, file_name_save):
    # создаем единый дф
    all_files = glob.glob(os.path.join(csv_path, "*.csv"))
    # read csv files, creating a single lazy result framez
    frames = [pl.read_csv(file, separator = ';', try_parse_dates = True).lazy() for file in all_files]
    lf = pl.concat(frames, how="vertical")
    # finally collect the result
    df = lf.collect()
    # check folder, if not exists create
    create_folder(path_save)
    # save workbook
    with Workbook(path_save + '\\' + file_name_save + '_' + cur_datetime() + '.xlsx') as wb:
        df.write_excel(
            workbook=wb,
            worksheet="data",
            column_formats = {'dt_month':'DD.MM.YYYY'}
    )
    # clean
    df = None
    gc.collect()
    
# сохранить ленивый фрейм в excel
def pl_save_excel(lf, path_save, file_name_save, cur_dt = True):
    try:
        file_name = path_save + '\\' + file_name_save
        file_name = file_name + '_' + cur_datetime() if cur_dt else file_name
        with Workbook(file_name + '.xlsx') as wb:
                lf.collect().write_excel(
                    workbook=wb,
                    worksheet="data",
                    column_formats = {'dt_month':'DD.MM.YYYY'}
        )
    except Exception as e:
        print(f"Ошибка при сохранении - {e}")

# сохранить ленивый фрейм в csv        
def pl_save_csv(lf, path_save, file_name_save, cur_dt = True):
    try:
        file_name = path_save + '\\' + file_name_save
        file_name = file_name + '_' + cur_datetime() if cur_dt else file_name
        lf.sink_csv(file_name + '.csv', separator = ';', batch_size = 100_000)
        print(f"Файл - {file_name} успешно сохранен")
    except Exception as e:
        print(f"Ошибка при сохранении - {e}")

# парсинг имени файла для функции process_reports
def parse_filename(filename: Path, pattern: str) -> dict | None:
    """Разбирает имя файла с помощью регулярного выражения."""
    match = pattern.match(filename.name)
    if not match:
        logging.info(f"SKIPPED (invalid format): {filename.name}")
        return None
    try:
        report = match.group("report")
        report_date = match.group("report_date")
        download_dt = datetime.strptime(match.group("download_date"), "%Y%m%d_%H%M")
        return {
            "report_name": report,
            "date_of_report": report_date,
            "download_dt": download_dt,
            "full_name": filename
        }
    except ValueError:
        logging.info(f"SKIPPED (invalid date format): {filename.name}")
        return None
        
# инфо файл для функции для функции process_reports
def get_files_info(directory: Path, pattern: str) -> list:
    """Собирает информацию о файлах в указанной папке (размер > 1 КБ)."""
    files = []
    for file in directory.glob("*.csv"):
        if file.stat().st_size <= 1024: # Пропускаем файлы <= 1 КБ
            logging.info(f"SKIPPED (size <= 1KB): {file.name}")
            continue
        file_info = parse_filename(file, pattern)
        if file_info:
            files.append(file_info)
    return files

# перемещение файлов из загруженных в рабочую, из рабочей в архив
def process_reports(path_root: str) -> None:
    """
    Обрабатывает файлы отчётов в папке path_root.
    Файлы из raw перемещаются в processed, если они новые или свежее, а старые файлы архивируются.
    Логируются причины, по которым файлы не были перемещены.
    Args:
        path_root (str): Путь к корневой папке, содержащей raw, processed и archive.
    """
    # Настройка логирования
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
    # Определяем пути к папкам
    root_dir = Path(path_root)
    raw_dir = root_dir / "raw"
    processed_dir = root_dir / "processed"
    archive_dir = root_dir / "archive"
    # Убедимся, что папки существуют
    for directory in [raw_dir, processed_dir, archive_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    # Паттерн имени файла
    pattern = re.compile(
        r"(?P<report>.+?)_(?P<report_date>\d{4}-\d{2}-\d{2})_(?P<download_date>\d{8}_\d{4})\.csv"
    )
    # Получаем информацию о файлах
    raw_files = get_files_info(raw_dir, pattern)
    processed_files = get_files_info(processed_dir, pattern)
    # Словарь для быстрого поиска в processed
    processed_dict = {
        (f["report_name"], f["date_of_report"]): f
        for f in processed_files
    }
    # Обработка файлов
    for raw_file in raw_files:
        key = (raw_file["report_name"], raw_file["date_of_report"])
        raw_dt = raw_file["download_dt"]
        if key in processed_dict:
            # Сравниваем время скачивания
            processed_dt = processed_dict[key]["download_dt"]
            if raw_dt > processed_dt:
                # Перемещаем старый файл в archive
                old_file = processed_dict[key]["full_name"]
                shutil.move(old_file, archive_dir / old_file.name)
                logging.info(f"ARCHIVED: {old_file.name}")
                # Перемещаем новый файл в processed
                shutil.move(raw_file["full_name"], processed_dir / raw_file["full_name"].name)
                logging.info(f"UPDATED: {raw_file['full_name'].name}")
            else:
                logging.info(f"SKIPPED (older than existing): {raw_file['full_name'].name}")
        else:
            # Новый файл, перемещаем в processed
            shutil.move(raw_file["full_name"], processed_dir / raw_file["full_name"].name)
            logging.info(f"ADDED: {raw_file['full_name'].name}")
            
# читаем файлы csv из папки со схемой
def read_processed_data(path_csv: str, schema: dict) -> pl.LazyFrame:
    dfs = [
        pl.scan_csv(file, schema_overrides = schema, separator = ';').select(schema.keys())
        for file in glob.glob(path_csv)
    ]
    return pl.concat(dfs, how="vertical")
    
# читаем excel файлы из папки
def read_all_excel_files(folder_path):
    lf = pl.read_excel(folder_path).lazy()
    return lf

# проверяет на наличие записей в ленивой таблице
def check_lf_if_null(lf):
    if lf.limit(1).collect().is_empty():
        print('Таблица пустая')
    else:
        output_data = lf.collect()
        output_data.write_clipboard(separator=';')
        output_data = None
        print('Имеются записи в таблице, данные скопированы в буфер')
        gc.collect()