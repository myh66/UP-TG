import inspect
import os
import random
import sys
import time
import functools
import threading
import pandas as pd
import traceback
import Base
import datetime

import Config


def get_calling_method_name():
    # 获取当前方法的调用栈
    stack = inspect.stack()
    # 获取调用当前方法的栈帧对象
    caller_frame = stack[1]
    # 获取调用当前方法的栈帧对象的局部变量
    local_vars = caller_frame[0].f_locals
    # 遍历局部变量，找到调用当前方法的方法名
    for var_name, var_value in local_vars.items():
        if inspect.ismethod(var_value) and var_value.__name__ == caller_frame[3]:
            return var_name


def get_current_date():
    """
    获取当前日期，格式为 YYYY-MM-DD。

    Returns:
        str: 当前日期字符串。
    """
    return datetime.datetime.now().strftime("%Y-%m-%d")


def create_log_file(log_folder):
    """
    创建每天的日志文件。

    Args:
        log_folder (str): 存放日志文件的文件夹路径。

    Returns:
        str: 日志文件的完整路径。
    """
    current_date = get_current_date()
    log_file_name = f"log_{current_date}.txt"
    log_file_path = os.path.join(log_folder, log_file_name)
    return log_file_path


def log_current_thread_and_datetime(level, message, log_folder):
    """
    输出当前线程名称和当前日期时间，并附带自定义消息，将日志写入指定文件。

    Args:
        level (str): 自定义日志级别字符串。
        message (str): 自定义消息字符串。
        log_folder (str): 存放日志文件的文件夹路径。
    """
    current_thread = threading.current_thread().name
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    log_message = f"{current_datetime}  {level} {current_thread} --- {get_calling_method_name()} : {message}"

    # 确保日志文件夹存在
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # 创建今天的日志文件
    log_file_path = create_log_file(log_folder)

    # 将日志写入文件
    with open(log_file_path, 'a', encoding="utf-8-sig") as log_file:
        print(log_message)
        log_file.write(log_message + '\n')


def get_file_modification_date(file_path):
    """
        获取当前file_path位置文件的“最后修改时间”
    """
    modified_timestamp = os.path.getmtime(file_path)
    modified_date = datetime.datetime.fromtimestamp(modified_timestamp)
    return modified_date


def get_formatter_datetime_now(formatter=Base.date_formatter_common) -> str:
    """
        Content:
            获取当前指定格式时间字符串

        Args:
            formatter: 时间格式化字符串
    """
    return datetime.datetime.now().strftime(formatter)


def execution_time_decorator(max_time):
    def decorator(func):
        def wrapper(*args, **kwargs):
            stop_event = threading.Event()
            exceeded_time_event = threading.Event()

            def monitor_execution_time():
                start_time = time.time()
                while not stop_event.is_set():
                    current_time = time.time()
                    execution_time = current_time - start_time
                    print(f"函数 {func.__name__} 已执行 {execution_time} 秒")
                    if execution_time > max_time:
                        exceeded_time_event.set()
                        break
                    time.sleep(1)

            monitor_thread = threading.Thread(target=monitor_execution_time)
            monitor_thread.start()

            result = func(*args, **kwargs)

            stop_event.set()
            monitor_thread.join()

            if exceeded_time_event.is_set():
                print(f"执行时间超过{max_time}秒，程序结束")
                # 可以选择在这里终止程序
                sys.exit("装饰器中结束函数执行")
            return result

        return wrapper

    return decorator


def writer_csv(new_rows):
    # 创建文件夹
    date_str = get_formatter_datetime_now(Base.date_formatter_date)
    folder_name = Base.up_file_path + os.sep + date_str

    if not os.path.isdir(folder_name):
        os.makedirs(folder_name, exist_ok=True)

    # CSV文件路径
    file_path = folder_name + os.sep + Base.up_file_name + ".csv"

    if new_rows == None:
        return file_path

    # 如果文件不存在
    if not os.path.isfile(file_path):
        # 创建文件
        df = pd.DataFrame(columns=Base.up_file_columns)
        # 保存到当前目录下
        df.to_csv(file_path, index_label=False)

    # 为 new 则创建文件
    if new_rows == "new":
        return

    # 新增一列
    df = pd.read_csv(file_path, encoding="UTF-8")

    # 新增一行
    df = df._append(new_rows, ignore_index=True)

    # 将 'ID' 列设置为索引列
    df = df.set_index('ID')

    # 保存到当前目录下
    df.to_csv(file_path)


def update_by_id(index, field, text):
    file_path = writer_csv(None)
    df = pd.read_csv(file_path, index_col="ID", encoding="UTF-8")

    # 根据INDEX赋值
    df[field].at[index] = text

    # 保存到当前目录下
    df.to_csv(file_path)


def update_by_filename(filename, field, text):
    file_path = writer_csv(None)
    df = pd.read_csv(file_path, index_col="ID", encoding="UTF-8")

    df.loc[df["FILE_NAME"] == filename, field] = text

    # 保存到当前目录下
    df.to_csv(file_path)


def get_csv_ID():
    return int(time.time()) + random.randint(1, 10)


def traceback_to_str(e):
    return ''.join(traceback.format_tb(e.__traceback__))


def find_df(field, text):
    file_path = writer_csv(None)
    df = pd.read_csv(file_path, index_col="ID", encoding="UTF-8")
    try:
        return df.loc[df[field] == text].index.values[0]
    except Exception as e:
        return None


def find_index(index):
    try:
        file_path = writer_csv(None)
        df = pd.read_csv(file_path, index_col="ID", encoding="UTF-8")
        return df.loc[index]
    except Exception as e:
        return None


