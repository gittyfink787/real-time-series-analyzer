from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import os
import time
import schedule
import threading
import math


class FileProcessor(ABC):
    def __init__(self, path):
        self.path = path
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"The file in {self.path} did not found.")
        self.file = None  # saving the file pip in order to close it properly

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def readlines(self, pos):
        pass

    # @abstractmethod
    # def read_from_pos(self,pos):
    #     pass
    # return [
    #     f"{row['timestamp']},{row['value']}"
    #     for _,row in self.file.iterrows()]

    # open(self.path,'r') as f:
    #    f.seek(pos)
    #    return f.readlines()

    def get_size(self):
        return os.path.getsize(self.path)

    def get_lines_from_df(self, df, pos):
        if df is None or df.empty or pos >= len(df):
            return []

        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        new_rows = df.iloc[pos:]

        return [(row['timestamp'], row['value'])
                for _, row in new_rows.iterrows()]


class ExcelProcessor(FileProcessor):
    def read(self):
        return pd.read_excel(self.path)

    def write(self):
        return pd.to_excel(self.path)

    def readlines(self, pos):
        df = self.read()
        return self.get_lines_from_df(df, pos)


class ParquetProcessor(FileProcessor):
    def read(self):
        return pd.read_parquet(self.path)

    def write(self):
        return pd.to_parquet(self.path)

    def readlines(self, pos):
        df = self.read()
        return self.get_lines_from_df(df, pos)


class CheckData:
    def __init__(self, file_processor: FileProcessor):
        self.file_processor = file_processor

    def get_df(self):
        return self.file_processor.read()

    def get_invalid_dates(self):
        df = self.get_df()

        def is_valid_date(date_column):
            try:
                date_str = str(date_column).strip()
                datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                return True
            except ValueError:
                return False

        # for a boolian validation answer only
        # return df.iloc[:,0].apply(is_valid_date).all()

        invalid_rows = df[~df.iloc[:, 0].apply(is_valid_date)]
        return invalid_rows

    def get_duplicated_records(self):
        df = self.get_df()
        duplicates = df.duplicated()

    def find_outliners(self):
        """returns all rows with a numeric value greater or smaller than avg+3 std"""
        df = self.get_df()
        num_value = pd.to_numeric(df.iloc[:, 1], errors='coerce')
        list_value = list(df.iloc[:, 1])

        avg = num_value.mean()
        std = num_value.std()
        ALLOWD_STD = 3

        mask = []
        for val in list_value:
            if (not isinstance(val, (int, float))
                    or np.isnan(val)
                    or avg - ALLOWD_STD * std < val < avg + ALLOWD_STD * std):  # a not numeric value does not included in the outliner values

                mask.append(False)  # this is not a outliner value

            else:
                mask.append(True)

        return df[mask]


class CalAverage(ABC):
    def __init__(self, file_processor):
        self.file_processor = file_processor

    @abstractmethod
    def average(self, pathes=None):
        pass


class AvgDividedData(CalAverage):
    def get_df(self):
        return self.file_processor.read()

    def divide_data(self):
        df = self.get_df()
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        pathes = []
        output_dir = "process_data_2"
        os.makedirs(output_dir, exist_ok=True)

        grouped = df.groupby(df['timestamp'].dt.date)

        for date, group in grouped:
            file_name = os.path.join(output_dir, f"data_{date}.csv")
            group.to_csv(file_name, index=False)
            pathes.append(file_name)

        return pathes

    def average(self, pathes):
        total_sum = 0
        total_count = 0

        for path in pathes:
            df = pd.read_csv(path)
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            total_sum += df['value'].sum()
            total_count += df['value'].count()

        return total_sum / total_count

    def divide_and_calc_avg(self):
        return self.average(self.divide_data())


class AvgDataStream(CalAverage):
    def __init__(self, file_processor):
        super().__init__(file_processor)
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_pos = 0
        self.last_count = 0
        self.last_sum = 0

        with open('hourly_update.txt', 'w') as f:
            f.truncate(0)

        self._stop_event = threading.Event()

        self.thread_read = threading.Thread(target=self.watch_file, daemon=True)
        self.thread_read.start()

        self.job = schedule.every().hour.at(":00").do(self.average)
        self.thread_analyze = threading.Thread(target=self.run_schedule, daemon=True)
        self.thread_analyze.start()

    def read_new_data(self):
        print(f"{datetime.now()}: Reading new data")
        curr_pos = self.file_processor.get_size()
        if curr_pos > self.last_pos:
            # with self.file_processor as f:
            #     f.seek(self.last_size)
            #     lines=f.readlines()
            lines = self.file_processor.readlines(self.last_pos)
            self.last_pos = curr_pos
            return self.write_to_buffer(lines)

    def write_to_buffer(self, lines):
        with self.buffer_lock:
            for line in lines:
                timestamp, value = line[0], line[1]
                self.buffer.append({'timestamp': timestamp,
                                    'value': value})

    def average(self):
        values = []
        with self.buffer_lock:
            if self.buffer:
                values = [d['value'] for d in self.buffer if pd.notna(d['value'])]
                self.buffer.clear()
            else:
                self.write_to_file(None, None)

        curr_count = len(values)
        curr_sum = sum(values)

        total_avg = (self.last_sum + curr_sum) / (self.last_count + curr_count)
        last_hour_avg = curr_sum / curr_count

        self.write_to_file(last_hour_avg, total_avg)

        self.last_count += curr_count
        self.last_sum += curr_sum

    def write_to_file(self, last_hour_avg, total_avg):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        with open("hourly_update.txt", 'a') as r:
            if last_hour_avg:

                r.write(f"Analysis at {now}  :Total average:  {total_avg}, Last hour average: {last_hour_avg}\n")
            else:
                r.write(f"No data in the last hour at {now} \n")

    def watch_file(self):
        while not self._stop_event.is_set():
            self.read_new_data()
            time.sleep(10)

    def run_schedule(self):
        while not self._stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

    def __del__(self):
        self._stop_event.set()
        schedule.clear()


if __name__ == "__main__":
    fp = ExcelProcessor(
        r"C:\Users\The user\Documents\לימודי מחשבים\שנה ג\סמסטר ב\הדסים\HomeworkProject\time_series.xlsx")

    obj = AvgDataStream(fp)

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping")
        del obj

    # check_data=CheckData(fp)
    # avg_divided=AvgDividedData(fp)
    # print(check_data.get_invalid_dates())
    # print(check_data.get_duplicated_records())
    # print(check_data.find_outliners())
    # print(type(check_data.get_df().iloc[0,0]))
    # print (avg_divided.divide_and_calc_avg())
