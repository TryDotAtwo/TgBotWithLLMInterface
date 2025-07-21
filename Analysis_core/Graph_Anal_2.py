# -*- coding: utf-8 -*-
import os
import glob
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib
matplotlib.use('TkAgg')
from datetime import datetime, timedelta
import numpy as np
from dateutil.tz import tzutc
from tqdm import tqdm

class AnomalyDetector:
    def __init__(self, folder_path, column_index, sensor_name, debug_mode=False, glitches_folder="glitches"):
        self.folder_path = folder_path
        self.column_index = column_index
        self.sensor_name = sensor_name
        self.debug_mode = debug_mode
        self.glitches_folder = glitches_folder
        self.raw_data = []
        self.db_files = []
        self.times = None
        self.values = None
        self.times_filtered = None
        self.values_filtered = None
        self.sampling_interval = 3600
        self.results = {"glitches": []}

    def load_data(self):
        self.raw_data = []
        if not os.path.exists(self.folder_path):
            if self.debug_mode:
                print(f"Папка {self.folder_path} не существует!")
            return False

        self.db_files = glob.glob(os.path.join(self.folder_path, "*.db"))
        if not self.db_files:
            if self.debug_mode:
                print(f"В папке {self.folder_path} не найдено файлов .db!")
            return False

        for db_file in self.db_files:
            if self.debug_mode:
                print(f"Обрабатываю файл: {db_file}")
            try:
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data';")
                if not cursor.fetchone():
                    if self.debug_mode:
                        print(f"В файле {db_file} нет таблицы data, пропускаю.")
                    conn.close()
                    continue

                sql_query = f"SELECT [time@timestamp], data_format_{self.column_index} FROM data"
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                if not rows:
                    if self.debug_mode:
                        print(f"Нет данных в таблице data файла {db_file}.")
                    conn.close()
                    continue

                self.raw_data.extend(rows)
                if self.debug_mode:
                    print(f"Загружено {len(rows)} строк из файла {db_file}")
                conn.close()
            except Exception as e:
                if self.debug_mode:
                    print(f"Ошибка при обработке файла {db_file}: {e}")

        if not self.raw_data:
            if self.debug_mode:
                print("Нет данных для анализа.")
            return False
        if self.debug_mode:
            print(f"Всего загружено {len(self.raw_data)} строк данных.")
        return True

    def process_data(self):
        self.times = None
        self.values = None
        processed_data = []
        for ts, val in self.raw_data:
            try:
                ts_float = float(ts)
                dt = datetime.fromtimestamp(ts_float, tz=tzutc())
                processed_data.append((dt, val))
            except Exception as e:
                if self.debug_mode:
                    print(f"Ошибка преобразования времени {ts}: {e}")

        if not processed_data:
            if self.debug_mode:
                print("Нет данных для анализа после обработки.")
            return False

        processed_data.sort(key=lambda x: x[0])
        self.times, self.values = zip(*processed_data)
        self.times = np.array(self.times)
        self.values = np.array(self.values)

        df = pd.DataFrame({'time': self.times, 'value': self.values})
        df['time'] = pd.to_datetime(df['time'])
        df = df.resample('1min', on='time').mean().reset_index()
        self.times = df['time'].to_numpy()
        self.values = df['value'].to_numpy()

        self.sampling_interval = (self.times[1] - self.times[0]).total_seconds() if len(self.times) > 1 else 60
        if self.debug_mode:
            print(f"Sampling interval: {self.sampling_interval} seconds")
            print(f"Диапазон времени: от {self.times[0]} до {self.times[-1]}")
            print(f"Диапазон значений: от {np.min(self.values)} до {np.max(self.values)} К")
        return True

    def remove_glitches(self, threshold_delta=50, max_glitch_duration=10):
        os.makedirs(self.glitches_folder, exist_ok=True)
        max_glitch_points = int(max_glitch_duration / self.sampling_interval)
        diffs = np.abs(np.diff(self.values))
        glitch_starts = np.where(diffs > threshold_delta)[0] + 1
        mask_good = np.ones(len(self.values), dtype=bool)

        for idx, start in enumerate(glitch_starts):
            end = start
            base_value = self.values[start - 1] if start > 0 else self.values[0]
            while end < len(self.values) and (end - start) <= max_glitch_points and abs(self.values[end] - base_value) > threshold_delta:
                end += 1
            if (end - start) <= max_glitch_points and end < len(self.values):
                if abs(self.values[end] - base_value) <= 5:
                    mask_good[start:end] = False
                    glitch_start_time = self.times[start]
                    glitch_end_time = self.times[end]
                    glitch_mask = (self.times >= glitch_start_time - timedelta(minutes=5)) & (self.times <= glitch_end_time + timedelta(minutes=5))
                    times_glitch = self.times[glitch_mask]
                    values_glitch = self.values[glitch_mask]
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.plot(times_glitch, values_glitch, label=f'{self.sensor_name}', color='blue')
                    ax.axvspan(glitch_start_time, glitch_end_time, color='red', alpha=0.3)
                    ax.set_xlabel("Время")
                    ax.set_ylabel("Температура (К)")
                    ax.set_title(f"Глюк датчика {self.sensor_name} с {glitch_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    ax.grid(True)
                    ax.legend()
                    locator = mdates.AutoDateLocator()
                    formatter = mdates.AutoDateFormatter(locator)
                    formatter.scaled[1/24] = '%H:%M'
                    formatter.scaled[1] = '%d %b %H:%M'
                    ax.xaxis.set_major_locator(locator)
                    ax.xaxis.set_major_formatter(formatter)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    glitch_filename = f"glitch_{idx}_{glitch_start_time.strftime('%Y%m%d_%H%M%S')}.png"
                    glitch_filepath = os.path.join(self.glitches_folder, glitch_filename)
                    plt.savefig(glitch_filepath)
                    plt.close()
                    glitch_info = {
                        "start_time": glitch_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "end_time": glitch_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "plot_file": glitch_filepath
                    }
                    self.results["glitches"].append(glitch_info)
                    if self.debug_mode:
                        print(f"Обнаружен глюк датчика с {glitch_start_time.strftime('%Y-%m-%d %H:%M:%S')} до {glitch_end_time.strftime('%Y-%m-%d %H:%M:%S')}, удален. График сохранен: {glitch_filepath}")

        self.times_filtered = self.times[mask_good]
        self.values_filtered = self.values[mask_good]

    def detect_transitions(self, target_values=[20, 80, 300], tolerance=10, min_duration=300):
        transitions = []
        in_transition = False
        start_idx = 0
        working_levels = sorted(target_values)
        min_points = int(min_duration / self.sampling_interval)

        for i in tqdm(range(1, len(self.values_filtered)), desc="Обнаружение переходов"):
            prev_value = self.values_filtered[i-1]
            curr_value = self.values_filtered[i]
            
            # Определяем ближайший уровень для предыдущего и текущего значения
            prev_level = min(working_levels, key=lambda x: abs(prev_value - x)) if any(abs(prev_value - level) <= tolerance for level in working_levels) else None
            curr_level = min(working_levels, key=lambda x: abs(curr_value - x)) if any(abs(curr_value - level) <= tolerance for level in working_levels) else None
            
            # Начало перехода: выход из текущего уровня
            if not in_transition and prev_level and (abs(curr_value - prev_value) > tolerance):
                in_transition = True
                start_idx = i - 1
            # Конец перехода: стабилизация на новом уровне
            elif in_transition and curr_level and (abs(curr_value - self.values_filtered[max(0, i-10):i+1].mean()) < tolerance) and (i - start_idx >= min_points):
                in_transition = False
                end_idx = i
                transition_type = "отогрев" if curr_value > prev_value else "охлаждение"
                transitions.append({
                    "start_time": self.times_filtered[start_idx],
                    "end_time": self.times_filtered[end_idx],
                    "start_value": prev_value,
                    "end_value": curr_value,
                    "start_idx": start_idx,
                    "end_idx": end_idx,
                    "type": transition_type
                })

        return transitions

    def detect_general_anomalies(self, z_threshold=5, window_size=30):
        anomalies = []
        in_anomaly = False
        start_idx = 0

        window = np.ones(window_size) / window_size
        rolling_mean = np.convolve(self.values_filtered, window, mode='valid')
        rolling_std = np.sqrt(np.convolve((self.values_filtered - np.convolve(self.values_filtered, window, mode='same'))**2, window, mode='valid') / window_size)
        pad_left = (window_size - 1) // 2
        pad_right = window_size - 1 - pad_left
        rolling_mean = np.pad(rolling_mean, (pad_left, pad_right), mode='edge')
        rolling_std = np.pad(rolling_std, (pad_left, pad_right), mode='edge')
        z_scores = np.abs((self.values_filtered - rolling_mean) / rolling_std)

        for i in tqdm(range(len(self.values_filtered)), desc="Обнаружение аномалий"):
            current_value = self.values_filtered[i]
            is_peak = i > 0 and abs(current_value - self.values_filtered[i-1]) > 30  # Резкий скачок более 30 К
            is_outside = current_value < 0 or current_value > 350  # Выход за пределы
            if not in_anomaly and (z_scores[i] > z_threshold or is_peak or is_outside):
                in_anomaly = True
                start_idx = i
            elif in_anomaly and (z_scores[i] < z_threshold and not is_peak and not is_outside):
                in_anomaly = False
                peak_value = np.max(self.values_filtered[start_idx:i+1])
                baseline = rolling_mean[start_idx]
                amplitude = peak_value - baseline
                if amplitude > 20:  # Минимальная амплитуда
                    anomalies.append({
                        "start_time": self.times_filtered[start_idx],
                        "end_time": self.times_filtered[i],
                        "amplitude": amplitude,
                        "peak_value": peak_value,
                        "start_idx": start_idx,
                        "end_idx": i,
                        "type": "general"
                    })

        return anomalies

    def plot_anomalies(self, anomalies, transitions, output_dir="anomaly_plots"):
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(self.glitches_folder, exist_ok=True)

        plt.figure(figsize=(12, 6))
        plt.plot(self.times_filtered, self.values_filtered, label=f'{self.sensor_name}', color='green')
        for anomaly in anomalies:
            plt.axvspan(anomaly["start_time"], anomaly["end_time"], color='red', alpha=0.3, label='Аномалия' if anomaly == anomalies[0] else "")
        for transition in transitions:
            color = 'orange' if transition["type"] == "отогрев" else 'blue'
            label = 'Отогрев' if transition["type"] == "отогрев" and transitions.index(transition) == next(i for i, t in enumerate(transitions) if t["type"] == "отогрев") else \
                    'Охлаждение' if transition["type"] == "охлаждение" and transitions.index(transition) == next(i for i, t in enumerate(transitions) if t["type"] == "охлаждение") else ""
            plt.axvspan(transition["start_time"], transition["end_time"], color=color, alpha=0.3, label=label)
        plt.xlabel("Время")
        plt.ylabel("Температура (К)")
        plt.title("Общий график с отмеченными переходами и аномалиями")
        plt.legend()
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))
        plt.xticks(rotation=45)
        plt.grid()
        plt.tight_layout()
        plt.savefig(f"{output_dir}/overall_plot.png")
        plt.show()

        for i, anomaly in enumerate(anomalies):
            plt.figure(figsize=(8, 4))
            mask = (self.times_filtered >= anomaly["start_time"]) & (self.times_filtered <= anomaly["end_time"])
            plt.plot(self.times_filtered[mask], self.values_filtered[mask], label=f'{self.sensor_name}', color='red')
            plt.xlabel("Время")
            plt.ylabel("Температура (К)")
            plt.title(f"Аномалия {i+1}: Амплитуда = {anomaly['amplitude']:.1f} К, Пик = {anomaly['peak_value']:.1f} К")
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %H:%M'))
            plt.xticks(rotation=45)
            plt.grid()
            plt.tight_layout()
            plt.savefig(f"{output_dir}/anomaly_{i+1}.png")
            plt.show()

        for i, transition in enumerate(transitions):
            plt.figure(figsize=(8, 4))
            mask = (self.times_filtered >= transition["start_time"]) & (self.times_filtered <= transition["end_time"])
            plt.plot(self.times_filtered[mask], self.values_filtered[mask], label=f'{self.sensor_name}', color='orange' if transition["type"] == "отогрев" else 'blue')
            plt.xlabel("Время")
            plt.ylabel("Температура (К)")
            plt.title(f"{transition['type'].capitalize()} {i+1}: От {transition['start_value']:.1f} К до {transition['end_value']:.1f} К")
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %H:%M'))
            plt.xticks(rotation=45)
            plt.grid()
            plt.tight_layout()
            plt.savefig(f"{output_dir}/{transition['type']}_{i+1}.png")
            plt.show()

    def run(self):
        if not self.load_data():
            return False
        if not self.process_data():
            return False
        self.remove_glitches(threshold_delta=50, max_glitch_duration=10)
        anomalies = self.detect_general_anomalies(z_threshold=5, window_size=30)
        transitions = self.detect_transitions(target_values=[20, 80, 300], tolerance=10, min_duration=300)
        self.plot_anomalies(anomalies, transitions)
        return True

if __name__ == "__main__":
    detector = AnomalyDetector(
        folder_path=r"D:\Автоматизация\cMT-7232\datalog\T1-T24",
        column_index=0,
        sensor_name="T0",
        debug_mode=True
    )
    detector.run()
