# -*- coding: utf-8 -*-
import os
import glob
import sqlite3
import json
from datetime import datetime, timezone, timedelta
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from uuid import uuid4

class Analyzer:
    def __init__(self, folder_path, column_index, sensor_name, debug_mode=False):
        self.folder_path = folder_path
        self.column_index = column_index
        self.sensor_name = f"T{column_index}"
        self.debug_mode = debug_mode
        self.db_files = glob.glob(os.path.join(folder_path, "*.db"))
        self.base_output_path = folder_path
        self.anomalies_folder = os.path.join(self.base_output_path, "anomalies")
        self.glitches_folder = os.path.join(self.base_output_path, "glitches")
        self.warming_folder = os.path.join(self.base_output_path, "warming")
        self.global_plot_folder = os.path.join(self.base_output_path, "global_plot")
        self.report_name = f"temperature_report_{datetime.now().strftime('%Y-%m-%d')}.json"
        self.results = {
            "sensor": self.sensor_name,
            "report_name": self.report_name,
            "global_plot_file": None,
            "glitches": [],
            "anomalies": [],
            "anomalies_with": [],
            "warming": None,
            "anomaly_accuracy": {
                "num_detected": 0,
                "num_ground_truth": 0,
                "matches": [],
                "overall_accuracy": 0.0,
                "amplitude_average_error": {"absolute": 0.0, "relative_percent": 0.0},
                "start_time_average_error": {"absolute_minutes": 0.0, "relative_percent": 0.0},
                "end_time_average_error": {"absolute_minutes": 0.0, "relative_percent": 0.0},
                "average_error": 0.0
            }
        }
        self.ground_truth_anomalies = []
        self.raw_data = []
        self.times = None
        self.values = None
        self.times_filtered = None
        self.values_filtered = None
        self.values_filtered_no_anomalies = None
        self.sampling_interval = None
        self.avg_pre_warming = None
        self.sigma_pre_warming = None

    def create_folders(self):
        os.makedirs(self.anomalies_folder, exist_ok=True)
        os.makedirs(self.glitches_folder, exist_ok=True)
        os.makedirs(self.warming_folder, exist_ok=True)
        os.makedirs(self.global_plot_folder, exist_ok=True)

    def load_data(self):
        for db_file in self.db_files:
            if self.debug_mode:
                print(f"Обрабатываю файл: {db_file}")
            try:
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data';")
                if not cursor.fetchone():
                    if self.debug_mode:
                        print(f" В файле {db_file} нет таблицы data, пропускаю.")
                    conn.close()
                    continue

                sql_query = f"SELECT [time@timestamp], data_format_{self.column_index} FROM data"
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                if not rows:
                    if self.debug_mode:
                        print(f" Нет данных в таблице data файла {db_file}.")
                    conn.close()
                    continue

                self.raw_data.extend(rows)
                conn.close()
            except Exception as e:
                if self.debug_mode:
                    print(f" Ошибка при обработке файла {db_file}: {e}")

        if not self.raw_data:
            if self.debug_mode:
                print("Нет данных для анализа.")
            return False
        return True

    def process_data(self):
        processed_data = []
        for ts, val in self.raw_data:
            try:
                ts_float = float(ts)
                dt = datetime.fromtimestamp(ts_float, tz=timezone.utc)
                processed_data.append((dt, val))
            except Exception as e:
                if self.debug_mode:
                    print(f" Ошибка преобразования времени {ts}: {e}")

        if not processed_data:
            if self.debug_mode:
                print("Нет данных для анализа.")
            return False

        processed_data.sort(key=lambda x: x[0])
        self.times, self.values = zip(*processed_data)
        self.times = np.array(self.times)
        self.values = np.array(self.values)
        self.sampling_interval = (self.times[1] - self.times[0]).total_seconds() if len(self.times) > 1 else 3600
        if self.debug_mode:
            print(f"Sampling interval: {self.sampling_interval} seconds")
        return True

    def remove_glitches(self, threshold_delta=50, max_glitch_duration=10):
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
        self.values_filtered_no_anomalies = self.values_filtered.copy()

    def add_anomalies(self):
        self.ground_truth_anomalies = []
        self.calculate_stats()

        num_anomalies = 3
        min_anomaly_duration = 2 * 60
        max_anomaly_duration = 60 * 60

        warming_start_time, warming_end_time = self.detect_warming()
        if not warming_start_time:
            if self.debug_mode:
                print("❗ Не удалось определить начало отогрева. Аномалии не будут добавлены.")
            return

        anomaly_period_start = self.times_filtered[0]
        anomaly_period_end = warming_start_time

        np.random.seed(42)
        existing_intervals = []

        for i in range(num_anomalies):
            attempt = 0
            while attempt < 20:
                attempt += 1

                period_range = (anomaly_period_end - anomaly_period_start).total_seconds()
                if period_range < min_anomaly_duration:
                    if self.debug_mode:
                        print("❗ Недостаточно времени до отогрева для размещения аномалий.")
                    return

                random_start_seconds = np.random.uniform(0, period_range - min_anomaly_duration)
                random_start = anomaly_period_start + timedelta(seconds=random_start_seconds)
                duration = np.random.uniform(min_anomaly_duration, min(max_anomaly_duration, (anomaly_period_end - random_start).total_seconds()))
                random_end = random_start + timedelta(seconds=duration)

                if random_end >= warming_start_time:
                    continue

                intersects = any(
                    (random_start <= existing_end and random_end >= existing_start)
                    for existing_start, existing_end in existing_intervals
                )
                if intersects:
                    continue

                anomaly_mask = (self.times_filtered >= random_start) & (self.times_filtered <= random_end)
                if not np.any(anomaly_mask):
                    continue

                multiplier = np.random.uniform(1.0, 1.7)
                sigma = self.sigma_pre_warming if self.sigma_pre_warming else 1
                anomaly_amplitude = multiplier * 30 * sigma

                t = np.linspace(0, 1, np.sum(anomaly_mask))
                gaussian_shape = anomaly_amplitude * np.exp(-(t - 0.5)**2 / (2 * (0.2**2)))
                self.values_filtered[anomaly_mask] += gaussian_shape

                self.ground_truth_anomalies.append({
                    "start_time": random_start.strftime('%Y-%m-%d %H:%M:%S'),
                    "end_time": random_end.strftime('%Y-%m-%d %H:%M:%S'),
                    "amplitude": float(anomaly_amplitude)
                })
                existing_intervals.append((random_start, random_end))
                if self.debug_mode:
                    print(f"Добавлена аномалия {i+1}: с {random_start.strftime('%Y-%m-%d %H:%M:%S')} до {random_end.strftime('%Y-%m-%d %H:%M:%S')}, амплитуда {anomaly_amplitude:.2f} K")
                break
            else:
                if self.debug_mode:
                    print(f"⚠️ Не удалось разместить аномалию {i+1} после 20 попыток.")

    def calculate_stats(self):
        cutoff_time = datetime(2025, 4, 24, 23, 59, 59, tzinfo=timezone.utc)
        pre_warming_mask = self.times_filtered < cutoff_time
        pre_warming_values = self.values_filtered_no_anomalies[pre_warming_mask]
        if len(pre_warming_values) > 0:
            self.avg_pre_warming = np.mean(pre_warming_values)
            self.sigma_pre_warming = np.std(pre_warming_values)
            if self.debug_mode:
                print(f"Среднее значение температуры до 24.04.2025: {self.avg_pre_warming:.2f} K")
                print(f"Стандартное отклонение (сигма) до 24.04.2025: {self.sigma_pre_warming:.2f} K")
        else:
            self.avg_pre_warming = self.values_filtered_no_anomalies[0]
            self.sigma_pre_warming = 1.0
            if self.debug_mode:
                print("Недостаточно данных до 24.04.2025, использую первую точку.")

    def detect_warming(self, warm_threshold=290, sustain_threshold=290, hold_duration_minutes=30):
        hold_points = int((hold_duration_minutes * 60) // self.sampling_interval)
        if self.debug_mode:
            print(f"Hold points: {hold_points}")
        start_warming_idx = None
        for i in range(len(self.values_filtered) - hold_points):
            if self.values_filtered[i] >= warm_threshold:
                sustain_segment = self.values_filtered[i:i + hold_points]
                if np.mean(sustain_segment) >= sustain_threshold:
                    start_warming_idx = i
                    break

        warming_start_time = None
        warming_end_time = None
        if start_warming_idx is not None:
            tolerance = 0.1
            actual_start_idx = None
            for i in range(start_warming_idx, -1, -1):
                if abs(self.values_filtered[i] - self.avg_pre_warming) <= tolerance:
                    actual_start_idx = i
                    break
            if actual_start_idx is not None:
                warming_start_time = self.times_filtered[actual_start_idx]
                warming_end_time = self.times_filtered[start_warming_idx + hold_points - 1] if start_warming_idx + hold_points - 1 < len(self.times_filtered) else self.times_filtered[-1]
            if self.debug_mode:
                print(warming_start_time)
                print(warming_end_time)
        return warming_start_time, warming_end_time

    def detect_anomalies(self, warming_start_time, warming_end_time, min_anomaly_duration=2*60, jump_threshold_factor=5, return_threshold_factor=1, use_anomalies=False):
        min_anomaly_points = int(min_anomaly_duration / self.sampling_interval)
        jump_threshold = jump_threshold_factor * self.sigma_pre_warming
        return_threshold = return_threshold_factor * self.sigma_pre_warming
        values = self.values_filtered if use_anomalies else self.values_filtered_no_anomalies
        deviations = np.abs(values - self.avg_pre_warming)
        jump_points = np.where(deviations >= jump_threshold)[0]
        anomalies = []

        i = 0
        while i < len(jump_points):
            start_jump = jump_points[i]
            j = i
            while j < len(jump_points) - 1 and jump_points[j + 1] == jump_points[j] + 1:
                j += 1
            segment_end = jump_points[j] if j < len(jump_points) else len(values) - 1
            peak_idx = start_jump + np.argmax(values[start_jump:segment_end + 1])
            
            start_idx = start_jump
            for k in range(peak_idx - 1, -1, -1):
                if abs(values[k] - self.avg_pre_warming) <= return_threshold:
                    start_idx = k
                    break
            
            end_idx = peak_idx
            for k in range(peak_idx + 1, len(values)):
                if abs(values[k] - self.avg_pre_warming) <= return_threshold:
                    end_idx = k
                    break
            
            duration_points = end_idx - start_idx
            anomaly_start_time = self.times_filtered[start_idx]
            anomaly_end_time = self.times_filtered[end_idx]
            
            if warming_start_time and warming_end_time:
                if not (anomaly_end_time >= warming_start_time and anomaly_start_time <= warming_end_time):
                    if duration_points >= min_anomaly_points:
                        amplitude = values[peak_idx] - self.avg_pre_warming
                        anomaly_info = {
                            "start_time": anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                            "end_time": anomaly_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                            "amplitude": float(amplitude)
                        }
                        if not use_anomalies:
                            anomaly_mask = (self.times_filtered >= anomaly_start_time - timedelta(minutes=5)) & (self.times_filtered <= anomaly_end_time + timedelta(minutes=5))
                            times_anomaly = self.times_filtered[anomaly_mask]
                            values_anomaly = values[anomaly_mask]
                            fig, ax = plt.subplots(figsize=(8, 4))
                            ax.plot(times_anomaly, values_anomaly, label=f'{self.sensor_name}', color='blue')
                            ax.axvspan(anomaly_start_time, anomaly_end_time, color='yellow', alpha=0.3)
                            ax.set_xlabel("Время")
                            ax.set_ylabel("Температура (К)")
                            ax.set_title(f"Аномалия датчика {self.sensor_name} с {anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
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
                            anomaly_filename = f"anomaly_{len(anomalies)}_{anomaly_start_time.strftime('%Y%m%d_%H%M%S')}.png"
                            anomaly_filepath = os.path.join(self.anomalies_folder, anomaly_filename)
                            plt.savefig(anomaly_filepath)
                            plt.close()
                            anomaly_info["plot_file"] = anomaly_filepath
                            self.results["anomalies"].append(anomaly_info)
                        else:
                            self.results["anomalies_with"].append(anomaly_info)
                        anomalies.append((start_idx, end_idx))
                        if self.debug_mode:
                            print(f"Обнаружена аномалия: с {anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S')} до {anomaly_end_time.strftime('%Y-%m-%d %H:%M:%S')}, амплитуда {amplitude:.2f} K")
            elif duration_points >= min_anomaly_points:
                amplitude = values[peak_idx] - self.avg_pre_warming
                anomaly_info = {
                    "start_time": anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "end_time": anomaly_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "amplitude": float(amplitude)
                }
                if not use_anomalies:
                    anomaly_mask = (self.times_filtered >= anomaly_start_time - timedelta(minutes=5)) & (self.times_filtered <= anomaly_end_time + timedelta(minutes=5))
                    times_anomaly = self.times_filtered[anomaly_mask]
                    values_anomaly = values[anomaly_mask]
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.plot(times_anomaly, values_anomaly, label=f'{self.sensor_name}', color='blue')
                    ax.axvspan(anomaly_start_time, anomaly_end_time, color='yellow', alpha=0.3)
                    ax.set_xlabel("Время")
                    ax.set_ylabel("Температура (К)")
                    ax.set_title(f"Аномалия датчика {self.sensor_name} с {anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
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
                    anomaly_filename = f"anomaly_{len(anomalies)}_{anomaly_start_time.strftime('%Y%m%d_%H%M%S')}.png"
                    anomaly_filepath = os.path.join(self.anomalies_folder, anomaly_filename)
                    plt.savefig(anomaly_filepath)
                    plt.close()
                    anomaly_info["plot_file"] = anomaly_filepath
                    self.results["anomalies"].append(anomaly_info)
                else:
                    self.results["anomalies_with"].append(anomaly_info)
                anomalies.append((start_idx, end_idx))
                if self.debug_mode:
                    print(f"Обнаружена аномалия: с {anomaly_start_time.strftime('%Y-%m-%d %H:%M:%S')} до {anomaly_end_time.strftime('%Y-%m-%d %H:%M:%S')}, амплитуда {amplitude:.2f} K")
            
            i = j + 1
        return anomalies

    def evaluate_anomaly_accuracy(self, ground_truth_anomalies):
        detected_anomalies = self.results["anomalies_with"]
        num_detected = len(detected_anomalies)
        num_ground_truth = len(ground_truth_anomalies)
        
        if self.debug_mode:
            print(f"Обнаружено аномалий: {num_detected}")
            print(f"Истинное количество аномалий: {num_ground_truth}")
        
        self.results["anomaly_accuracy"]["num_detected"] = num_detected
        self.results["anomaly_accuracy"]["num_ground_truth"] = num_ground_truth
        
        matches = 0
        amplitude_errors = []
        start_time_errors = []
        end_time_errors = []
        
        for gt_anomaly in ground_truth_anomalies:
            gt_start = datetime.strptime(gt_anomaly["start_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            gt_end = datetime.strptime(gt_anomaly["end_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            gt_amplitude = gt_anomaly["amplitude"]
            
            closest_anomaly = None
            min_time_diff = float('inf')
            
            for det_anomaly in detected_anomalies:
                det_start = datetime.strptime(det_anomaly["start_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                time_diff = abs((det_start - gt_start).total_seconds())
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_anomaly = det_anomaly
            
            if closest_anomaly and min_time_diff < 1200:
                det_start = datetime.strptime(closest_anomaly["start_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                det_end = datetime.strptime(closest_anomaly["end_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                det_amplitude = closest_anomaly["amplitude"]
                
                start_diff = abs((det_start - gt_start).total_seconds()) / 60.0
                end_diff = abs((det_end - gt_end).total_seconds()) / 60.0
                amplitude_diff = abs(det_amplitude - gt_amplitude)
                amplitude_relative_error = (amplitude_diff / gt_amplitude) * 100 if gt_amplitude != 0 else 0
                
                match_info = {
                    "ground_truth_start": gt_anomaly["start_time"],
                    "detected_start": closest_anomaly["start_time"],
                    "start_diff_minutes": start_diff,
                    "ground_truth_end": gt_anomaly["end_time"],
                    "detected_end": closest_anomaly["end_time"],
                    "end_diff_minutes": end_diff,
                    "ground_truth_amplitude": gt_amplitude,
                    "detected_amplitude": det_amplitude,
                    "amplitude_absolute_error": amplitude_diff,
                    "amplitude_relative_error": amplitude_relative_error
                }
                self.results["anomaly_accuracy"]["matches"].append(match_info)
                amplitude_errors.append(amplitude_diff)
                start_time_errors.append(start_diff)
                end_time_errors.append(end_diff)
                matches += 1
                if self.debug_mode:
                    print(f"\nСоответствие для аномалии:")
                    print(f"Истинное начало: {gt_anomaly['start_time']}, Обнаруженное начало: {closest_anomaly['start_time']}, Разница: {start_diff:.2f} минут")
                    print(f"Истинный конец: {gt_anomaly['end_time']}, Обнаруженный конец: {closest_anomaly['end_time']}, Разница: {end_diff:.2f} минут")
                    print(f"Истинная амплитуда: {gt_amplitude:.2f} K, Обнаруженная амплитуда: {det_amplitude:.2f} K")
                    print(f"Абсолютная ошибка амплитуды: {amplitude_diff:.2f} K, Относительная ошибка: {amplitude_relative_error:.2f}%")
            else:
                if self.debug_mode:
                    print(f"\nАномалия не обнаружена: Истинное начало: {gt_anomaly['start_time']}, Истинная амплитуда: {gt_amplitude:.2f} K")
        
        self.results["anomaly_accuracy"]["overall_accuracy"] = matches / num_ground_truth if num_ground_truth > 0 else 0
        
        if matches > 0:
            self.results["anomaly_accuracy"]["amplitude_average_error"]["absolute"] = float(np.mean(amplitude_errors) if amplitude_errors else 0)
            self.results["anomaly_accuracy"]["amplitude_average_error"]["relative_percent"] = float(np.mean([e / gt_amplitude * 100 for e in amplitude_errors]) if amplitude_errors else 0)
            self.results["anomaly_accuracy"]["start_time_average_error"]["absolute_minutes"] = float(np.mean(start_time_errors) if start_time_errors else 0)
            self.results["anomaly_accuracy"]["start_time_average_error"]["relative_percent"] = float(np.mean([e / 60 * 100 for e in start_time_errors]) if start_time_errors else 0)
            self.results["anomaly_accuracy"]["end_time_average_error"]["absolute_minutes"] = float(np.mean(end_time_errors) if end_time_errors else 0)
            self.results["anomaly_accuracy"]["end_time_average_error"]["relative_percent"] = float(np.mean([e / 60 * 100 for e in end_time_errors]) if end_time_errors else 0)
            avg_error = (self.results["anomaly_accuracy"]["amplitude_average_error"]["relative_percent"] +
                        self.results["anomaly_accuracy"]["start_time_average_error"]["relative_percent"] +
                        self.results["anomaly_accuracy"]["end_time_average_error"]["relative_percent"]) / 3
            self.results["anomaly_accuracy"]["average_error"] = float(avg_error)
            if self.debug_mode:
                print(f"Средняя ошибка амплитуды: {self.results['anomaly_accuracy']['amplitude_average_error']['absolute']:.2f} K ({self.results['anomaly_accuracy']['amplitude_average_error']['relative_percent']:.2f}%)")
                print(f"Средняя ошибка времени начала: {self.results['anomaly_accuracy']['start_time_average_error']['absolute_minutes']:.3f} минут ({self.results['anomaly_accuracy']['start_time_average_error']['relative_percent']:.2f}%)")
                print(f"Средняя ошибка времени конца: {self.results['anomaly_accuracy']['end_time_average_error']['absolute_minutes']:.3f} минут ({self.results['anomaly_accuracy']['end_time_average_error']['relative_percent']:.2f}%)")
                print(f"Общая средняя ошибка: {avg_error:.2f}%")
        
        if self.debug_mode:
            print(f"Общая точность обнаружения аномалий: {self.results['anomaly_accuracy']['overall_accuracy']:.2%}")

    def find_closest_index(self, times, target_time, tolerance_seconds=5):
        time_diffs = np.array([(t - target_time).total_seconds() for t in times])
        abs_diffs = np.abs(time_diffs)
        closest_idx = np.argmin(abs_diffs)
        if self.debug_mode:
            print(f"Отладка: find_closest_index для {target_time}, ближайшее время {times[closest_idx]}, разница {abs_diffs[closest_idx]:.2f} секунд")
        if abs_diffs[closest_idx] <= tolerance_seconds:
            return closest_idx
        else:
            if self.debug_mode:
                print(f"Отладка: Разница времени {abs_diffs[closest_idx]:.2f} секунд превышает tolerance_seconds={tolerance_seconds}")
            return closest_idx

    def plot_results(self, warming_start_time, warming_end_time):
        if warming_end_time is not None and warming_end_time >= warming_start_time:
            end_time = warming_end_time
        else:
            end_time = self.times_filtered[-1]
        if self.debug_mode:
            print(f"Отладка: Установлен end_time={end_time}")

        if end_time < self.times_filtered[0]:
            if self.debug_mode:
                print(f"Ошибка: end_time={end_time} раньше начала данных ({self.times_filtered[0]}). Устанавливаю end_time на последнюю точку данных.")
            end_time = self.times_filtered[-1]
        elif warming_start_time is not None and end_time < warming_start_time:
            if self.debug_mode:
                print(f"Предупреждение: end_time={end_time} раньше warming_start_time ({warming_start_time}). Устанавливаю end_time на warming_start_time + 1 день.")
            end_time = warming_start_time + timedelta(days=1)

        mask_plot = self.times_filtered <= end_time
        times_plot = self.times_filtered[mask_plot]
        values_plot_no_anomalies = self.values_filtered_no_anomalies[mask_plot]

        if self.debug_mode:
            print(f"Диапазон времени в данных: от {self.times_filtered[0]} до {self.times_filtered[-1]}")
            print(f"Диапазон графика: от {times_plot[0]} до {times_plot[-1]}")

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.plot(times_plot, values_plot_no_anomalies, label=f'{self.sensor_name} (без аномалий)', color='blue')
        y_min2, y_max2 = np.min(values_plot_no_anomalies), np.max(values_plot_no_anomalies)
        y_range2 = y_max2 - y_min2
        y_start2 = y_max2 - (y_range2 * 0.1)
        text_offset2 = y_range2 * 0.04

        anomalies_no = []
        for anomaly in self.results["anomalies"]:
            start_time = datetime.strptime(anomaly["start_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            end_time_anomaly = datetime.strptime(anomaly["end_time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            start_idx = self.find_closest_index(self.times_filtered, start_time)
            end_idx = self.find_closest_index(self.times_filtered, end_time_anomaly)
            if start_idx is not None and end_idx is not None and self.times_filtered[start_idx] <= end_time:
                anomalies_no.append((start_idx, end_idx))
                if self.debug_mode:
                    print(f"Добавлена аномалия для графика: начало {anomaly['start_time']}, конец {anomaly['end_time']}")
            else:
                if self.debug_mode:
                    print(f"Аномалия не добавлена: начало {anomaly['start_time']}, конец {anomaly['end_time']} - вне диапазона или не найдены индексы")

        peak_info_no = []
        for start_idx, end_idx in anomalies_no:
            if self.times_filtered[start_idx] <= end_time and self.times_filtered[end_idx] <= end_time:
                peak_idx = start_idx + np.argmax(self.values_filtered_no_anomalies[start_idx:end_idx+1])
                peak_time = self.times_filtered[peak_idx]
                peak_value = self.values_filtered_no_anomalies[peak_idx]
                start_time = self.times_filtered[start_idx]
                end_time_anomaly = self.times_filtered[end_idx]
                amplitude = peak_value - self.avg_pre_warming
                peak_info_no.append({
                    "peak_time": peak_time,
                    "peak_value": peak_value,
                    "start_time": start_time,
                    "end_time": end_time_anomaly,
                    "amplitude": amplitude
                })

        peak_info_no.sort(key=lambda x: x["peak_time"])

        current_y2 = y_start2
        previous_peak_time2 = None
        for idx, info in enumerate(peak_info_no):
            peak_time = info["peak_time"]
            peak_value = info["peak_value"]
            start_time = info["start_time"]
            end_time_anomaly = info["end_time"]
            amplitude = info["amplitude"]

            if previous_peak_time2 is not None:
                time_diff = (peak_time - previous_peak_time2).total_seconds() / 3600
                if time_diff < 2:
                    current_y2 -= (3 * text_offset2)

            ax.axvspan(start_time, end_time_anomaly, color='yellow', alpha=0.3)
            ax.text(
                peak_time,
                current_y2,
                f"Начало: {start_time.strftime('%d %b %H:%M')}",
                fontsize=10,
                ha='center',
                va='bottom'
            )
            ax.text(
                peak_time,
                current_y2 - text_offset2,
                f"Амплитуда: {amplitude:.2f} K",
                fontsize=10,
                ha='center',
                va='bottom'
            )
            ax.text(
                peak_time,
                current_y2 - (2 * text_offset2),
                f"Конец: {end_time_anomaly.strftime('%d %b %H:%M')}",
                fontsize=10,
                ha='center',
                va='bottom'
            )
            previous_peak_time2 = peak_time
            current_y2 -= (4 * text_offset2)

        if warming_start_time is not None and warming_start_time <= end_time:
            if self.debug_mode:
                print(f"Отладка: Попытка аннотации отогрева с warming_start_time={warming_start_time}")
            time_diffs = np.array([(t - warming_start_time).total_seconds() for t in times_plot])
            abs_diffs = np.abs(time_diffs)
            actual_start_idx = np.argmin(abs_diffs)
            time_diff = abs_diffs[actual_start_idx]
            if self.debug_mode:
                print(f"Отладка: Найден ближайший индекс {actual_start_idx}, время {times_plot[actual_start_idx]}, разница {time_diff:.2f} секунд")

            warm_time = times_plot[actual_start_idx]
            warm_value = values_plot_no_anomalies[actual_start_idx]
            warm_value = max(min(warm_value, y_max2), y_min2)

            warm_mask = (self.times_filtered >= warm_time - timedelta(minutes=30)) & (self.times_filtered <= warm_time + timedelta(minutes=30))
            times_warm = self.times_filtered[warm_mask]
            values_warm = self.values_filtered[warm_mask]
            fig_warm, ax_warm = plt.subplots(figsize=(8, 4))
            ax_warm.plot(times_warm, values_warm, label=f'{self.sensor_name}', color='blue')
            ax_warm.axvline(warm_time, color='green', linestyle='--')
            ax_warm.set_xlabel("Время")
            ax_warm.set_ylabel("Температура (К)")
            ax_warm.set_title(f"Отогрев датчика {self.sensor_name} с {warm_time.strftime('%Y-%m-%d %H:%M:%S')}")
            ax_warm.grid(True)
            ax_warm.legend(loc='upper right')
            locator = mdates.AutoDateLocator()
            formatter = mdates.AutoDateFormatter(locator)
            formatter.scaled[1/24] = '%H:%M'
            formatter.scaled[1] = '%d %b %H:%M'
            ax_warm.xaxis.set_major_locator(locator)
            ax_warm.xaxis.set_major_formatter(formatter)
            plt.xticks(rotation=45)
            plt.tight_layout()
            warm_filename = f"warming_{warm_time.strftime('%Y%m%d_%H%M%S')}.png"
            warm_filepath = os.path.join(self.warming_folder, warm_filename)
            plt.savefig(warm_filepath)
            plt.close(fig_warm)
            self.results["warming"] = {
                "start_time": warm_time.strftime('%Y-%m-%d %H:%M:%S'),
                "type": "confirmed",
                "plot_file": warm_filepath
            }

            x_offset = timedelta(minutes=10)
            y_offset = y_range2 * 0.05
            if self.debug_mode:
                print(f"Отладка: warm_time={warm_time}, warm_value={warm_value}, y_min2={y_min2}, y_max2={y_max2}")
            ax.axvline(warm_time, color='green', linestyle='--', alpha=0.5)
            ax.annotate(
                f"Отогрев:\n{warm_time.strftime('%d %b %H:%M')}",
                xy=(warm_time, warm_value),
                xytext=(warm_time + x_offset, warm_value + y_offset),
                arrowprops=dict(arrowstyle='->', color='green'),
                color='green',
                fontsize=12,
                ha='left',
                va='bottom'
            )
            if self.debug_mode:
                print(f"Начало отогрева обнаружено: {warm_time}. График сохранен: {warm_filepath}")
        else:
            if self.debug_mode:
                print(f"Отладка: Отогрев вне диапазона графика или не определен: warming_start_time={warming_start_time}, end_time={end_time}")
            for i in range(len(values_plot_no_anomalies)):
                if values_plot_no_anomalies[i] >= 290:
                    warm_time = times_plot[i]
                    warm_value = values_plot_no_anomalies[i]
                    warm_value = max(min(warm_value, y_max2), y_min2)
                    warm_mask = (self.times_filtered >= warm_time - timedelta(minutes=30)) & (self.times_filtered <= warm_time + timedelta(minutes=30))
                    times_warm = self.times_filtered[warm_mask]
                    values_warm = self.values_filtered[warm_mask]
                    fig_warm, ax_warm = plt.subplots(figsize=(8, 4))
                    ax_warm.plot(times_warm, values_warm, label=f'{self.sensor_name}', color='blue')
                    ax_warm.axvline(warm_time, color='orange', linestyle='--')
                    ax_warm.set_xlabel("Время")
                    ax_warm.set_ylabel("Температура (К)")
                    ax_warm.set_title(f"Возможный отогрев датчика {self.sensor_name} с {warm_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    ax_warm.grid(True)
                    ax_warm.legend(loc='upper right')
                    locator = mdates.AutoDateLocator()
                    formatter = mdates.AutoDateFormatter(locator)
                    formatter.scaled[1/24] = '%H:%M'
                    formatter.scaled[1] = '%d %b %H:%M'
                    ax_warm.xaxis.set_major_locator(locator)
                    ax_warm.xaxis.set_major_formatter(formatter)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    warm_filename = f"warming_{warm_time.strftime('%Y%m%d_%H%M%S')}.png"
                    warm_filepath = os.path.join(self.warming_folder, warm_filename)
                    plt.savefig(warm_filepath)
                    plt.close(fig_warm)
                    self.results["warming"] = {
                        "start_time": warm_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "type": "possible",
                        "plot_file": warm_filepath
                    }
                    x_offset = timedelta(minutes=10)
                    y_offset = y_range2
                    if self.debug_mode:
                        print(f"Отладка: warm_time={warm_time}, warm_value={warm_value}, y_min2={y_min2}, y_max2={y_max2}")
                    ax.axvline(warm_time, color='orange', linestyle='--', alpha=0.5)
                    ax.annotate(
                        f"Возможный отогрев:\n{warm_time.strftime('%d %b %H:%M')}",
                        xy=(warm_time, warm_value),
                        xytext=(warm_time + x_offset, warm_value + y_offset),
                        arrowprops=dict(arrowstyle='->', color='orange'),
                        color='orange',
                        fontsize=12,
                        ha='left',
                        va='bottom'
                    )
                    if self.debug_mode:
                        print(f"Возможное начало отогрева: {warm_time}. График сохранен: {warm_filepath}")
                    break

        start_date = self.times_filtered[0].strftime('%Y-%d.%m')
        end_date = warming_start_time.strftime('%Y-%d-%m') if warming_start_time and warming_start_time <= end_time else times_plot[-1].strftime('%Y-%d-%m')
        ax.set_title(f"Температурные данные термопары {self.sensor_name} с {start_date} до {end_date} (без аномалий)")
        ax.set_xlabel("Время")
        ax.set_ylabel("Температура (К)")
        ax.grid(True)
        ax.legend(loc='upper right')
        locator = mdates.AutoDateLocator()
        formatter = mdates.AutoDateFormatter(locator)
        formatter.scaled[1/24] = '%H:%M'
        formatter.scaled[1] = '%d %b %H:%M'
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

        plt.xticks(rotation=45)
        plt.tight_layout()
        if self.debug_mode:
            global_plot_filename = f"global_plot_{self.sensor_name}_{self.times_filtered[0].strftime('%Y%m%d')}_{times_plot[-1].strftime('%Y%m%d')}.png"
            global_plot_filepath = os.path.join(self.global_plot_folder, global_plot_filename)
            self.results["global_plot_file"] = global_plot_filepath
            fig.savefig(global_plot_filepath)
            plt.show()
            print(f"График сохранен: {global_plot_filepath}")

        else:
            global_plot_filename = f"global_plot_{self.sensor_name}_{self.times_filtered[0].strftime('%Y%m%d')}_{times_plot[-1].strftime('%Y%m%d')}.png"
            global_plot_filepath = os.path.join(self.global_plot_folder, global_plot_filename)
            self.results["global_plot_file"] = global_plot_filepath
            fig.savefig(global_plot_filepath)
            plt.close(fig)
            if self.debug_mode:
                print(f"График сохранен: {global_plot_filepath}")

    def save_results(self):
        json_filepath = os.path.join(self.base_output_path, self.report_name)
        with open(json_filepath, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4)
        if self.debug_mode:
            print(f"Результаты сохранены в {json_filepath}")

    def run(self):
        self.create_folders()
        if not self.load_data():
            return
        if not self.process_data():
            return
        self.remove_glitches()
        
        self.results["anomalies_with"] = []
        self.add_anomalies()
        warming_start_time, warming_end_time = self.detect_warming()
        self.detect_anomalies(warming_start_time, warming_end_time, use_anomalies=True)
        self.evaluate_anomaly_accuracy(self.ground_truth_anomalies)
        
        self.results["anomalies"] = []
        warming_start_time, warming_end_time = self.detect_warming()
        anomalies = self.detect_anomalies(warming_start_time, warming_end_time, use_anomalies=False)
        if self.debug_mode:
            print(f"Найдено аномалий (без искусственных): {len(anomalies)}")
        
        self.plot_results(warming_start_time, warming_end_time)
        self.save_results()

if __name__ == "__main__":
    folder_path = r"C:\Users\Иван Литвак\Desktop\Автоматизация\cMT-7232\cMT-7232\datalog\T1-T24"
    analyzer = Analyzer(folder_path, column_index=0, sensor_name="T0", debug_mode=True)
    analyzer.run()
