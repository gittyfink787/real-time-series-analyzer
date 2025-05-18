# Real-Time Time Analyzer (Independent Project)

A modular and extensible Python system for ingesting, validating, and analyzing time-series data in real-time. Designed and implemented independently to demonstrate strong architectural design, advanced data handling, and multi-threaded processing.

---

## 📌 Motivation

This project was born out of a personal challenge to build a production-style streaming data pipeline from scratch without relying on external platforms like Kafka or Spark. The goal was to simulate real-world ingestion and analysis of time-series data using flexible Python components, while practicing principles of software design, concurrency, and data integrity.

---

## 🧱 Architecture Overview

* **FileProcessor (Abstract Base Class):** Unified interface for various file formats (Excel, Parquet) with support for polymorphism.
* **ExcelProcessor & ParquetProcessor:** Concrete implementations for reading data by format.
* **CheckData:** Validates datasets for timestamp format, duplicates, and outliers.
* **CalAverage (Abstract Base Class):** General interface for average calculation strategies.

  * **AvgDividedData:** Batch processing by splitting input into daily CSV files.
  * **AvgDataStream:** Real-time analysis using background threads and hourly scheduling.

The system cleanly separates concerns using OOP principles, supporting flexible input formats, reusable logic, and two independent average-calculation strategies.

---

## ✨ Features

* Real-time ingestion of Excel or Parquet time-series data
* Validates timestamps against strict datetime formats
* Detects and reports duplicate rows and numeric outliers (via 3σ rule)
* Multi-threaded buffering system using `threading.Lock` for data consistency
* Scheduled hourly average computation with live streaming updates
* Outputs average data into a text file with timestamps
* Modular, easily extendable to new file formats or aggregation strategies

---

## ⚙️ How It Works

1. **Startup**: User initializes `AvgDataStream` with a file processor.
2. **Thread 1 - File Watcher**: Monitors file size; when it grows, reads new lines and appends them to a buffer.
3. **Thread 2 - Scheduler**: Every hour, computes and logs averages of new values.
4. **Data Validations**: Can be optionally triggered using the `CheckData` class.
5. **Alternative Batch Mode**: `AvgDividedData` provides per-day file splitting and average calculation.

---

## 🧰 Technologies Used

* **Python 3**
* **Pandas & NumPy** – for efficient data wrangling
* **Threading** – for concurrent data processing
* **Schedule** – for timed task execution
* **ABC (Abstract Base Classes)** – for enforcing clean design
* **OS / File I/O** – for monitoring and streaming

---

## 🚀 Setup Instructions

1. Install dependencies:

```bash
pip install pandas numpy schedule
```

2. Place your time-series Excel or Parquet file in the working directory.
3. Update the file path in `__main__` section.
4. Run the script:

```bash
python real_time_analyzer.py
```

5. View output in `hourly_update.txt`

---

## 📷 Demo Output

```
Analysis at 2025-05-19 01:00  :Total average:  46.92, Last hour average: 45.67
No data in the last hour at 2025-05-19 02:00
```

---

## 📚 What I Learned

* Designing thread-safe systems using locks and concurrent logic
* Using abstract classes to enforce structure in a growing codebase
* Handling real-world data inconsistencies like nulls, malformed timestamps, and outliers
* Writing flexible and maintainable OOP Python code

---

## 📈 Future Improvements

* CLI for triggering data validation or switching modes
* Support for live plotting of time-series trends
* Configurable threshold rules for outlier detection
* Plugin architecture for custom aggregations

---

## 🧠 Acknowledgements

This is a **completely independent project**, built outside any course or employment. Every aspect—from idea to architecture to implementation—was created and executed solo to sharpen real-world engineering skills.

---

Feel free to explore the code, raise issues, or suggest enhancements!
