# 📦 Real Estate Distributed Data Crawler

This project is a distributed data collection system built to assist with real-world real estate data gathering and processing. 

It leverages a combination of technologies and services to reliably extract, organize, and manage large-scale real estate data.

## 🚀 Key Components

1. Data Crawling Engine:

Utilizes Scrapy, Redis, Selenium, and AdsPower to perform robust data extraction from target property listing platforms.

2. Rule Generation:

Custom rule generators are used to define how and where to crawl specific types of data, supporting dynamic and complex site structures.

3. Rule-Based Data Scheduling:

Parsed rules are used to identify valid data targets and push them into Redis for distributed and fault-tolerant processing.
