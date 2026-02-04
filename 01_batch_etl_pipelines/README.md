# Batch ETL Pipelines – PySpark

## Overview
This module demonstrates production-grade batch ETL pipelines built using PySpark.
The pipelines follow a bronze → silver → gold architecture and are designed for
scalability, performance, and analytics consumption.

## Tech Stack
- PySpark
- Databricks
- Delta Lake
- AWS S3, Athena

## Architecture
Source Systems → Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)

## Key Engineering Principles
- Explicit schema definitions
- Idempotent pipeline design
- Partitioned outputs for query optimization
- Reusable and modular code structure

## Use Case
Transform raw source data into analytics-ready fact and dimension tables
consumed by BI tools and reporting systems.
