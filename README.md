# GCPDE - Google Cloud Platform Data Engineering
A Python library that provides an opinionated interface for working with Google Cloud Platform (GCP) services from a data engineering perspective.

## Features
- typing for GCS and BigQuery API
- standardized auth using service account credentials

### Google Cloud Storage (GCS)
- upload and download files with retry logic and async capabilities
- list files with prefix filtering
- copy files between buckets
- work with datasets using standardized file path conventions
- partition data by date/time automatically

### BigQuery
- create, delete, and check tables
- generate schemas automatically from data
- insert records with automatic chunking for large datasets
- run queries and commands with timeout handling
- replace tables with no-downtime strategy
- convert query results to pandas DataFrames

## Installation
```bash
pip install "git+https://github.com/wandercom/gcpde.git"
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.
