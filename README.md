# Translation runner

# Translation Runner

This project provides a robust translation management system with AWS S3 integration, bulk translation jobs, and a web dashboard for file operations. It supports automated translation workflows using AWS Translate and custom dictionary management.

## Features

- **Translation Runner**: Automates bulk translation of i18n modules using AWS Translate.
- **AWS S3 Integration**: Upload, download, copy, and delete files and buckets via REST API and dashboard.
- **Custom Dictionary**: Manage custom translation dictionaries and import to AWS Translate.
- **Logging**: Structured logging with retention policies and critical event notifications.
- **Frontend Dashboard**: Web UI for S3 operations and translation job monitoring.

## Project Structure

## Getting Started

### Prerequisites

- Node.js v16+
- AWS credentials with S3 and Translate permissions
- Configure `.env` with your AWS settings:

### Install Dependencies

```sh
npm install

npm start

Or use the dashboard to trigger jobs and manage files.

API Endpoints
POST /s3/bucket — Create bucket
GET /s3/buckets — List buckets
GET /s3/bucket/exists — Check bucket existence
DELETE /s3/bucket — Delete bucket
POST /s3/upload — Upload file
GET /s3/file/exists — Check file existence
DELETE /s3/file — Delete file
GET /s3/files — List files in bucket
POST /s3/file/copy — Copy file
GET /s3/file — Download file
GET /s3/presign — Get presigned URL
See routes/routes.js for full API details.

Utilities
SafeUtils: Input sanitization, URL handling, query parsing.
DateTime: Date/time parsing, formatting, relative time.
ErrorHandler: Centralized error tracking.
UtilityLogger: Structured logging with retention and critical event support.
ScyllaDb: DynamoDB-compatible client for ScyllaDB (optional).
Customization
Add new target locales in LOCALES.
Configure log routes in configs/LogRoutes.js.
Extend translation logic in service/TranslationRunner.js.
License
MIT

```
