# 📦 MSSQL to Parquet Exporter

This Python script exports millions of records from a Microsoft SQL Server database table to Parquet files, chunk by chunk, and uploads them to an S3 bucket.  
It supports **restartable syncs**, so you can resume where it left off in case of failures.

---

## ⚙️ Prerequisites

- Python 3.12 (Anaconda recommended on macOS for `pyodbc`)
- `msodbcsql17` driver installed and registered
- AWS CLI profile already configured (`aws configure --profile <your-profile>`)

---

## 📂 Project Structure

```
.
├── mssql2parquet.py
├── checkpoint.json   # auto-created to track last exported row
└── README.md
```

---

## 🔑 Required Environment Variables

| Variable           | Description                                                    |
| ------------------ | -------------------------------------------------------------- |
| `MSSQL_USERNAME`   | Your MSSQL database username (e.g. `krish`)                    |
| `MSSQL_PASSWORD`   | Your MSSQL database password                                   |
| `MSSQL_SERVERNAME` | Your MSSQL server IP or name                                   |
| `MSSQL_DBNAME`     | Your MSSQL server database name                                |
| `TABLE_NAME  `     | Your MSSQL server database table name to be export             |
| `AWS_PROFILE`      | The name of your AWS credentials profile (default: `personal`) |

---

## ✅ How to set up your environment

### 1️⃣ **Export your MSSQL credentials**

```bash
export MSSQL_USERNAME="krish"
export MSSQL_PASSWORD="your_strong_password"
export MSSQL_SERVERNAME="servername"
export MSSQL_DBNAME="dbname"
export TABLE_NAME="tablename"
export AWS_PROFILE="personal"
```

---

## 🟢 **Run the script**

```bash
# Use your Anaconda Python if installed
python mssql2parquet.py
```

✅ The script:

- Loads your credentials from environment variables.
- Connects to your MSSQL table.
- Writes each 1000-row chunk to a Parquet file.
- Uploads each file to your S3 bucket under your configured prefix.
- Updates the `checkpoint.json` after each chunk, so it can resume on failure.

---

## 🔒 **Best Practices**

- **Never commit your password** to the script or version control.
- Use `~/.aws/credentials` to manage AWS profiles securely.
- For production, consider storing secrets in a secure vault (e.g., AWS SSM Parameter Store or Secrets Manager).

---

## 🗑️ **To clear progress**

To restart from scratch, delete the `checkpoint.json`:

```bash
rm checkpoint.json
```
