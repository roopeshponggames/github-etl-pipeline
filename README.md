# GitHub ETL Pipeline for .pol Pool Data Files

Automated ETL pipeline that extracts pool data from `.pol` files in this repository, transforms the data with statistical analysis, and stores the results in the `Meta_data/` folder.

## 🔄 How It Works

1. **Trigger**: Pipeline runs automatically on every push to `main` branch
2. **Extract**: Scans all folders for `.pol` files (value-type pair format)
3. **Transform**: Parses content, calculates statistics by type, generates distributions
4. **Load**: Saves transformed data as JSON to `Meta_data/` folder (mirroring source structure)

## 📁 Project Structure

```
your-repo/
├── .github/
│   └── workflows/
│       └── etl_pipeline.yml    # GitHub Actions workflow
├── etl/
│   ├── __init__.py
│   ├── main.py                 # Pipeline orchestrator
│   ├── extract.py              # File discovery and reading
│   ├── transform.py            # Data transformation logic
│   └── load.py                 # Save to Meta_data
├── Meta_data/                  # OUTPUT: Transformed data (auto-generated)
│   ├── _pipeline_summary.json  # Latest run summary with aggregated stats
│   └── [folder]/               # Mirrors source structure
│       └── [file].json
├── pools/                      # Your source folders
│   └── Pool_0201_395.pol
├── requirements.txt
└── README.md
```

## 📄 .pol File Format

The pipeline expects `.pol` files with space-separated value-type pairs:

```
1800 TB2
900 TB3
515 TB2
715 TB2
3810 TB3
875 TF1
...
```

Where:
- **First column**: Numeric value (integer)
- **Second column**: Type code (e.g., TB1, TB2, TB3, TF1, TF2)

## 📊 Output Format

Each `.pol` file produces a JSON file with comprehensive statistics:

```json
{
  "metadata": {
    "source_file": "pools/Pool_0201_395.pol",
    "pool_type": "Pool",
    "pool_id": "0201",
    "pool_variant": "395",
    "processed_at": "2024-01-15T10:30:00+00:00"
  },
  "statistics": {
    "summary": {
      "total_records": 100000,
      "total_sum": 118744550,
      "min_value": 375,
      "max_value": 25000,
      "avg_value": 1187.45,
      "median_value": 850.0,
      "std_deviation": 858.95,
      "unique_types": 5
    },
    "type_distribution": {
      "TB3": 29550,
      "TF1": 27850,
      "TB2": 22750,
      "TB1": 19740,
      "TF2": 110
    },
    "type_statistics": {
      "TB1": {
        "count": 19740,
        "sum": 15926700,
        "min": 375,
        "max": 1150,
        "avg": 806.82,
        "percentage": 19.74
      }
    },
    "value_distribution": {
      "0-500": { "count": 5790, "percentage": 5.79 },
      "501-1000": { "count": 59900, "percentage": 59.9 },
      "1001-2000": { "count": 19400, "percentage": 19.4 }
    }
  },
  "sample_data": {
    "first_10": [...],
    "last_10": [...]
  }
}
```

## 🚀 Setup

1. **Copy the pipeline files** to your repository:
   - `.github/workflows/etl_pipeline.yml`
   - `etl/` folder
   - `requirements.txt`

2. **Push to main branch** - the pipeline will run automatically!

## ⚙️ Configuration

### Trigger Conditions

Edit `.github/workflows/etl_pipeline.yml` to customize:

```yaml
on:
  push:
    branches:
      - main           # Change branch if needed
    paths-ignore:
      - 'Meta_data/**' # Prevent infinite loops
      - '.github/**'
```

### Exclude Folders from Scanning

Edit `etl/extract.py`:

```python
EXCLUDED_DIRS = {
    '.git',
    '.github', 
    'Meta_data',
    '__pycache__',
    # Add more folders to exclude
}
```

## 🏃 Running Locally

```bash
# Install dependencies (minimal - uses only standard library)
pip install -r requirements.txt

# Run pipeline (process only changed files)
python etl/main.py

# Run pipeline (process ALL files)
python etl/main.py --all
```

## 🐛 Troubleshooting

**Pipeline not triggering?**
- Check that changes are on `main` branch
- Ensure changes are not in `paths-ignore` folders

**Infinite loop?**
- The commit message includes `[skip ci]` to prevent re-triggering
- `Meta_data/**` is in `paths-ignore`

**Permission errors?**
- Workflow has `contents: write` permission
- Uses `GITHUB_TOKEN` (auto-provided)

## 📝 Manual Trigger

You can manually trigger the pipeline from GitHub:
1. Go to **Actions** tab
2. Select **ETL Pipeline**
3. Click **Run workflow**
