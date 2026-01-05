"""
Main ETL Pipeline Orchestrator
Extracts data from .pol files, transforms it, and saves to Meta_data folder.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

from extract import extract_all_pol_files, get_changed_pol_files
from transform import transform_pol_data, generate_aggregated_summary
from load import save_to_metadata_folder, save_summary_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_repo_root() -> Path:
    """Get the repository root directory."""
    # When running in GitHub Actions
    if 'GITHUB_WORKSPACE' in os.environ:
        return Path(os.environ['GITHUB_WORKSPACE'])
    # When running locally, find git root
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / '.git').exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find repository root")


def run_pipeline(process_all: bool = False):
    """
    Run the ETL pipeline.
    
    Args:
        process_all: If True, process all .pol files. If False, only process changed files.
    """
    repo_root = get_repo_root()
    metadata_dir = repo_root / 'Meta_data'
    
    logger.info(f"Repository root: {repo_root}")
    logger.info(f"Metadata output directory: {metadata_dir}")
    
    # Create Meta_data directory if it doesn't exist
    metadata_dir.mkdir(exist_ok=True)
    
    # =========================================================================
    # EXTRACT
    # =========================================================================
    logger.info("=" * 60)
    logger.info("EXTRACT PHASE")
    logger.info("=" * 60)
    
    if process_all:
        pol_files_data = extract_all_pol_files(repo_root)
    else:
        # Try to get only changed files first
        pol_files_data = get_changed_pol_files(repo_root)
        if not pol_files_data:
            logger.info("No changed .pol files detected, processing all files...")
            pol_files_data = extract_all_pol_files(repo_root)
    
    if not pol_files_data:
        logger.warning("No .pol files found to process")
        return
    
    logger.info(f"Found {len(pol_files_data)} .pol files to process")
    
    # =========================================================================
    # TRANSFORM
    # =========================================================================
    logger.info("=" * 60)
    logger.info("TRANSFORM PHASE")
    logger.info("=" * 60)
    
    transformed_data = []
    errors = []
    
    for file_info in pol_files_data:
        try:
            result = transform_pol_data(file_info)
            transformed_data.append(result)
            logger.info(f"✓ Transformed: {file_info['relative_path']}")
        except Exception as e:
            error_msg = f"✗ Error transforming {file_info['relative_path']}: {str(e)}"
            logger.error(error_msg)
            errors.append({
                'file': file_info['relative_path'],
                'error': str(e)
            })
    
    logger.info(f"Successfully transformed {len(transformed_data)} files")
    if errors:
        logger.warning(f"Failed to transform {len(errors)} files")
    
    # =========================================================================
    # LOAD
    # =========================================================================
    logger.info("=" * 60)
    logger.info("LOAD PHASE")
    logger.info("=" * 60)
    
    saved_files = save_to_metadata_folder(transformed_data, metadata_dir, repo_root)
    logger.info(f"Saved {len(saved_files)} files to Meta_data/")
    
    # Generate summary report
    summary = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'total_files_processed': len(pol_files_data),
        'successful_transforms': len(transformed_data),
        'failed_transforms': len(errors),
        'errors': errors,
        'output_files': [str(f.relative_to(repo_root)) for f in saved_files]
    }
    
    # Add aggregated statistics across all files
    if transformed_data:
        summary['aggregated'] = generate_aggregated_summary(transformed_data)
    
    save_summary_report(summary, metadata_dir)
    logger.info("Pipeline completed successfully!")
    
    return summary


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run ETL Pipeline for .pol files')
    parser.add_argument(
        '--all', 
        action='store_true', 
        help='Process all .pol files instead of just changed ones'
    )
    args = parser.parse_args()
    
    run_pipeline(process_all=args.all)
