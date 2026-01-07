"""
Load Module
Handles saving transformed data to the Meta_data folder in the repository.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)



def save_to_metadata_folder(
    transformed_data_list: List[Dict[str, Any]], 
    metadata_dir: Path,
    repo_root: Path
) -> List[Path]:
    """
    Save all transformed data to a single consolidated JSON file.
    Updates only the entries for the files that were processed.
    
    Args:
        transformed_data_list: List of transformed data dictionaries
        metadata_dir: Path to the Meta_data directory
        repo_root: Path to the repository root
        
    Returns:
        List of paths to saved files (single file in this case)
    """
    output_file = metadata_dir / 'all_pools_data.json'
    
    # Load existing data if available to preserve other files' data
    all_data = {}
    if output_file.exists():
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
            logger.info(f"Loaded existing data with {len(all_data)} entries")
        except Exception as e:
            logger.warning(f"Could not load existing data from {output_file}: {e}. Starting fresh.")
    
    # Update data with new transformed items
    updated_count = 0
    for transformed_data in transformed_data_list:
        try:
            # Use source_file (relative path) as the unique key
            metadata = transformed_data.get('metadata', {})
            relative_path = metadata.get('source_file')
            
            if not relative_path:
                logger.warning(f"Missing source_file in metadata for pool {transformed_data.get('pool_name')}, skipping item")
                continue
            
            # Normalize path separators to look cleaner in JSON
            key = str(Path(relative_path).as_posix())
            
            all_data[key] = transformed_data
            updated_count += 1
            
        except Exception as e:
            source_file = transformed_data.get('metadata', {}).get('source_file', 'unknown')
            logger.error(f"Error preparing data for {source_file}: {e}")
    
    # Create parent directories
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Save back to file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Successfully updated {output_file}")
        logger.info(f"Updated {updated_count} entries, total entries: {len(all_data)}")
        
        return [output_file]
        
    except Exception as e:
        logger.error(f"Failed to save consolidated data to {output_file}: {e}")
        return []


def save_summary_report(summary: Dict[str, Any], metadata_dir: Path) -> Path:
    """
    Save a summary report of the ETL pipeline run.
    
    Args:
        summary: Summary dictionary with pipeline statistics
        metadata_dir: Path to the Meta_data directory
        
    Returns:
        Path to the saved summary file
    """
    # Create a timestamped filename for the latest run
    summary_path = metadata_dir / '_pipeline_summary.json'
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"Saved pipeline summary to {summary_path}")
    
    return summary_path


def generate_index_file(metadata_dir: Path, repo_root: Path) -> Path:
    """
    Generate an index file listing all processed files and their metadata.
    
    Args:
        metadata_dir: Path to the Meta_data directory
        repo_root: Path to the repository root
        
    Returns:
        Path to the index file
    """
    index_data = {
        'generated_at': datetime.utcnow().isoformat(timespec='seconds'),
        'files': []
    }
    
    # Find all JSON files in Meta_data (excluding special files)
    for json_file in metadata_dir.rglob('*.json'):
        if json_file.name.startswith('_'):
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = data.get('metadata', {})
            stats = data.get('statistics', {})
            
            index_data['files'].append({
                'output_file': str(json_file.relative_to(metadata_dir)),
                'source_file': metadata.get('source_file'),
                'parent_folder': metadata.get('parent_folder'),
                'processed_at': metadata.get('processed_at'),
                'line_count': stats.get('total_lines'),
            })
        except Exception as e:
            logger.warning(f"Could not read {json_file} for index: {e}")
    
    # Sort by source file path
    index_data['files'].sort(key=lambda x: x.get('source_file', ''))
    index_data['total_files'] = len(index_data['files'])
    
    index_path = metadata_dir / '_index.json'
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Generated index file with {len(index_data['files'])} entries")
    
    return index_path


def save_as_csv(
    transformed_data_list: List[Dict[str, Any]], 
    metadata_dir: Path,
    filename: str = '_all_files_summary.csv'
) -> Path:
    """
    Save a CSV summary of all processed files.
    Useful for quick analysis in Excel or other tools.
    
    Args:
        transformed_data_list: List of transformed data dictionaries
        metadata_dir: Path to the Meta_data directory
        filename: Name of the output CSV file
        
    Returns:
        Path to the saved CSV file
    """
    import csv
    
    csv_path = metadata_dir / filename
    
    # Define CSV columns
    fieldnames = [
        'source_file',
        'file_name', 
        'parent_folder',
        'processed_at',
        'file_size_bytes',
        'total_lines',
        'non_empty_lines',
        'detected_format'
    ]
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for data in transformed_data_list:
            metadata = data.get('metadata', {})
            stats = data.get('statistics', {})
            parsed = data.get('parsed', {})
            
            writer.writerow({
                'source_file': metadata.get('source_file'),
                'file_name': metadata.get('file_name'),
                'parent_folder': metadata.get('parent_folder'),
                'processed_at': metadata.get('processed_at'),
                'file_size_bytes': metadata.get('file_size_bytes'),
                'total_lines': stats.get('total_lines'),
                'non_empty_lines': stats.get('non_empty_lines'),
                'detected_format': parsed.get('format'),
            })
    
    logger.info(f"Saved CSV summary to {csv_path}")
    
    return csv_path
