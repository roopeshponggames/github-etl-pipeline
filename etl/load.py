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


def create_output_path(transformed_data: Dict[str, Any], metadata_dir: Path, repo_root: Path) -> Path:
    """
    Create the output file path, mirroring the source folder structure.
    
    Args:
        transformed_data: The transformed data dictionary
        metadata_dir: Path to the Meta_data directory
        repo_root: Path to the repository root
        
    Returns:
        Path object for the output file
    """
    source_metadata = transformed_data.get('metadata', {})
    folder_path = source_metadata.get('folder_path', '')
    file_name = source_metadata.get('file_name', 'unknown.pol')
    
    # Create output filename (replace .pol with .json)
    output_filename = file_name.replace('.pol', '.json')
    
    # Mirror the folder structure in Meta_data
    if folder_path:
        output_dir = metadata_dir / folder_path
    else:
        output_dir = metadata_dir
    
    return output_dir / output_filename


def save_to_metadata_folder(
    transformed_data_list: List[Dict[str, Any]], 
    metadata_dir: Path,
    repo_root: Path
) -> List[Path]:
    """
    Save all transformed data to the Meta_data folder.
    
    Args:
        transformed_data_list: List of transformed data dictionaries
        metadata_dir: Path to the Meta_data directory
        repo_root: Path to the repository root
        
    Returns:
        List of paths to saved files
    """
    saved_files = []
    
    for transformed_data in transformed_data_list:
        try:
            output_path = create_output_path(transformed_data, metadata_dir, repo_root)
            
            # Create parent directories if they don't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as formatted JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, indent=2, ensure_ascii=False, default=str)
            
            saved_files.append(output_path)
            logger.info(f"Saved: {output_path.relative_to(repo_root)}")
            
        except Exception as e:
            source_file = transformed_data.get('metadata', {}).get('source_file', 'unknown')
            logger.error(f"Error saving transformed data for {source_file}: {e}")
    
    return saved_files


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
        'generated_at': datetime.utcnow().isoformat(),
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
