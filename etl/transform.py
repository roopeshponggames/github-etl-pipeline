"""
Transform Module
Parses .pol pool data files containing value-type pairs.

Format: Each line contains "<numeric_value> <type_code>"
Example:
    1800 TB2
    900 TB3
    515 TB2
"""

import re
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime, timezone
from collections import Counter

logger = logging.getLogger(__name__)


def parse_pol_content(content: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Parse .pol file content into structured records.
    
    Each line format: "<value> <type_code>"
    
    Args:
        content: Raw file content
        
    Returns:
        Tuple of (parsed records list, error messages list)
    """
    records = []
    errors = []
    
    for line_num, line in enumerate(content.splitlines(), start=1):
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
        
        # Parse "value type" format
        parts = line.split()
        
        if len(parts) >= 2:
            try:
                value = int(parts[0])
                type_code = parts[1].upper()
                
                records.append({
                    'value': value,
                    'type': type_code,
                    'line_number': line_num
                })
            except ValueError:
                # Try float if int fails
                try:
                    value = float(parts[0])
                    type_code = parts[1].upper()
                    records.append({
                        'value': value,
                        'type': type_code,
                        'line_number': line_num
                    })
                except ValueError:
                    errors.append(f"Line {line_num}: Could not parse '{line}'")
        elif len(parts) == 1:
            # Single value without type
            try:
                value = int(parts[0])
                records.append({
                    'value': value,
                    'type': 'UNKNOWN',
                    'line_number': line_num
                })
            except ValueError:
                errors.append(f"Line {line_num}: Invalid format '{line}'")
    
    return records, errors


def calculate_statistics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics from parsed records.
    
    Args:
        records: List of parsed record dictionaries
        
    Returns:
        Dictionary with statistical analysis
    """
    if not records:
        return {'error': 'No records to analyze'}
    
    values = [r['value'] for r in records]
    types = [r['type'] for r in records]
    
    # Basic statistics
    total_count = len(values)
    total_sum = sum(values)
    min_value = min(values)
    max_value = max(values)
    avg_value = total_sum / total_count
    
    # Calculate median
    sorted_values = sorted(values)
    mid = total_count // 2
    if total_count % 2 == 0:
        median_value = (sorted_values[mid - 1] + sorted_values[mid]) / 2
    else:
        median_value = sorted_values[mid]
    
    # Calculate standard deviation
    variance = sum((x - avg_value) ** 2 for x in values) / total_count
    std_dev = variance ** 0.5
    
    # Type distribution
    type_counts = Counter(types)
    type_distribution = dict(type_counts.most_common())
    
    # Value distribution by type
    type_stats = {}
    for type_code in set(types):
        type_values = [r['value'] for r in records if r['type'] == type_code]
        if type_values:
            type_stats[type_code] = {
                'count': len(type_values),
                'sum': sum(type_values),
                'min': min(type_values),
                'max': max(type_values),
                'avg': round(sum(type_values) / len(type_values), 2),
                'percentage': round(len(type_values) / total_count * 100, 2)
            }
    
    # Value range buckets
    buckets = {
        '0-500': 0,
        '501-1000': 0,
        '1001-2000': 0,
        '2001-3000': 0,
        '3001-5000': 0,
        '5001+': 0
    }
    
    for v in values:
        if v <= 500:
            buckets['0-500'] += 1
        elif v <= 1000:
            buckets['501-1000'] += 1
        elif v <= 2000:
            buckets['1001-2000'] += 1
        elif v <= 3000:
            buckets['2001-3000'] += 1
        elif v <= 5000:
            buckets['3001-5000'] += 1
        else:
            buckets['5001+'] += 1
    
    # Convert bucket counts to percentages too
    value_distribution = {
        bucket: {
            'count': count,
            'percentage': round(count / total_count * 100, 2)
        }
        for bucket, count in buckets.items()
    }
    
    return {
        'summary': {
            'total_records': total_count,
            'total_sum': total_sum,
            'min_value': min_value,
            'max_value': max_value,
            'avg_value': round(avg_value, 2),
            'median_value': round(median_value, 2),
            'std_deviation': round(std_dev, 2),
            'unique_types': len(type_counts),
            'unique_values': len(set(values))
        },
        'type_distribution': type_distribution,
        'type_statistics': type_stats,
        'value_distribution': value_distribution
    }


def extract_filename_metadata(filename: str) -> Dict[str, Any]:
    """
    Extract metadata from filename pattern like 'Pool_0201_395.pol'
    
    Args:
        filename: The filename to parse
        
    Returns:
        Dictionary with extracted metadata
    """
    metadata = {}
    
    # Remove extension
    name = filename.replace('.pol', '')
    
    # Try to parse Pool_XXXX_YYY pattern
    match = re.match(r'(\w+)_(\d+)_(\d+)', name)
    if match:
        metadata['pool_type'] = match.group(1)
        metadata['pool_id'] = match.group(2)
        metadata['pool_variant'] = match.group(3)
    else:
        # Try simpler patterns
        parts = name.split('_')
        if len(parts) >= 1:
            metadata['pool_type'] = parts[0]
        if len(parts) >= 2:
            metadata['pool_id'] = parts[1]
        if len(parts) >= 3:
            metadata['pool_variant'] = '_'.join(parts[2:])
    
    return metadata


def transform_pol_data(file_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main transformation function for .pol pool data files.
    
    Args:
        file_info: Dictionary containing file metadata and content
        
    Returns:
        Transformed data dictionary ready for loading
    """
    content = file_info.get('content', '')
    filename = file_info.get('file_name', '')
    
    # Parse the content
    records, parse_errors = parse_pol_content(content)
    
    # Calculate statistics
    statistics = calculate_statistics(records)
    
    # Extract filename metadata
    filename_meta = extract_filename_metadata(filename)
    
    # Build transformed output
    transformed = {
        # File metadata
        'metadata': {
            'source_file': file_info.get('relative_path'),
            'file_name': filename,
            'parent_folder': file_info.get('parent_folder'),
            'folder_path': file_info.get('folder_path'),
            'processed_at': datetime.now(timezone.utc).isoformat(),
            'file_size_bytes': file_info.get('size_bytes'),
            **filename_meta  # Include parsed filename components
        },
        
        # Statistics and analysis
        'statistics': statistics,
        
        # Parse info
        'parse_info': {
            'total_lines': file_info.get('line_count', len(content.splitlines())),
            'parsed_records': len(records),
            'parse_errors': len(parse_errors),
            'error_details': parse_errors[:10] if parse_errors else []  # First 10 errors
        },
        
        # Sample records (first and last 10)
        'sample_data': {
            'first_10': records[:10],
            'last_10': records[-10:] if len(records) > 10 else []
        }
    }
    
    return transformed


def generate_aggregated_summary(all_transformed: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate an aggregated summary across all processed .pol files.
    
    Args:
        all_transformed: List of all transformed data dictionaries
        
    Returns:
        Aggregated summary dictionary
    """
    if not all_transformed:
        return {}
    
    total_records = 0
    total_sum = 0
    all_types = Counter()
    files_by_folder = Counter()
    
    for data in all_transformed:
        stats = data.get('statistics', {}).get('summary', {})
        total_records += stats.get('total_records', 0)
        total_sum += stats.get('total_sum', 0)
        
        type_dist = data.get('statistics', {}).get('type_distribution', {})
        all_types.update(type_dist)
        
        folder = data.get('metadata', {}).get('parent_folder', 'root')
        files_by_folder[folder] += 1
    
    return {
        'total_files_processed': len(all_transformed),
        'total_records_across_all_files': total_records,
        'total_sum_across_all_files': total_sum,
        'global_type_distribution': dict(all_types.most_common()),
        'files_by_folder': dict(files_by_folder),
        'generated_at': datetime.now(timezone.utc).isoformat()
    }
