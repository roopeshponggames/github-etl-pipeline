"""
Transform Module
Parses .pol pool data files and calculates RTP, volatility, hit frequency.

Format: Each line contains "<value> <type_code>" or "<value> <type_code> <extra_value>"
Example:
    1800 TB2
    900 TB3 100
    515 TB2
"""

import re
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from collections import Counter

logger = logging.getLogger(__name__)

# Global game lookup dataframe - loaded once
_game_df: Optional[pd.DataFrame] = None


def load_game_lookup(repo_root: Path) -> Optional[pd.DataFrame]:
    """
    Load the game_id_to_pools.xlsx lookup file.
    
    Args:
        repo_root: Path to the repository root
        
    Returns:
        DataFrame with game lookup data or None if not found
    """
    global _game_df
    
    if _game_df is not None:
        return _game_df
    
    # Search for the lookup file
    possible_paths = [
        repo_root / 'game_id_to_pools.xlsx',
        repo_root / 'config' / 'game_id_to_pools.xlsx',
        repo_root / 'data' / 'game_id_to_pools.xlsx',
    ]
    
    for path in possible_paths:
        if path.exists():
            logger.info(f"Loading game lookup from: {path}")
            _game_df = pd.read_excel(path)
            _game_df['Game_id'] = _game_df['Game_id'].astype(str)
            _game_df['Pool_id'] = _game_df['Pool_id'].astype(str)
            return _game_df
    
    logger.warning("game_id_to_pools.xlsx not found. Some calculations will be skipped.")
    return None


def parse_pol_content(content: str) -> pd.DataFrame:
    """
    Parse .pol file content into a DataFrame.
    
    Handles format: "<value> <type_code>" or "<value> <type_code> <extra_value>"
    Extra value (if present) gets added to the main value.
    
    Args:
        content: Raw file content
        
    Returns:
        DataFrame with 'game_win' column
    """
    lines = content.strip().split('\n')
    
    # Parse into dataframe
    data = []
    for line in lines:
        parts = line.strip().split()
        if len(parts) >= 1:
            try:
                value = int(parts[0])
                # Handle third column (extra value to add)
                if len(parts) >= 3:
                    try:
                        extra = int(parts[2])
                        value += extra
                    except (ValueError, IndexError):
                        pass
                data.append(value)
            except ValueError:
                continue
    
    df = pd.DataFrame({'game_win': data})
    return df


def calculate_volatility(df: pd.DataFrame, min_bet: float, rtp: float) -> float:
    """
    Calculate volatility at 90% CI (z=1.645).
    
    Args:
        df: DataFrame with 'game_win' column
        min_bet: Minimum bet amount
        rtp: RTP percentage
        
    Returns:
        Volatility value
    """
    n = len(df)
    
    # Get value counts
    stats = pd.DataFrame(df['game_win'].value_counts())
    stats = stats.reset_index()
    stats.columns = ['winning', 'count']
    stats = stats.sort_values(by=['winning'])
    
    # Calculate metrics
    stats['win/bet'] = stats['winning'] / min_bet
    stats['freq%'] = stats['count'] / n
    stats['variance'] = (stats['freq%'] * (stats['win/bet'] - (rtp / 100)) ** 2).round(4)
    
    variance = stats['variance'].sum()
    sd = np.sqrt(variance)
    volatility = round(1.645 * sd, 2)
    
    return float(volatility)


def classify_pool(pool_type: str) -> Dict[str, Any]:
    """
    Classify pool based on pool_type number.
    
    Args:
        pool_type: Pool type string (e.g., '395', '50001234')
        
    Returns:
        Dictionary with classification info
    """
    pool_type_str = str(pool_type)
    
    # Determine tag
    if pool_type_str == '395':
        tag = ['GAB','PFB']
    elif len(pool_type_str) > 4 and pool_type_str[0] == '5':
        tag = ['PFB']
    else:
        tag = ['REG']
    
    # Determine if flat
    is_flat = 0
    max_multiplier = None
    
    if len(pool_type_str) > 4 and pool_type_str[0] == '4':
        is_flat = 1
        max_multiplier = pool_type_str[-4:]
    
    return {
        'tag': tag,
        'is_flat': is_flat,
        'max_multiplier': max_multiplier
    }


def transform_pol_data(file_info: Dict[str, Any], game_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Main transformation function for .pol pool data files.
    
    Calculates RTP, volatility, hit frequency based on game lookup data.
    
    Args:
        file_info: Dictionary containing file metadata and content
        game_df: Optional game lookup DataFrame
        
    Returns:
        Transformed data dictionary ready for loading
    """
    content = file_info.get('content', '')
    filename = file_info.get('file_name', '')
    
    # Extract pool info from filename (Pool_0201_395.pol)
    name = filename.replace('.pol', '')
    splits = name.split('_')
    
    pool_id = splits[1] if len(splits) > 1 else None
    pool_type = splits[2] if len(splits) > 2 else None
    
    # Parse content into DataFrame
    df = parse_pol_content(content)
    size = len(df)
    
    # Initialize results
    min_bet = None
    game_ids = []
    rtp = None
    volatility = None
    hit_freq = None
    
    # Get min_bet and game_ids from lookup
    if game_df is not None and pool_id is not None:
        # Try exact match first
        tmp = game_df[game_df['Pool_id'] == pool_id]
        
        # If no match, try without leading zeros
        if tmp.empty:
            pool_id_stripped = pool_id.lstrip('0') or '0'
            tmp = game_df[game_df['Pool_id'] == pool_id_stripped]
        
        # If still no match, try padding the lookup values
        if tmp.empty:
            tmp = game_df[game_df['Pool_id'].str.zfill(4) == pool_id]
        
        if not tmp.empty:
            min_bet = float(tmp['Bet'].iloc[0])
            game_ids = tmp['Game_id'].tolist()
    
    # Calculate metrics if we have min_bet
    if min_bet is not None and min_bet > 0 and size > 0:
        # RTP = sum(wins) / (count * min_bet) * 100
        total_win = df['game_win'].sum()
        rtp = round(float(total_win / (size * min_bet)) * 100, 2)
        
        # Hit Frequency = count(win > 0) / total_count * 100
        hits = len(df[df['game_win'] > 0])
        hit_freq = round((hits / size) * 100, 2)
        
        # Volatility at 90% CI
        volatility = calculate_volatility(df, min_bet, rtp)
    
    # Classify pool
    classification = classify_pool(pool_type) if pool_type else {'tag': 'UNKNOWN', 'is_flat': 0, 'max_multiplier': None}
    
    # Basic statistics
    values = df['game_win'].tolist()
    
    # Build result
    result = {
        'pool_name': filename,
        'pool_id': pool_id,
        'pool_type': pool_type,
        'game_ids': game_ids,
        'min_bet': min_bet,
        'rtp': rtp,
        'volatility': volatility,
        'is_flat': classification['is_flat'],
        'tag': classification['tag'],
        'size': size,
        'max_multiplier': classification['max_multiplier'],
        'metadata': {
            'source_file': file_info.get('relative_path'),
            'file_name': filename,
            'folder_path': file_info.get('folder_path'),
            'processed_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'hit_frequency': hit_freq
        }
    }
    
    return result


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
    tags_count = Counter()
    files_by_folder = Counter()
    rtp_values = []
    volatility_values = []
    
    for data in all_transformed:
        total_records += data.get('size', 0)
        
        if data.get('tag'):
            tags = data['tag']
            if isinstance(tags, list):
                for tag in tags:
                    tags_count[tag] += 1
            else:
                tags_count[tags] += 1
        
        if data.get('rtp') is not None:
            rtp_values.append(data['rtp'])
        
        if data.get('volatility') is not None:
            volatility_values.append(data['volatility'])
        
        folder = data.get('metadata', {}).get('parent_folder', 'root')
        files_by_folder[folder] += 1
    
    summary = {
        'total_files_processed': len(all_transformed),
        'total_records_across_all_files': total_records,
        'tags_distribution': dict(tags_count),
        'files_by_folder': dict(files_by_folder),
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds')
    }
    
    if rtp_values:
        summary['rtp_stats'] = {
            'min': min(rtp_values),
            'max': max(rtp_values),
            'avg': round(sum(rtp_values) / len(rtp_values), 2)
        }
    
    if volatility_values:
        summary['volatility_stats'] = {
            'min': min(volatility_values),
            'max': max(volatility_values),
            'avg': round(sum(volatility_values) / len(volatility_values), 2)
        }
    
    return summary
