"""
Extract Module
Handles discovery and reading of .pol files from the repository.
"""

import subprocess
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# Directories to exclude from scanning
EXCLUDED_DIRS = {
    '.git',
    '.github',
    'Meta_data',
    '__pycache__',
    '.venv',
    'venv',
    'node_modules',
    'etl'
}


def find_all_pol_files(repo_root: Path) -> List[Path]:
    """
    Recursively find all .pol files in the repository.
    
    Args:
        repo_root: Path to the repository root
        
    Returns:
        List of Path objects for each .pol file found
    """
    pol_files = []
    
    for path in repo_root.rglob('*.pol'):
        # Skip excluded directories
        if any(excluded in path.parts for excluded in EXCLUDED_DIRS):
            continue
        pol_files.append(path)
    
    return sorted(pol_files)


def get_changed_files_from_git(repo_root: Path) -> List[str]:
    """
    Get list of changed files from the last commit using git.
    
    Args:
        repo_root: Path to the repository root
        
    Returns:
        List of relative file paths that changed
    """
    try:
        result = subprocess.run(
            ['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True
        )
        changed = [f.strip() for f in result.stdout.strip().split('\n') if f.strip()]
        return changed
    except subprocess.CalledProcessError as e:
        logger.warning(f"Could not get git diff: {e}")
        return []
    except Exception as e:
        logger.warning(f"Error getting changed files: {e}")
        return []


def read_pol_file(file_path: Path) -> str:
    """
    Read the contents of a .pol file.
    
    Args:
        file_path: Path to the .pol file
        
    Returns:
        String contents of the file
    """
    # Try different encodings
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    
    # If all text encodings fail, read as binary and decode with errors='replace'
    with open(file_path, 'rb') as f:
        return f.read().decode('utf-8', errors='replace')


def extract_file_metadata(file_path: Path, repo_root: Path) -> Dict[str, Any]:
    """
    Extract metadata about a file.
    
    Args:
        file_path: Path to the file
        repo_root: Path to the repository root
        
    Returns:
        Dictionary with file metadata
    """
    stat = file_path.stat()
    
    return {
        'file_name': file_path.name,
        'file_stem': file_path.stem,  # filename without extension
        'extension': file_path.suffix,
        'relative_path': str(file_path.relative_to(repo_root)),
        'parent_folder': file_path.parent.name,
        'folder_path': str(file_path.parent.relative_to(repo_root)),
        'absolute_path': str(file_path),
        'size_bytes': stat.st_size,
        'modified_timestamp': stat.st_mtime,
    }


def extract_all_pol_files(repo_root: Path) -> List[Dict[str, Any]]:
    """
    Extract all .pol files with their content and metadata.
    
    Args:
        repo_root: Path to the repository root
        
    Returns:
        List of dictionaries containing file data
    """
    pol_files = find_all_pol_files(repo_root)
    logger.info(f"Found {len(pol_files)} .pol files in repository")
    
    extracted_data = []
    
    for file_path in pol_files:
        try:
            file_data = extract_file_metadata(file_path, repo_root)
            file_data['content'] = read_pol_file(file_path)
            file_data['line_count'] = len(file_data['content'].splitlines())
            extracted_data.append(file_data)
        except Exception as e:
            logger.error(f"Error extracting {file_path}: {e}")
    
    return extracted_data


def get_changed_pol_files(repo_root: Path) -> List[Dict[str, Any]]:
    """
    Extract only .pol files that changed in the last commit.
    
    Args:
        repo_root: Path to the repository root
        
    Returns:
        List of dictionaries containing file data for changed files
    """
    changed_files = get_changed_files_from_git(repo_root)
    
    if not changed_files:
        logger.info("No changed files detected from git")
        return []
    
    # Filter for .pol files only
    changed_pol_files = [f for f in changed_files if f.endswith('.pol')]
    
    if not changed_pol_files:
        logger.info("No .pol files in the changed files list")
        return []
    
    logger.info(f"Found {len(changed_pol_files)} changed .pol files")
    
    extracted_data = []
    
    for relative_path in changed_pol_files:
        file_path = repo_root / relative_path
        
        # Skip if file was deleted
        if not file_path.exists():
            logger.info(f"Skipping deleted file: {relative_path}")
            continue
        
        # Skip excluded directories
        if any(excluded in file_path.parts for excluded in EXCLUDED_DIRS):
            continue
        
        try:
            file_data = extract_file_metadata(file_path, repo_root)
            file_data['content'] = read_pol_file(file_path)
            file_data['line_count'] = len(file_data['content'].splitlines())
            extracted_data.append(file_data)
        except Exception as e:
            logger.error(f"Error extracting {file_path}: {e}")
    
    return extracted_data
