"""Input/Output handling for JSON and JSONL files."""

import json
from pathlib import Path
from typing import List, Optional
from loguru import logger

from .models import InputData, CrawledRepository


class InputHandler:
    """Handler for reading input JSON files."""
    
    @staticmethod
    def load_input_data(file_path: str) -> Optional[InputData]:
        """Load input data from JSON file."""
        try:
            logger.info(f"Loading input data from: {file_path}")
            
            path = Path(file_path)
            if not path.exists():
                logger.error(f"Input file does not exist: {file_path}")
                return None
            
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert to InputData object
            input_data = InputData.from_dict(data)
            
            logger.info(f"Successfully loaded {len(input_data.repositories)} repositories from {file_path}")
            return input_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading input file {file_path}: {e}")
            return None
    
    @staticmethod
    def load_multiple_input_files(file_paths: List[str]) -> List[InputData]:
        """Load multiple input JSON files."""
        input_data_list = []
        
        for file_path in file_paths:
            input_data = InputHandler.load_input_data(file_path)
            if input_data:
                input_data_list.append(input_data)
        
        logger.info(f"Successfully loaded {len(input_data_list)} input files")
        return input_data_list


class OutputHandler:
    """Handler for writing output JSONL files."""
    
    @staticmethod
    def save_crawled_repositories(repositories: List[CrawledRepository], output_path: str) -> bool:
        """Save crawled repositories to JSONL file."""
        try:
            logger.info(f"Saving {len(repositories)} repositories to: {output_path}")
            
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                for repo in repositories:
                    # Convert to dict and write as JSON line
                    repo_dict = repo.to_dict()
                    json_line = json.dumps(repo_dict, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            logger.info(f"Successfully saved repositories to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving repositories to {output_path}: {e}")
            return False
    
    @staticmethod
    def load_crawled_repositories(file_path: str) -> List[CrawledRepository]:
        """Load crawled repositories from JSONL file."""
        try:
            logger.info(f"Loading crawled repositories from: {file_path}")
            
            path = Path(file_path)
            if not path.exists():
                logger.error(f"Output file does not exist: {file_path}")
                return []
            
            repositories = []
            with open(path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        repo_dict = json.loads(line)
                        repo = CrawledRepository.from_dict(repo_dict)
                        repositories.append(repo)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num} in {file_path}: {e}")
                    except Exception as e:
                        logger.warning(f"Error parsing line {line_num} in {file_path}: {e}")
            
            logger.info(f"Successfully loaded {len(repositories)} repositories from {file_path}")
            return repositories
            
        except Exception as e:
            logger.error(f"Error loading repositories from {file_path}: {e}")
            return []
    
    @staticmethod
    def append_crawled_repository(repository: CrawledRepository, output_path: str) -> bool:
        """Append a single crawled repository to JSONL file."""
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'a', encoding='utf-8') as f:
                repo_dict = repository.to_dict()
                json_line = json.dumps(repo_dict, ensure_ascii=False)
                f.write(json_line + '\n')
            
            return True
            
        except Exception as e:
            logger.error(f"Error appending repository to {output_path}: {e}")
            return False


class FileManager:
    """Manager for handling file operations."""
    
    @staticmethod
    def generate_output_filename(input_filename: str, star_threshold: int) -> str:
        """Generate output filename based on input filename and parameters."""
        input_path = Path(input_filename)
        stem = input_path.stem
        
        output_filename = f"{stem}_crawled_stars_{star_threshold}.jsonl"
        return output_filename
    
    @staticmethod
    def validate_input_files(file_paths: List[str]) -> List[str]:
        """Validate that input files exist and return valid paths."""
        valid_paths = []
        
        for file_path in file_paths:
            path = Path(file_path)
            if path.exists() and path.is_file():
                valid_paths.append(file_path)
                logger.info(f"Valid input file: {file_path}")
            else:
                logger.warning(f"Invalid or missing input file: {file_path}")
        
        return valid_paths
    
    @staticmethod
    def ensure_output_directory(output_path: str) -> bool:
        """Ensure output directory exists."""
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Failed to create output directory for {output_path}: {e}")
            return False
