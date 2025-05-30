#!/usr/bin/env python3
"""
Main execution script for City of Cape Town Data Engineer Code Challenge
Run this to execute all tasks in sequence
"""

import time
import logging
import subprocess
import sys

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def run_script(script_name):
    """Run a script and return success/failure"""
    logger = setup_logging()
    logger.info(f"Starting {script_name}")
    start_time = time.time()
    
    try:
        result = subprocess.run([sys.executable, f'scripts/{script_name}'], 
                              capture_output=True, text=True, check=True)
        end_time = time.time()
        logger.info(f"{script_name} completed in {end_time - start_time:.2f} seconds")
        return True
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        logger.error(f"{script_name} failed after {end_time - start_time:.2f} seconds")
        logger.error(f"Error: {e.stderr}")
        return False

def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("CAPE TOWN DATA CHALLENGE - MAIN PIPELINE")
    logger.info("=" * 60)
    
    overall_start = time.time()
    
    tasks = [
    ('task1_s3_select.py', 'Task 1: Data Extraction with S3 SELECT'),
    ('task2_join_data.py', 'Task 2: Service Request H3 Joining'),  
    ('task3.py', 'Task 3: Bellville South Analysis & Anonymization')
]
    
    results = {}
    
    for script, description in tasks:
        logger.info(f"\n{description}")
        logger.info("-" * 50)
        success = run_script(script)
        results[description] = success
        
        if not success:
            logger.error(f"Pipeline stopped due to failure in {description}")
            break
    
    overall_end = time.time()
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    
    for task, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"{status} - {task}")
    
    logger.info(f"\nTotal pipeline execution time: {overall_end - overall_start:.2f} seconds")
    
    if all(results.values()):
        logger.info("ALL TASKS COMPLETED SUCCESSFULLY!")
        return 0
    else:
        logger.error("PIPELINE FAILED - Check logs above")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)