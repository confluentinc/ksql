#!/usr/bin/env python3

import os
import sys
import json
from typing import List, Dict
from datetime import datetime
from topology_diff import compare_topologies

def process_batch(test_cases: List[str], start_idx: int, batch_num: int, base_dir: str, output_dir: str, changes_list: List[Dict]) -> None:
    """Process a batch of test cases and save results to a single file."""
    batch_results = []
    
    print(f"\nProcessing batch {batch_num} ({len(test_cases)} test cases)")
    print("=" * 80)
    
    for idx, test_case in enumerate(test_cases, start=start_idx):
        test_case_dir = os.path.join(base_dir, test_case)
        if not os.path.exists(test_case_dir):
            print(f"#{idx}: Skipping - directory not found: {test_case}")
            continue
            
        results = compare_topologies(test_case_dir)
        batch_results.append(results)
        
        # Print a brief status
        status = results.get("status", "completed")
        if status == "skipped":
            print(f"#{idx}: {test_case}: {results['reason']}")
        elif "error" in results:
            print(f"#{idx}: {test_case}: Error - {results['error']}")
        else:
            has_changes = results['comparison'].get('status') == 'changes_detected'
            changes = "changes detected" if has_changes else "no changes"
            print(f"#{idx}: {test_case}: {results['versions']['combination']} - {changes}")
            
            # If changes detected, add to changes list
            if has_changes:
                changes_list.append({
                    "test_case": test_case,
                    "test_number": idx,
                    "batch_number": batch_num,
                    "versions": results['versions'],
                    "changes": results['comparison']['sub_topologies']
                })
    
    # Save batch results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"batch_{batch_num:03d}_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump({
            "batch_number": batch_num,
            "start_index": start_idx,
            "timestamp": datetime.now().isoformat(),
            "test_cases": batch_results
        }, f, indent=2)
    
    print(f"\nBatch {batch_num} results saved to: {output_file}")

def main():
    # Create output directories if they don't exist
    base_output_dir = "/Users/pragatigupta/Desktop/Projects/ksql/topology_analysis"
    batch_dir = os.path.join(base_output_dir, "batches")
    os.makedirs(batch_dir, exist_ok=True)
    
    # Get all test cases
    base_dir = "/Users/pragatigupta/Desktop/Projects/ksql/ksqldb-functional-tests/src/test/resources/historical_plans"
    all_test_cases = sorted(os.listdir(base_dir))
    
    # Start from test case #401 (0-based index 400)
    start_idx = 400
    
    # Process in batches of 100
    batch_size = 100
    remaining_cases = all_test_cases[start_idx:]
    num_batches = (len(remaining_cases) + batch_size - 1) // batch_size
    
    print(f"Found {len(remaining_cases)} remaining test cases to process in {num_batches} batches")
    print(f"Starting from test case #{start_idx + 1}")
    
    # List to store all test cases with changes
    changes_list = []
    
    # Process all batches
    for i in range(num_batches):
        batch_start = i * batch_size
        batch_end = min(batch_start + batch_size, len(remaining_cases))
        batch = remaining_cases[batch_start:batch_end]
        
        process_batch(batch, start_idx + batch_start, i + 1, base_dir, batch_dir, changes_list)
    
    # Save the list of changes
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    changes_file = os.path.join(base_output_dir, f"topology_changes_{timestamp}.json")
    
    with open(changes_file, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "total_changes_found": len(changes_list),
            "changes": changes_list
        }, f, indent=2)
    
    # Also save a simple list of changed test cases
    changes_list_file = os.path.join(base_output_dir, f"changed_test_cases_{timestamp}.txt")
    
    with open(changes_list_file, 'w') as f:
        f.write(f"Found {len(changes_list)} test cases with changes:\n\n")
        for change in sorted(changes_list, key=lambda x: x['test_number']):
            f.write(f"#{change['test_number']}: {change['test_case']}\n")
            f.write(f"    {change['versions']['combination']}\n")
            for subtop, details in change['changes'].items():
                if details.get('status') == 'changes_detected':
                    added = len(details['changes'].get('added_in_v8', []))
                    removed = len(details['changes'].get('removed_in_v8', []))
                    f.write(f"    Sub-topology {subtop}: {removed} components removed, {added} components added\n")
            f.write("\n")
    
    print(f"\nProcessing complete!")
    print(f"Found {len(changes_list)} test cases with changes")
    print(f"Detailed changes saved to: {changes_file}")
    print(f"Summary of changes saved to: {changes_list_file}")

if __name__ == "__main__":
    main()