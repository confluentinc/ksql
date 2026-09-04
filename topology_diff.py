#!/usr/bin/env python3

import os
import sys
import json
from typing import Tuple, Optional, Dict, Any
from datetime import datetime

def find_version_dirs(test_case_dir: str) -> Tuple[Optional[str], Optional[str], str]:
    """Find version directories:
    - Latest 7.x version for v7
    - Try 8.0.1, fallback to 8.0.0 for v8
    Returns: (v7_dir, v8_dir, version_combination)
    """
    v7_dir = None
    v801_dir = None
    v800_dir = None
    has_8x = False
    
    try:
        for dirname in os.listdir(test_case_dir):
            version = dirname.split('_')[0]  # e.g., "7.1.0" from "7.1.0_1633125989807"
            
            # Latest 7.x version
            if version.startswith('7.'):
                if not v7_dir or dirname > v7_dir:
                    v7_dir = dirname
            # Track 8.x versions
            elif version.startswith('8.'):
                has_8x = True
                # 8.0.1 version
                if version.startswith('8.0.1'):
                    if not v801_dir or dirname > v801_dir:
                        v801_dir = dirname
                # 8.0.0 version (fallback)
                elif version == '8.0.0':
                    if not v800_dir or dirname > v800_dir:
                        v800_dir = dirname
                    
    except Exception as e:
        print(f"Error reading directory {test_case_dir}: {e}")
        return None, None, ""
    
    # Skip if no 8.x version exists
    if not has_8x:
        print(f"\nSkipping test case - no 8.x version found in: {test_case_dir}")
        return None, None, "skip"
    
    # Use 8.0.1 if available, otherwise fallback to 8.0.0
    v8_dir = v801_dir if v801_dir else v800_dir
    version_desc = "8.0.1" if v801_dir else "8.0.0"
    
    if v7_dir and v8_dir:
        return v7_dir, v8_dir, f"Latest 7.x vs {version_desc}"
    
    return None, None, ""

def read_topology_file(filepath: str) -> list:
    """Read and return the contents of a topology file as a list of lines."""
    try:
        with open(filepath, 'r') as f:
            return [line.rstrip() for line in f.readlines()]
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return []

def normalize_line(line: str) -> str:
    """Normalize line by replacing PROCESSVALUES with TRANSFORMVALUES and standardizing arrows."""
    line = line.replace("PROCESSVALUES", "TRANSFORMVALUES")
    # Remove arrow prefixes for consistent comparison
    if line.startswith("-->"):
        line = line[3:].strip()
    elif line.startswith("<--"):
        line = line[3:].strip()
    return line

def clean_line(line: str) -> str:
    """Clean a line by removing any PEEK processor references."""
    if "PEEK" in line:
        # If line contains multiple processors (e.g., "-> Processor1, PEEK-123, Processor2")
        parts = [p.strip() for p in line.split(',')]
        cleaned_parts = [p for p in parts if "PEEK" not in p and "none" not in p]
        return ', '.join(cleaned_parts) if cleaned_parts else ""
    return line if "none" not in line else ""

def parse_topology(lines: list, normalize: bool = False) -> dict:
    """Parse topology file into a dictionary of sub-topologies."""
    subtops = {}
    current_subtop = None
    
    for line in lines:
        if "Sub-topology: " in line:
            current_subtop = line.split("Sub-topology: ")[1].strip()
            subtops[current_subtop] = []
        elif current_subtop is not None and line.strip():
            # Clean and normalize the line
            cleaned_line = clean_line(line.strip())
            if cleaned_line:  # Only add if there's content after cleaning
                if normalize:
                    cleaned_line = normalize_line(cleaned_line)
                else:
                    cleaned_line = normalize_line(cleaned_line)  # Always normalize arrows
                subtops[current_subtop].append(cleaned_line)
    
    return subtops

def compare_topologies(test_case_dir: str) -> Dict[str, Any]:
    """Compare topology files between versions for a given test case."""
    test_case_name = os.path.basename(test_case_dir)
    print(f"\nAnalyzing test case: {test_case_name}")
    
    result = {
        "test_case": test_case_name,
        "analysis_timestamp": datetime.now().isoformat(),
        "versions": {},
        "comparison": {
            "sub_topology_counts": {},
            "sub_topologies": {}
        }
    }
    
    # Find the version directories
    v7_dir, v8_dir, version_combo = find_version_dirs(test_case_dir)
    
    # Skip if no 8.x version or error finding versions
    if version_combo == "skip":
        result["status"] = "skipped"
        result["reason"] = "No 8.x version found"
        return result
    
    if not v7_dir or not v8_dir:
        result["error"] = "Could not find compatible version directories"
        return result
    
    result["versions"] = {
        "combination": version_combo,
        "v7": v7_dir,
        "v8": v8_dir
    }
    
    print(f"\nComparing versions ({version_combo}):")
    print(f"v7: {v7_dir}")
    print(f"v8: {v8_dir}")
    
    # Read topology files
    v7_topology = read_topology_file(os.path.join(test_case_dir, v7_dir, 'topology'))
    v8_topology = read_topology_file(os.path.join(test_case_dir, v8_dir, 'topology'))
    
    if not v7_topology or not v8_topology:
        result["error"] = "Could not read topology files"
        return result
    
    # Parse topologies - normalize both versions
    v7_subtops = parse_topology(v7_topology, normalize=True)
    v8_subtops = parse_topology(v8_topology, normalize=True)
    
    # Store sub-topology counts
    result["comparison"]["sub_topology_counts"] = {
        "v7": len(v7_subtops),
        "v8": len(v8_subtops)
    }
    
    # Compare each sub-topology
    all_subtops = sorted(set(v7_subtops.keys()) | set(v8_subtops.keys()))
    has_changes = False
    
    for subtop in all_subtops:
        subtop_result = {}
        
        # Check if sub-topology exists in both versions
        if subtop not in v7_subtops:
            subtop_result["status"] = "only_in_v8"
            subtop_result["contents_v8"] = v8_subtops[subtop]
            has_changes = True
        elif subtop not in v8_subtops:
            subtop_result["status"] = "only_in_v7"
            subtop_result["contents_v7"] = v7_subtops[subtop]
            has_changes = True
        else:
            # Compare processors and their connections
            v7_lines = set(v7_subtops[subtop])
            v8_lines = set(v8_subtops[subtop])
            
            removed = sorted(v7_lines - v8_lines)
            added = sorted(v8_lines - v7_lines)
            
            # Only include in results if there are actual changes
            if removed or added:
                subtop_result["status"] = "changes_detected"
                subtop_result["changes"] = {
                    "removed_in_v8": removed,
                    "added_in_v8": added
                }
                has_changes = True
        
        if subtop_result:  # Only add if there are changes
            result["comparison"]["sub_topologies"][subtop] = subtop_result
    
    if not has_changes:
        result["comparison"]["status"] = "no_significant_changes"
    else:
        result["comparison"]["status"] = "changes_detected"
    
    return result

def main():
    # Get test case from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 topology_diff.py <test_case_name>")
        sys.exit(1)
        
    test_case = sys.argv[1]
    # Create output directory if it doesn't exist
    output_dir = "/Users/pragatigupta/Desktop/Projects/ksql/topology_analysis"
    os.makedirs(output_dir, exist_ok=True)
    
    base_dir = "/Users/pragatigupta/Desktop/Projects/ksql/ksqldb-functional-tests/src/test/resources/historical_plans"
    test_case_dir = os.path.join(base_dir, test_case)
    
    if not os.path.exists(test_case_dir):
        print(f"Test case directory not found: {test_case}")
        return
    
    # Compare topologies and get results
    results = compare_topologies(test_case_dir)
    
    # Skip further processing if test was skipped
    if results.get("status") == "skipped":
        print(f"\nSkipped test case: {results['reason']}")
        return
    
    # Save results to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"{test_case}_{timestamp}.json")
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {output_file}")
    
    # Print a summary to console
    print("\nQuick Summary:")
    print(f"Test case: {results['test_case']}")
    print(f"Version combination: {results['versions']['combination']}")
    print(f"v7: {results['versions']['v7']}")
    print(f"v8: {results['versions']['v8']}")
    print(f"Sub-topology counts: v7={results['comparison']['sub_topology_counts']['v7']}, v8={results['comparison']['sub_topology_counts']['v8']}")
    
    # Print major structural changes
    print("\nMajor Structural Changes:")
    if results['comparison'].get('status') == 'no_significant_changes':
        print("No significant changes found between versions")
    else:
        for subtop, details in results['comparison']['sub_topologies'].items():
            if details.get('status') == 'changes_detected':
                added = len(details['changes'].get('added_in_v8', []))
                removed = len(details['changes'].get('removed_in_v8', []))
                if added > 0 or removed > 0:
                    print(f"Sub-topology {subtop}: {removed} components removed, {added} components added")

if __name__ == "__main__":
    main()