#!/bin/bash
#
# Deprecation Tracker for ksqlDB
# Extracts and reports UNIQUE deprecated methods and configs from EXTERNAL DEPENDENCIES
# that ksqlDB uses (e.g., Kafka, Vert.x, Jackson, Apache Commons, etc.)
#
# Usage:
#   ./extract-deprecations.sh [build_log_file]
#
# If no build log file is provided, it will run a Maven build and capture the output.
#

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${PROJECT_ROOT}/target/deprecation-report"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

# Colors for output (sent to stderr to not interfere with return values)
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

mkdir -p "${OUTPUT_DIR}"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_header() {
    echo -e "${BOLD}${CYAN}$1${NC}" >&2
}

# Function to run Maven build and capture output (only used if no build log provided)
run_maven_build() {
    local build_log="${OUTPUT_DIR}/maven-build-${TIMESTAMP}.log"
    log_info "Running Maven compile to capture deprecation warnings..."
    log_info "Build log: ${build_log}"
    
    cd "${PROJECT_ROOT}"
    
    # Run Maven compile with deprecation warnings
    # Note: This is a fallback - in CI, the build log from the main build should be passed
    mvn compile test-compile \
        -Dmaven.compiler.showDeprecation=true \
        -Dmaven.compiler.showWarnings=true \
        -DskipTests \
        --batch-mode \
        --no-transfer-progress \
        2>&1 > "${build_log}" || true
    
    echo "${build_log}"
}

# Function to extract UNIQUE deprecated APIs from EXTERNAL DEPENDENCIES
extract_unique_deprecated_apis() {
    local build_log="$1"
    local api_report="${OUTPUT_DIR}/unique-deprecated-apis-${TIMESTAMP}.txt"
    local api_json="${OUTPUT_DIR}/unique-deprecated-apis-${TIMESTAMP}.json"
    local api_details="${OUTPUT_DIR}/deprecated-api-details-${TIMESTAMP}.txt"
    
    log_info "Extracting UNIQUE deprecated APIs from EXTERNAL DEPENDENCIES..."
    
    # Temp files for processing
    local raw_file="${OUTPUT_DIR}/.raw_deprecations.tmp"
    local unique_file="${OUTPUT_DIR}/.unique_apis.tmp"
    local counts_file="${OUTPUT_DIR}/.api_counts.tmp"
    local runtime_file="${OUTPUT_DIR}/.runtime_deprecations.tmp"
    
    # Extract compiler deprecation warnings (multiple formats)
    # Format 1: [WARNING] /path/file.java:[line,col] APIName has been deprecated
    # Format 2: warning: [deprecation] APIName has been deprecated
    grep -E "(\[WARNING\].*has been deprecated|warning:.*\[deprecation\])" "${build_log}" > "${raw_file}" 2>/dev/null || touch "${raw_file}"
    
    # Also extract runtime deprecation warnings (from test runs, etc.)
    grep -E "WARN.*Using @Deprecated" "${build_log}" > "${runtime_file}" 2>/dev/null || touch "${runtime_file}"
    
    local total_warnings=0
    if [ -s "${raw_file}" ]; then
        total_warnings=$(wc -l < "${raw_file}" | tr -d ' ')
    fi
    
    local runtime_warnings=0
    if [ -s "${runtime_file}" ]; then
        runtime_warnings=$(wc -l < "${runtime_file}" | tr -d ' ')
    fi
    
    # Extract just the deprecated API signature
    # Format: "APIName in ClassName has been deprecated"
    # This captures the EXTERNAL library API being used
    
    # Process compiler warnings - extract the deprecation message
    sed -n 's/.*\] \([^[:space:]].*\) has been deprecated.*/\1/p' "${raw_file}" | \
        sed 's/[[:space:]]*$//' | \
        sort > "${unique_file}" 2>/dev/null || touch "${unique_file}"
    
    # Count occurrences and get unique entries
    if [ -s "${unique_file}" ]; then
        sort "${unique_file}" | uniq -c | sort -rn > "${counts_file}"
    else
        touch "${counts_file}"
    fi
    
    local unique_count=0
    if [ -s "${counts_file}" ]; then
        unique_count=$(wc -l < "${counts_file}" | tr -d ' ')
    fi
    
    # Process runtime warnings separately
    local runtime_unique_file="${OUTPUT_DIR}/.runtime_unique.tmp"
    sed -n 's/.*Using @Deprecated Class \([^[:space:]]*\).*/\1/p' "${runtime_file}" | \
        sort | uniq -c | sort -rn > "${runtime_unique_file}" 2>/dev/null || touch "${runtime_unique_file}"
    
    local runtime_unique_count=0
    if [ -s "${runtime_unique_file}" ]; then
        runtime_unique_count=$(wc -l < "${runtime_unique_file}" | tr -d ' ')
    fi
    
    # Generate the unique APIs report
    cat > "${api_report}" << EOF
================================================================================
UNIQUE DEPRECATED APIs FROM EXTERNAL DEPENDENCIES
Used by ksqlDB
Generated: $(date)
================================================================================

These are deprecated methods/classes from libraries that ksqlDB depends on
(Kafka, Vert.x, Jackson, Apache Commons, Jetty, etc.)

COMPILER DEPRECATION WARNINGS:
  Total warnings in build: ${total_warnings}
  Unique deprecated APIs: ${unique_count}

RUNTIME DEPRECATION WARNINGS:
  Total warnings: ${runtime_warnings}
  Unique deprecated classes: ${runtime_unique_count}

--------------------------------------------------------------------------------
UNIQUE DEPRECATED APIs FROM COMPILER (sorted by occurrence count)
--------------------------------------------------------------------------------

EOF
    
    # Output unique APIs with counts
    local idx=1
    while IFS= read -r line; do
        if [ -n "${line}" ]; then
            local count=$(echo "${line}" | awk '{print $1}')
            local api=$(echo "${line}" | sed 's/^[[:space:]]*[0-9]*[[:space:]]*//')
            echo "  ${idx}. ${api}" >> "${api_report}"
            echo "      Occurrences: ${count}" >> "${api_report}"
            echo "" >> "${api_report}"
            idx=$((idx + 1))
        fi
    done < "${counts_file}"
    
    if [ "${runtime_unique_count}" -gt 0 ]; then
        cat >> "${api_report}" << EOF

--------------------------------------------------------------------------------
UNIQUE DEPRECATED CLASSES FROM RUNTIME (sorted by occurrence count)
--------------------------------------------------------------------------------

EOF
        
        idx=1
        while IFS= read -r line; do
            if [ -n "${line}" ]; then
                local count=$(echo "${line}" | awk '{print $1}')
                local class=$(echo "${line}" | sed 's/^[[:space:]]*[0-9]*[[:space:]]*//')
                echo "  ${idx}. ${class}" >> "${api_report}"
                echo "      Occurrences: ${count}" >> "${api_report}"
                echo "" >> "${api_report}"
                idx=$((idx + 1))
            fi
        done < "${runtime_unique_file}"
    fi
    
    cat >> "${api_report}" << EOF
================================================================================
END OF UNIQUE DEPRECATED APIs REPORT
================================================================================
EOF
    
    # Generate JSON report
    cat > "${api_json}" << EOF
{
  "generated": "$(date)",
  "description": "Deprecated APIs from external dependencies used by ksqlDB",
  "compiler_warnings": {
    "total_warnings": ${total_warnings},
    "unique_api_count": ${unique_count}
  },
  "runtime_warnings": {
    "total_warnings": ${runtime_warnings},
    "unique_class_count": ${runtime_unique_count}
  },
  "unique_deprecated_apis": [
EOF
    
    local first=true
    while IFS= read -r line; do
        if [ -n "${line}" ]; then
            local count=$(echo "${line}" | awk '{print $1}')
            local api=$(echo "${line}" | sed 's/^[[:space:]]*[0-9]*[[:space:]]*//')
            # Escape special characters for JSON
            local escaped_api=$(echo "${api}" | sed 's/\\/\\\\/g; s/"/\\"/g')
            
            if [ "$first" = "true" ]; then
                first=false
            else
                echo "," >> "${api_json}"
            fi
            printf '    {"api": "%s", "occurrences": %s, "type": "compiler"}' "${escaped_api}" "${count}" >> "${api_json}"
        fi
    done < "${counts_file}"
    
    while IFS= read -r line; do
        if [ -n "${line}" ]; then
            local count=$(echo "${line}" | awk '{print $1}')
            local class=$(echo "${line}" | sed 's/^[[:space:]]*[0-9]*[[:space:]]*//')
            local escaped_class=$(echo "${class}" | sed 's/\\/\\\\/g; s/"/\\"/g')
            
            if [ "$first" = "true" ]; then
                first=false
            else
                echo "," >> "${api_json}"
            fi
            printf '    {"api": "%s", "occurrences": %s, "type": "runtime"}' "${escaped_class}" "${count}" >> "${api_json}"
        fi
    done < "${runtime_unique_file}"
    
    cat >> "${api_json}" << EOF

  ]
}
EOF
    
    # Generate detailed report with all occurrences
    cat > "${api_details}" << EOF
================================================================================
DETAILED DEPRECATED API USAGE (All Occurrences)
From External Dependencies
Generated: $(date)
================================================================================

--- COMPILER WARNINGS ---
EOF
    cat "${raw_file}" >> "${api_details}"
    
    if [ -s "${runtime_file}" ]; then
        cat >> "${api_details}" << EOF

--- RUNTIME WARNINGS ---
EOF
        cat "${runtime_file}" >> "${api_details}"
    fi
    
    # Cleanup
    rm -f "${raw_file}" "${unique_file}" "${counts_file}" "${runtime_file}" "${runtime_unique_file}"
    
    # Copy to latest
    cp "${api_report}" "${OUTPUT_DIR}/unique-deprecated-apis-latest.txt"
    cp "${api_json}" "${OUTPUT_DIR}/unique-deprecated-apis-latest.json"
    cp "${api_details}" "${OUTPUT_DIR}/deprecated-api-details-latest.txt"
    
    log_success "Unique deprecated APIs analysis complete!"
    log_info "Compiler: ${total_warnings} warnings, ${unique_count} unique APIs"
    log_info "Runtime: ${runtime_warnings} warnings, ${runtime_unique_count} unique classes"
    
    # Return the combined unique count for summary
    echo "$((unique_count + runtime_unique_count))"
}

# Function to extract UNIQUE deprecated configs from EXTERNAL DEPENDENCIES
extract_unique_deprecated_configs() {
    local config_report="${OUTPUT_DIR}/unique-deprecated-configs-${TIMESTAMP}.txt"
    local config_json="${OUTPUT_DIR}/unique-deprecated-configs-${TIMESTAMP}.json"
    
    log_info "Analyzing UNIQUE deprecated configuration patterns from dependencies..."
    
    cat > "${config_report}" << EOF
================================================================================
UNIQUE DEPRECATED CONFIGURATIONS FROM EXTERNAL DEPENDENCIES
Used by ksqlDB (Kafka, Kafka Streams, etc.)
Generated: $(date)
================================================================================

These are deprecated configuration keys from libraries that ksqlDB depends on.

--------------------------------------------------------------------------------
DEPRECATED CONFIGURATIONS FOUND
--------------------------------------------------------------------------------

EOF
    
    local unique_config_count=0
    local json_entries=""
    
    # Function to check a config pattern
    check_config() {
        local config_key="$1"
        local deprecation_reason="$2"
        local library="$3"
        
        # Search for this config in properties, yaml, and java files
        local matches
        matches=$(grep -rn "${config_key}" "${PROJECT_ROOT}" \
            --include="*.properties" \
            --include="*.yaml" \
            --include="*.yml" \
            --include="*.java" \
            --include="*.json" \
            --exclude-dir="target" \
            --exclude-dir=".git" \
            --exclude-dir="docs" \
            --exclude-dir="design-proposals" \
            --exclude-dir="scripts" \
            2>/dev/null | grep -v "Test" | grep -v "test" | head -20 || true)
        
        if [ -n "${matches}" ]; then
            unique_config_count=$((unique_config_count + 1))
            
            echo "  ${unique_config_count}. ${config_key}" >> "${config_report}"
            echo "     Library: ${library}" >> "${config_report}"
            echo "     Reason: ${deprecation_reason}" >> "${config_report}"
            echo "     Found in:" >> "${config_report}"
            echo "${matches}" | head -5 | sed 's/^/       /' >> "${config_report}"
            local match_count=$(echo "${matches}" | wc -l | tr -d ' ')
            if [ "${match_count}" -gt 5 ]; then
                echo "       ... and $((match_count - 5)) more occurrences" >> "${config_report}"
            fi
            echo "" >> "${config_report}"
            
            # Add to JSON entries
            local escaped_key=$(echo "${config_key}" | sed 's/\\/\\\\/g; s/"/\\"/g')
            local escaped_reason=$(echo "${deprecation_reason}" | sed 's/\\/\\\\/g; s/"/\\"/g')
            
            if [ -n "${json_entries}" ]; then
                json_entries="${json_entries},"$'\n'
            fi
            json_entries="${json_entries}    {\"config\": \"${escaped_key}\", \"library\": \"${library}\", \"reason\": \"${escaped_reason}\", \"occurrences\": ${match_count}}"
        fi
    }
    
    # Kafka Streams deprecated configs
    check_config "CACHE_MAX_BYTES_BUFFERING_CONFIG" "Use statestore.cache.max.bytes instead" "Kafka Streams"
    check_config "DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG" "Use deserialization.exception.handler instead" "Kafka Streams"
    check_config "DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG" "Use production.exception.handler instead" "Kafka Streams"
    check_config "WINDOW_SIZE_MS_CONFIG" "Deprecated in Kafka Streams" "Kafka Streams"
    
    # Kafka Client deprecated configs
    check_config "zookeeper.connect" "Use KRaft mode instead (Kafka 3.0+)" "Kafka"
    check_config "kafka.rest.url" "Use bootstrap.servers instead" "Kafka"
    
    # ksqlDB configs that wrap deprecated Kafka configs
    check_config "ksql.streams.state.dir" "Use state.dir instead" "Kafka Streams (via ksqlDB)"
    check_config "ksql.sink.partitions" "Use PARTITIONS in CREATE statement" "ksqlDB (deprecated)"
    check_config "ksql.sink.replicas" "Use REPLICAS in CREATE statement" "ksqlDB (deprecated)"
    
    cat >> "${config_report}" << EOF

================================================================================
SUMMARY
================================================================================

Unique deprecated configurations from dependencies: ${unique_config_count}

================================================================================
END OF DEPRECATED CONFIGURATIONS REPORT
================================================================================
EOF
    
    # Generate JSON report
    cat > "${config_json}" << EOF
{
  "generated": "$(date)",
  "description": "Deprecated configurations from external dependencies used by ksqlDB",
  "unique_config_count": ${unique_config_count},
  "deprecated_configs": [
${json_entries}
  ]
}
EOF
    
    # Copy to latest
    cp "${config_report}" "${OUTPUT_DIR}/unique-deprecated-configs-latest.txt"
    cp "${config_json}" "${OUTPUT_DIR}/unique-deprecated-configs-latest.json"
    
    log_success "Unique deprecated configs analysis complete!"
    log_info "Unique deprecated configs found: ${unique_config_count}"
    
    echo "${unique_config_count}"
}

# Function to print final summary
print_final_summary() {
    local unique_apis="$1"
    local unique_configs="$2"
    
    local summary_report="${OUTPUT_DIR}/deprecation-summary-${TIMESTAMP}.txt"
    
    cat > "${summary_report}" << EOF
================================================================================
DEPRECATION TRACKING SUMMARY FOR ksqlDB
External Dependencies Only
Generated: $(date)
================================================================================

These are deprecated APIs and configs from EXTERNAL LIBRARIES that ksqlDB uses:
  - Apache Kafka (clients, streams)
  - Vert.x
  - Jackson
  - Apache Commons
  - Jetty
  - And other dependencies

+-----------------------------------------------------------------------------+
|              UNIQUE DEPRECATED ITEMS FROM DEPENDENCIES                      |
+-----------------------------------------------------------------------------+
|  Unique Deprecated APIs (methods/classes):               $(printf "%16s" "${unique_apis}") |
|  Unique Deprecated Configs:                              $(printf "%16s" "${unique_configs}") |
+-----------------------------------------------------------------------------+

Reports generated in: ${OUTPUT_DIR}

  - Unique Deprecated APIs:    unique-deprecated-apis-latest.txt
  - Unique Deprecated Configs: unique-deprecated-configs-latest.txt  
  - Full Details:              deprecated-api-details-latest.txt
  - JSON Reports:              *-latest.json

================================================================================
EOF
    
    cat "${summary_report}"
    
    cp "${summary_report}" "${OUTPUT_DIR}/deprecation-summary-latest.txt"
}

# Main execution
main() {
    log_info "Starting deprecation analysis for ksqlDB..."
    log_info "Tracking deprecated APIs from EXTERNAL DEPENDENCIES only"
    log_info "Project root: ${PROJECT_ROOT}"
    log_info "Output directory: ${OUTPUT_DIR}"
    echo "" >&2
    
    local build_log=""
    
    if [ $# -gt 0 ] && [ -f "$1" ]; then
        build_log="$1"
        log_info "Using provided build log: ${build_log}"
    else
        build_log=$(run_maven_build)
    fi
    
    echo "" >&2
    echo "================================================================================" >&2
    echo "ANALYZING DEPRECATED APIs FROM EXTERNAL DEPENDENCIES..." >&2
    echo "================================================================================" >&2
    echo "" >&2
    
    # Run analyses and capture counts
    local unique_apis
    unique_apis=$(extract_unique_deprecated_apis "${build_log}")
    echo "" >&2
    
    local unique_configs
    unique_configs=$(extract_unique_deprecated_configs)
    echo "" >&2
    
    # Print final summary
    print_final_summary "${unique_apis}" "${unique_configs}"
    
    log_success "All deprecation analyses complete!"
    echo "" >&2
    echo "Quick view commands:" >&2
    echo "  cat ${OUTPUT_DIR}/unique-deprecated-apis-latest.txt" >&2
    echo "  cat ${OUTPUT_DIR}/unique-deprecated-configs-latest.txt" >&2
    echo "  cat ${OUTPUT_DIR}/deprecation-summary-latest.txt" >&2
}

main "$@"
