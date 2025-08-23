#!/bin/bash

# Redis Chaos Testing Script
# This script helps you run various chaos testing scenarios

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Base URL for the application
BASE_URL="http://localhost:8080/chaos"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${PURPLE}[CHAOS]${NC} $1"
}

# Function to check if the application is running
check_app_status() {
    print_status "Checking if Redis Chaos app is running..."
    if curl -s "$BASE_URL/health" > /dev/null; then
        print_status "✅ Application is running and responsive"
        return 0
    else
        print_error "❌ Application is not running or not responding"
        print_warning "Please start the application first:"
        print_warning "  cd redis-chaos-app && mvn spring-boot:run"
        return 1
    fi
}

# Function to run a test scenario
run_test() {
    local test_name="$1"
    local endpoint="$2"
    local description="$3"
    
    print_header "Running: $test_name"
    print_status "$description"
    
    response=$(curl -s -w "HTTPSTATUS:%{http_code}" "$endpoint")
    http_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    body=$(echo "$response" | sed -e 's/HTTPSTATUS:.*//g')
    
    if [ "$http_code" -eq 200 ]; then
        print_status "✅ $test_name completed successfully"
        echo "$body" | jq '.' 2>/dev/null || echo "$body"
    else
        print_error "❌ $test_name failed with HTTP status $http_code"
        echo "$body"
    fi
    echo ""
}

# Function to run Maven tests
run_maven_tests() {
    print_header "Running Maven Test Suite"
    print_status "This will run all JUnit integration tests..."
    
    if mvn test -Dtest=ChaosTestControllerIT; then
        print_status "✅ Maven integration tests completed successfully"
    else
        print_error "❌ Maven integration tests failed"
    fi
}

# Function to run the full test suite
run_full_test_suite() {
    print_header "Running Full Chaos Test Suite"
    print_status "This will run comprehensive integration tests..."
    
    if mvn test -Dtest=ChaosScenarioTestSuite; then
        print_status "✅ Full chaos test suite completed successfully"
    else
        print_error "❌ Full chaos test suite failed"
    fi
}

# Function to run manual tests
run_manual_tests() {
    print_header "Running Manual Test Runner"
    print_status "Starting interactive test runner..."
    
    if mvn exec:java -Dexec.mainClass="com.fostermoore.redis.chaos.ManualChaosTestRunner"; then
        print_status "✅ Manual test runner completed"
    else
        print_error "❌ Manual test runner failed"
    fi
}

# Function to display menu
show_menu() {
    echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}          🔥 Redis Chaos Test Runner 🔥         ${BLUE}║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Choose a test scenario:"
    echo "1. Quick Health Check"
    echo "2. Light Performance Test (250 ops, 5 threads)"
    echo "3. Heavy Performance Test (2000 ops, 20 threads)"
    echo "4. Stress Test (1000 ops, 15 concurrent users)"
    echo "5. Failover Test (300 keys, 5 retry attempts)"
    echo "6. Data Integrity Test (500 records)"
    echo "7. Memory Test (50 large objects, 20KB each)"
    echo "8. Concurrent Access Test (20 threads, 50 ops each)"
    echo "9. All API Tests (runs scenarios 1-8)"
    echo "10. Maven Integration Tests"
    echo "11. Full Maven Test Suite"
    echo "12. Interactive Manual Test Runner"
    echo "13. Cleanup Test Data"
    echo "0. Exit"
    echo ""
}

# Main menu loop
main_menu() {
    if ! check_app_status; then
        exit 1
    fi
    
    while true; do
        show_menu
        read -p "Enter your choice (0-13): " choice
        echo ""
        
        case $choice in
            1)
                run_test "Health Check" "$BASE_URL/health" "Checking Redis cluster health and connectivity"
                ;;
            2)
                run_test "Light Performance Test" "$BASE_URL/performance-test?operations=250&threads=5&async=false" "Running light load performance test"
                ;;
            3)
                run_test "Heavy Performance Test" "$BASE_URL/performance-test?operations=2000&threads=20&async=true" "Running heavy load performance test"
                ;;
            4)
                run_test "Stress Test" "$BASE_URL/stress-test?operations=1000&concurrentUsers=15" "Running stress test with concurrent users"
                ;;
            5)
                run_test "Failover Test" "$BASE_URL/failover-test?keyCount=300&retryAttempts=5" "Testing failover and retry mechanisms"
                ;;
            6)
                run_test "Data Integrity Test" "$BASE_URL/data-integrity-test?recordCount=500" "Testing data integrity under load"
                ;;
            7)
                run_test "Memory Test" "$BASE_URL/memory-test?largeObjectCount=50&objectSizeBytes=20480" "Testing large object memory handling"
                ;;
            8)
                run_test "Concurrent Access Test" "$BASE_URL/concurrent-access-test?threadCount=20&operationsPerThread=50" "Testing concurrent access patterns"
                ;;
            9)
                print_header "Running All API Test Scenarios"
                run_test "Health Check" "$BASE_URL/health" "1/8: Checking cluster health"
                run_test "Light Performance" "$BASE_URL/performance-test?operations=250&threads=5&async=false" "2/8: Light performance test"
                run_test "Heavy Performance" "$BASE_URL/performance-test?operations=1000&threads=15&async=true" "3/8: Heavy performance test"
                run_test "Stress Test" "$BASE_URL/stress-test?operations=800&concurrentUsers=12" "4/8: Stress testing"
                run_test "Failover Test" "$BASE_URL/failover-test?keyCount=200&retryAttempts=4" "5/8: Failover testing"
                run_test "Data Integrity" "$BASE_URL/data-integrity-test?recordCount=300" "6/8: Data integrity testing"
                run_test "Memory Test" "$BASE_URL/memory-test?largeObjectCount=30&objectSizeBytes=15360" "7/8: Memory testing"
                run_test "Concurrent Access" "$BASE_URL/concurrent-access-test?threadCount=15&operationsPerThread=40" "8/8: Concurrent access testing"
                print_status "🎉 All API test scenarios completed!"
                ;;
            10)
                run_maven_tests
                ;;
            11)
                run_full_test_suite
                ;;
            12)
                run_manual_tests
                ;;
            13)
                run_test "Cleanup" "$BASE_URL/cleanup" "Cleaning up all test data" "DELETE"
                ;;
            0)
                print_status "Thanks for using Redis Chaos Test Runner! 🐵"
                exit 0
                ;;
            *)
                print_warning "Invalid choice. Please select a number between 0-13."
                ;;
        esac
        
        read -p "Press ENTER to continue..."
        echo ""
    done
}

# Check if curl and jq are available
check_dependencies() {
    if ! command -v curl &> /dev/null; then
        print_error "curl is required but not installed. Please install curl."
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        print_warning "jq is recommended for JSON formatting but not installed."
        print_warning "JSON responses will be displayed without formatting."
    fi
}

# Run quick tests if argument provided
if [ "$1" = "quick" ]; then
    check_dependencies
    check_app_status
    run_test "Quick Health Check" "$BASE_URL/health" "Quick Redis cluster health check"
    run_test "Quick Performance Test" "$BASE_URL/performance-test?operations=100&threads=3&async=false" "Quick performance validation"
    print_status "🎉 Quick tests completed!"
elif [ "$1" = "all" ]; then
    check_dependencies
    check_app_status
    print_header "Running All Test Scenarios"
    # Run scenarios 1-8 from the menu
    run_test "Health Check" "$BASE_URL/health" "Checking cluster health"
    run_test "Light Performance" "$BASE_URL/performance-test?operations=250&threads=5&async=false" "Light performance test"
    run_test "Heavy Performance" "$BASE_URL/performance-test?operations=1000&threads=15&async=true" "Heavy performance test"
    run_test "Stress Test" "$BASE_URL/stress-test?operations=800&concurrentUsers=12" "Stress testing"
    run_test "Failover Test" "$BASE_URL/failover-test?keyCount=200&retryAttempts=4" "Failover testing"
    run_test "Data Integrity" "$BASE_URL/data-integrity-test?recordCount=300" "Data integrity testing"
    run_test "Memory Test" "$BASE_URL/memory-test?largeObjectCount=30&objectSizeBytes=15360" "Memory testing"
    run_test "Concurrent Access" "$BASE_URL/concurrent-access-test?threadCount=15&operationsPerThread=40" "Concurrent access testing"
    print_status "🎉 All test scenarios completed!"
else
    # Interactive menu
    check_dependencies
    main_menu
fi