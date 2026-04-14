#!/bin/bash

# =====================================================
# RCA PoC - Setup and Execution Script
# Comprehensive setup for running the RCA PoC
# =====================================================

echo "====================================================="
echo "RCA PoC - Asset Data Modernisation on Snowflake"
echo "Setup & Execution Script"
echo "====================================================="
echo ""

# Check Python installation
if ! command -v python &> /dev/null; then
    echo "❌ Python 3.9+ is not installed"
    exit 1
fi

PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
echo "✅ Python $PYTHON_VERSION detected"

# Navigate to project directory
cd "$(dirname "$0")"
PROJECT_DIR=$(pwd)
echo "📂 Project directory: $PROJECT_DIR"

# Create virtual environment
echo ""
echo "Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/Scripts/activate
echo "✅ Virtual environment activated"

# Install dependencies
echo ""
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Display configuration
echo ""
echo "====================================================="
echo "Configuration Summary"
echo "====================================================="
echo "Snowflake Account: TQWSLTQ-TW60698"
echo "Snowflake User: RANIT532"
echo "Database: RCA_POC_DB"
echo "Warehouse: RCA_ANALYTICS_WH"
echo "---"
echo "🔗 Before running, ensure you:"
echo "   1. Have run SQL setup scripts (sql/01-07)"
echo "   2. Updated Snowflake credentials if needed"
echo "   3. Have valid Snowflake account access"
echo ""

# Run orchestration
echo "====================================================="
echo "Launching Streamlit Controls Cockpit (feature branch)"
echo "====================================================="
echo ""
echo "Streamlit UI is the primary interface for this branch."
echo "Cortex AI panel available under the 'Cortex AI' sidebar tab."
echo ""
cd streamlit_app
streamlit run app.py
    3)
        echo ""
        echo "Running DYD Backend Integration Example..."
        python examples/dyd_backend_integration.py
        ;;
    4)
        echo ""
        echo "Generating Sample DYD Configuration..."
        python examples/dyd_sample_config.py
        ;;
    *)
        echo "Invalid option"
        exit 1
        ;;
esac

echo ""
echo "✅ Completed"
