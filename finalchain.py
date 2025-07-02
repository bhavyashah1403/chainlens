import time
from datetime import datetime, time as dt_time, timedelta
from collections import defaultdict
import os
import json
import math
import redis
import pandas as pd
from py_vollib_vectorized import vectorized_implied_volatility
from py_vollib_vectorized.greeks import delta, gamma, vega, theta
from scipy.interpolate import CubicSpline
import logging
import re
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import cProfile
import pstats
from functools import wraps
import numpy as np
from flask import Flask, request, jsonify
import threading

# Try to import msgpack for faster serialization
try:
    import msgpack
    MSGPACK_AVAILABLE = True
    logger_serialization_lib = "msgpack"
except ImportError:
    MSGPACK_AVAILABLE = False
    logger_serialization_lib = "json"

# Try to import ujson for faster JSON serialization (fallback)
try:
    import ujson
    JSON_DUMPS = ujson.dumps
    JSON_LOADS = ujson.loads
    logger_json_lib = "ujson"
except ImportError:
    JSON_DUMPS = json.dumps
    JSON_LOADS = json.loads
    logger_json_lib = "json"

# Configure main logging (console only)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()  # Console only
    ]
)
logger = logging.getLogger(__name__)

# Configure separate snapshot logger (file only)
snapshot_logger = logging.getLogger('snapshots')
snapshot_logger.setLevel(logging.INFO)
snapshot_handler = logging.FileHandler('snapshots.log')
snapshot_formatter = logging.Formatter('%(asctime)s - %(message)s')
snapshot_handler.setFormatter(snapshot_formatter)
snapshot_logger.addHandler(snapshot_handler)
snapshot_logger.propagate = False  # Don't propagate to main logger

# Configure performance logger (file only)
perf_logger = logging.getLogger('performance')
perf_logger.setLevel(logging.INFO)
perf_handler = logging.FileHandler('performance.log')
perf_formatter = logging.Formatter('%(asctime)s - %(message)s')
perf_handler.setFormatter(perf_formatter)
perf_logger.addHandler(perf_handler)
perf_logger.propagate = False

# Configure performance summary logger (file only)
perf_summary_logger = logging.getLogger('performance_summary')
perf_summary_logger.setLevel(logging.INFO)
perf_summary_handler = logging.FileHandler('performance_summary.log')
perf_summary_formatter = logging.Formatter('%(asctime)s - %(message)s')
perf_summary_handler.setFormatter(perf_summary_formatter)
perf_summary_logger.addHandler(perf_summary_handler)
perf_summary_logger.propagate = False

# Globals
snapshots = defaultdict(lambda: defaultdict(dict))
last_update = defaultdict(lambda: defaultdict(lambda: {k: None for k in ["1m", "5m", "10m"]}))
last_redis_save = defaultdict(lambda: defaultdict(lambda: None))
app_start_time = datetime.now()  # Track when app started for 10-minute rule

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

# Redis connections with optimizations
r = redis.Redis(
    host='172.24.169.200', 
    port=6379, 
    decode_responses=False,  # Keep binary for msgpack
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30
)

redis_storage = redis.Redis(
    host='172.24.169.200', 
    port=6379, 
    db=1, 
    decode_responses=False,  # Keep binary for msgpack
    socket_connect_timeout=5,
    socket_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30
)

# Constants
rate = 0.0
market_open = dt_time(9, 15, 0)
market_close = dt_time(15, 30, 0)
full_trading_seconds = (datetime.combine(datetime.now().date(), market_close) - datetime.combine(datetime.now().date(), market_open)).seconds

intervals = {
    "1m": 60,
    "5m": 300,
    "10m": 600
}

snapshot_root_dir = "snapshots"
config_dir = "./configs"

PROCESSING_INTERVAL = 60  # 1 minute instead of 5 seconds

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Thread lock for shared resources
data_lock = threading.Lock()

# Target symbols to process
TARGET_SYMBOLS = [
    'NIFTY','BANKNIFTY','FINNIFTY','ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK', 'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV', 'BEL', 'BHARTIARTL', 'CIPLA', 'COALINDIA', 'DRREDDY', 'EICHERMOT', 'ETERNAL', 'GRASIM', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'INDUSINDBK', 'INFY', 'ITC', 'JIOFIN', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'M&M', 'MARUTI', 'NESTLEIND', 'NTPC', 'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE', 'SHRIRAMFIN', 'SBIN', 'SUNPHARMA', 'TCS', 'TATACONSUM', 'TATAMOTORS', 'TATASTEEL', 'TECHM', 'TITAN', 'TRENT', 'ULTRACEMCO', 'WIPRO'
]

# Global variable to store holidays
HOLIDAYS_SET = set()

# Flask app for web interface - MOVE THIS TO TOP
app = Flask(__name__)

def load_holidays_from_csv():
    """Load holidays from HolidayMaster.csv and return as set of dates"""
    try:
        holidays_file = "HolidayMaster.csv"
        if not os.path.exists(holidays_file):
            logger.warning(f"HolidayMaster.csv not found. TTE calculation will not exclude holidays.")
            return set()
        
        # Read the CSV file
        df = pd.read_csv(holidays_file)
        
        # The CSV has all data in a single column, we need to parse it
        holidays = set()
        
        for index, row in df.iterrows():
            try:
                # Get the first (and likely only) column value
                row_data = str(row.iloc[0]) if len(row) > 0 else ""
                
                # Split by spaces to extract date part
                parts = row_data.split()
                if len(parts) >= 2:
                    # Extract date part (should be in format like "01/01/2023")
                    date_part = parts[1]  # Skip the first column (nMarketSegmentId)
                    
                    # Parse the date
                    if '/' in date_part:
                        holiday_date = datetime.strptime(date_part, "%d/%m/%Y").date()
                        holidays.add(holiday_date)
                        
            except Exception as e:
                logger.debug(f"Could not parse holiday row {index}: {row_data}, error: {e}")
                continue
        
        logger.info(f"Loaded {len(holidays)} holidays from HolidayMaster.csv")
        if holidays:
            # Log first few holidays for verification
            sorted_holidays = sorted(list(holidays))
            logger.info(f"Sample holidays: {sorted_holidays[:5]}")
        
        return holidays
        
    except Exception as e:
        logger.error(f"Error loading holidays from CSV: {e}")
        return set()

def test_flask_startup():
    """Test if Flask can start properly"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 5000))
        sock.close()
        return result == 0
    except:
        return False

# Add this right after app initialization
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({
        "error": "Not Found",
        "message": "The requested URL was not found on the server",
        "available_endpoints": [
            "/",
            "/api/snapshot",
            "/api/symbols", 
            "/api/intervals",
            "/api/health",
            "/test",
            "/docs",
            "/web"
        ],
        "example": "/api/snapshot?symbol=RELIANCE&interval=1m&expiry=27122024"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal Server Error",
        "message": "An internal server error occurred",
        "timestamp": datetime.now().isoformat()
    }), 500

# Root route - ADD THIS IMMEDIATELY AFTER ERROR HANDLERS
@app.route('/')
def index():
    """Root endpoint with API documentation"""
    return jsonify({
        "message": "Options Chain Processor API",
        "version": "1.0.0",
        "status": "running",
        "uptime_seconds": (datetime.now() - app_start_time).total_seconds(),
        "holidays_loaded": len(HOLIDAYS_SET),
        "endpoints": {
            "GET /": "This documentation",
            "GET /api/snapshot": "Get snapshot data - ?symbol=SYMBOL&interval=INTERVAL&expiry=DDMMYYYY",
            "GET /api/symbols": "Get list of available symbols",
            "GET /api/intervals": "Get list of available intervals",
            "GET /api/health": "Health check",
            "GET /test": "Test endpoint",
            "GET /docs": "Detailed documentation",
            "GET /web": "Web interface"
        },
        "example_urls": {
            "snapshot": "/api/snapshot?symbol=RELIANCE&interval=1m&expiry=27122024",
            "symbols": "/api/symbols",
            "intervals": "/api/intervals",
            "health": "/api/health"
        },
        "parameters": {
            "symbol": f"One of: {TARGET_SYMBOLS[:10]}... (total {len(TARGET_SYMBOLS)} symbols)",
            "interval": "One of: 1m, 5m, 10m",
            "expiry": "Format: DDMMYYYY (e.g., 27122024)"
        }
    })

@app.route('/test')
def test_endpoint():
    """Test endpoint to verify API is working"""
    try:
        return jsonify({
            "status": "API is working perfectly!",
            "timestamp": datetime.now().isoformat(),
            "system_info": {
                "serialization": logger_serialization_lib,
                "json_lib": logger_json_lib,
                "uptime_seconds": (datetime.now() - app_start_time).total_seconds(),
                "target_symbols_count": len(TARGET_SYMBOLS),
                "holidays_loaded": len(HOLIDAYS_SET)
            },
            "quick_test_urls": [
                "/api/symbols",
                "/api/intervals", 
                "/api/health"
            ],
            "message": "If you see this, the API is working correctly!"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime_seconds": (datetime.now() - app_start_time).total_seconds(),
        "serialization": logger_serialization_lib,
        "json_lib": logger_json_lib,
        "target_symbols": len(TARGET_SYMBOLS),
        "holidays_loaded": len(HOLIDAYS_SET)
    })

@app.route('/api/symbols')
def get_symbols():
    """Get list of available symbols"""
    return jsonify({
        "symbols": TARGET_SYMBOLS,
        "count": len(TARGET_SYMBOLS)
    })

@app.route('/api/intervals')
def get_intervals():
    """Get list of available intervals"""
    return jsonify({
        "intervals": list(intervals.keys()),
        "descriptions": {
            "1m": "1 minute interval",
            "5m": "5 minute interval", 
            "10m": "10 minute interval"
        }
    })

@app.route('/docs')
def docs():
    """Detailed API documentation"""
    return jsonify({
        "api_documentation": {
            "title": "Options Chain Processor API Documentation",
            "version": "1.0.0",
            "description": "Real-time options chain data with dividend adjustments and holiday exclusions",
            "base_url": request.host_url,
            "endpoints": {
                "/": {
                    "method": "GET",
                    "description": "API overview and quick start",
                    "response": "JSON with API information"
                },
                "/api/snapshot": {
                    "method": "GET",
                    "description": "Get options chain snapshot data",
                    "parameters": {
                        "symbol": {
                            "required": True,
                            "type": "string",
                            "description": "Stock symbol (uppercase)",
                            "example": "RELIANCE"
                        },
                        "interval": {
                            "required": True,
                            "type": "string",
                            "default": "1m",
                            "description": "Data interval",
                            "valid_values": list(intervals.keys())
                        },
                        "expiry": {
                            "required": True,
                            "type": "string",
                            "format": "DDMMYYYY",
                            "description": "Option expiry date",
                            "example": "27122024"
                        }
                    },
                    "example_request": f"{request.host_url}api/snapshot?symbol=RELIANCE&interval=1m&expiry=27122024"
                }
            }
        }
    })

@app.route('/web')
def web_interface():
    """Simple web interface"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Options Chain API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; }
            h1 { color: #333; }
            .endpoint { background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .test-btn { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
            .result { background: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 15px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Options Chain Processor API</h1>
            <p><strong>Status:</strong> Running ✅</p>
            <p><strong>Holidays Loaded:</strong> """ + str(len(HOLIDAYS_SET)) + """</p>
            
            <h2>Quick Tests</h2>
            <button class="test-btn" onclick="testEndpoint('/api/health')">Test Health</button>
            <button class="test-btn" onclick="testEndpoint('/api/symbols')">Get Symbols</button>
            <button class="test-btn" onclick="testEndpoint('/api/intervals')">Get Intervals</button>
            <button class="test-btn" onclick="testEndpoint('/test')">Test API</button>
            
            <div id="result" class="result" style="display: none;"></div>
            
            <h2>Available Endpoints</h2>
            <div class="endpoint">
                <strong>GET /</strong> - API Documentation
            </div>
            <div class="endpoint">
                <strong>GET /api/snapshot</strong> - Get options data<br>
                Example: /api/snapshot?symbol=RELIANCE&interval=1m&expiry=27122024
            </div>
            <div class="endpoint">
                <strong>GET /api/symbols</strong> - List all symbols
            </div>
            <div class="endpoint">
                <strong>GET /api/intervals</strong> - List all intervals
            </div>
            <div class="endpoint">
                <strong>GET /api/health</strong> - Health check
            </div>
        </div>
        
        <script>
            function testEndpoint(url) {
                const resultDiv = document.getElementById('result');
                resultDiv.style.display = 'block';
                resultDiv.innerHTML = 'Loading...';
                
                fetch(url)
                    .then(response => response.json())
                    .then(data => {
                        resultDiv.innerHTML = '<h3>Response from ' + url + '</h3><pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    })
                    .catch(error => {
                        resultDiv.innerHTML = '<h3>Error</h3><p>' + error.message + '</p>';
                    });
            }
        </script>
    </body>
    </html>
    """
    return html_content

# Move the snapshot endpoint here too
@app.route('/api/snapshot')
def get_snapshot():
    """
    API endpoint to get snapshot data
    URL format: /api/snapshot?symbol=RELIANCE&interval=1m&expiry=27122024
    """
    try:
        symbol = request.args.get('symbol', '').upper()
        interval = request.args.get('interval', '1m')
        expiry = request.args.get('expiry', '')
        
        # Validate parameters
        if not symbol:
            return jsonify({"error": "Symbol parameter is required"}), 400
        
        if interval not in intervals:
            return jsonify({"error": f"Invalid interval. Must be one of: {list(intervals.keys())}"}), 400
        
        if not expiry:
            return jsonify({"error": "Expiry parameter is required (format: DDMMYYYY)"}), 400
        
        # Validate expiry format
        try:
            datetime.strptime(expiry, "%d%m%Y")
        except ValueError:
            return jsonify({"error": "Invalid expiry format. Use DDMMYYYY (e.g., 27122024)"}), 400
        
        # Check if symbol is in target symbols
        if symbol not in TARGET_SYMBOLS:
            return jsonify({"error": f"Symbol {symbol} not in target symbols list"}), 404
        
        # Get snapshot data
        with data_lock:
            snapshot_data = snapshots.get(symbol, {}).get(expiry, {}).get(interval)
        
        if not snapshot_data:
            return jsonify({"error": f"No data found for {symbol} {expiry} {interval}"}), 404
        
        return jsonify(snapshot_data)
        
    except Exception as e:
        logger.error(f"Error in get_snapshot API: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Performance profiling decorator
def profile_time(func_name):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            perf_logger.info(f"{func_name}: {duration:.4f} seconds")
            if duration > 1.0:  # Log to console if > 1 second
                logger.info(f"PERFORMANCE: {func_name} took {duration:.2f} seconds")
            return result
        return wrapper
    return decorator

def optimized_serialize(data):
    """Use msgpack if available, otherwise ujson/json"""
    if MSGPACK_AVAILABLE:
        return msgpack.packb(data, use_bin_type=True)
    else:
        return JSON_DUMPS(data).encode('utf-8')

def optimized_deserialize(data):
    """Use msgpack if available, otherwise ujson/json"""
    if MSGPACK_AVAILABLE:
        return msgpack.unpackb(data, raw=False)
    else:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return JSON_LOADS(data)

def load_json(json_path):
    try:
        with open(json_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON {json_path}: {e}")
        raise

def load_strike_gaps():
    """Load strike gaps configuration"""
    try:
        strike_gaps_path = "strikegaps.json"
        return load_json(strike_gaps_path)
    except Exception as e:
        logger.error(f"Failed to load strike gaps: {e}")
        return {}

def load_underlying_tokens():
    """Load underlying tokens configuration"""
    try:
        underlying_tokens_path = "underlyingtoken.json"
        return load_json(underlying_tokens_path)
    except Exception as e:
        logger.error(f"Failed to load underlying tokens: {e}")
        return {}

# Load configurations once at startup
logger.info("Loading configurations...")
STRIKE_GAPS = load_strike_gaps()
UNDERLYING_TOKENS = load_underlying_tokens()
HOLIDAYS_SET = load_holidays_from_csv()  # Load holidays
logger.info(f"Configurations loaded successfully (JSON: {logger_json_lib}, Serialization: {logger_serialization_lib}, Holidays: {len(HOLIDAYS_SET)})")

def get_strike_gap(symbol):
    """Get strike gap for a symbol, default to 50 if not found"""
    return STRIKE_GAPS.get(symbol.upper(), 50)

def get_underlying_token(symbol):
    """Get underlying token for a symbol"""
    return UNDERLYING_TOKENS.get(symbol.upper())

@profile_time("parse_dividend_amount")
def parse_dividend_amount(s_remarks):
    """
    Parse dividend amount from sRemarks string
    Examples:
    - "INTDIV - RS 2 PER SHARE" -> 2.0
    - "DIV-RE 0.50 PER SHARE" -> 0.50
    - "AGM/DIV - RS 3 PER SHARE" -> 3.0
    - "AGM/DIV RS 9.50 PER SHARE" -> 9.50
    """
    try:
        if not s_remarks or s_remarks == "N/A":
            return 0.0
        
        # Convert to uppercase for consistent matching
        remarks_upper = str(s_remarks).upper()
        
        # Pattern to match dividend amounts
        # Matches: RS 2, RE 0.50, RS 9.50, etc.
        patterns = [
            r'RS\s+(\d+\.?\d*)\s+PER\s+SHARE',
            r'RE\s+(\d+\.?\d*)\s+PER\s+SHARE',
            r'RS\s+(\d+\.?\d*)',
            r'RE\s+(\d+\.?\d*)',
            r'(\d+\.?\d*)\s+PER\s+SHARE'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, remarks_upper)
            if match:
                dividend_amount = float(match.group(1))
                logger.debug(f"Parsed dividend amount {dividend_amount} from '{s_remarks}'")
                return dividend_amount
        
        logger.warning(f"Could not parse dividend amount from: '{s_remarks}'")
        return 0.0
        
    except Exception as e:
        logger.error(f"Error parsing dividend amount from '{s_remarks}': {e}")
        return 0.0

@profile_time("process_dividend_data")
def process_dividend_data(data, symbol, expiry_date):
    """
    Process dividend data for a symbol and expiry
    Returns: (dividend_amount, ex_date_ist, is_applicable)
    """
    try:
        # Find dividend data for the symbol in NSE FO data
        header = data[0]
        rows = data[1:]
        
        # Find required column indices
        try:
            symbol_idx = header.index("sSymbol")
            dividend_idx = header.index("nDividend") if "nDividend" in header else None
            remarks_idx = header.index("sRemarks") if "sRemarks" in header else None
            ex_date_idx = header.index("nExDate") if "nExDate" in header else None
        except ValueError as e:
            logger.warning(f"Dividend columns not found in data: {e}")
            return 0.0, None, False
        
        if dividend_idx is None or remarks_idx is None or ex_date_idx is None:
            logger.warning(f"Required dividend columns missing for {symbol}")
            return 0.0, None, False
        
        # Find dividend data for this symbol
        dividend_amount = 0.0
        ex_date_ist = None
        is_applicable = False
        
        for row in rows:
            if row[symbol_idx] == symbol.upper():
                n_dividend = row[dividend_idx]
                s_remarks = row[remarks_idx]
                n_ex_date = row[ex_date_idx]
                
                # Check if dividend exists (nDividend = 1)
                if str(n_dividend) == "1" and s_remarks and n_ex_date:
                    try:
                        # Parse dividend amount
                        dividend_amount = parse_dividend_amount(s_remarks)
                        
                        if dividend_amount > 0:
                            # Convert ex-date (add 10 years and convert to IST)
                            ex_timestamp = int(n_ex_date) + (10 * 365 * 24 * 3600)  # Add 10 years
                            ex_date_utc = datetime.fromtimestamp(ex_timestamp, tz=pytz.UTC)
                            ex_date_ist = ex_date_utc.astimezone(IST)
                            
                            # Check if ex-date is before expiry
                            expiry_dt = datetime.strptime(expiry_date, "%d%m%Y")
                            expiry_ist = IST.localize(expiry_dt)
                            
                            if ex_date_ist < expiry_ist:
                                is_applicable = True
                                logger.info(f"DIVIDEND APPLICABLE: {symbol} - Amount: {dividend_amount}, Ex-Date: {ex_date_ist.strftime('%Y-%m-%d')}, Expiry: {expiry_ist.strftime('%Y-%m-%d')}")
                            else:
                                logger.info(f"DIVIDEND NOT APPLICABLE: {symbol} - Ex-Date {ex_date_ist.strftime('%Y-%m-%d')} is after expiry {expiry_ist.strftime('%Y-%m-%d')}")
                            
                            break
                            
                    except Exception as e:
                        logger.error(f"Error processing dividend data for {symbol}: {e}")
                        continue
        
        return dividend_amount, ex_date_ist, is_applicable
        
    except Exception as e:
        logger.error(f"Error in process_dividend_data for {symbol}: {e}")
        return 0.0, None, False

@profile_time("filter_option_data")
def filter_option_data(data, symbol):
    try:
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        df_filtered = df[df["sSymbol"] == symbol.upper()].copy()
        df_filtered["nStrikePrice"] = df_filtered["nStrikePrice"].astype(float) / 100
        df_filtered["id"] = df_filtered["nMarketSegmentId"].astype(str) + "_" + df_filtered["nToken"].astype(str)
        df_filtered = df_filtered[["id", "ExpiryDate", "nStrikePrice", "sOptionType"]]
        df_filtered = df_filtered.rename(columns={
            "id": "id",
            "ExpiryDate": "expirydate",
            "nStrikePrice": "strikeprice",
            "sOptionType": "type"
        })
        return df_filtered
    except Exception as e:
        logger.error(f"Error in filter_option_data for symbol {symbol}: {e}")
        raise

@profile_time("filter_all_symbols_data")
def filter_all_symbols_data(data, symbols):
    """HEAVILY OPTIMIZED: Filter data for all symbols at once"""
    try:
        start_time = time.time()
        
        # Step 1: Extract header and rows (minimal overhead)
        step_start = time.time()
        header = data[0]
        rows = data[1:]
        perf_logger.info(f"  Header/rows extraction: {time.time() - step_start:.4f} seconds")
        
        # Step 2: Create symbol lookup set (O(1) lookups instead of O(n))
        step_start = time.time()
        target_symbols_set = set(s.upper() for s in symbols)
        perf_logger.info(f"  Symbol set creation: {time.time() - step_start:.4f} seconds")
        
        # Step 3: Find required column indices once (avoid repeated lookups)
        step_start = time.time()
        try:
            symbol_idx = header.index("sSymbol")
            strike_idx = header.index("nStrikePrice") 
            segment_idx = header.index("nMarketSegmentId")
            token_idx = header.index("nToken")
            expiry_idx = header.index("ExpiryDate")
            option_type_idx = header.index("sOptionType")
        except ValueError as e:
            logger.error(f"Required column not found in header: {e}")
            raise
        perf_logger.info(f"  Column index lookup: {time.time() - step_start:.4f} seconds")
        
        # Step 4: Filter rows using list comprehension (much faster than pandas)
        step_start = time.time()
        logger.info(f"Filtering {len(rows)} rows for symbols: {symbols}")
        
        filtered_rows = []
        for row in rows:
            if row[symbol_idx] in target_symbols_set:
                # Pre-calculate values to avoid repeated operations
                strike_price = float(row[strike_idx]) / 100
                row_id = f"{row[segment_idx]}_{row[token_idx]}"
                
                filtered_rows.append({
                    "id": row_id,
                    "expirydate": row[expiry_idx],
                    "strikeprice": strike_price,
                    "type": row[option_type_idx],
                    "symbol": row[symbol_idx]
                })
        
        filter_time = time.time() - step_start
        logger.info(f"Filtered to {len(filtered_rows)} rows in {filter_time:.2f} seconds")
        perf_logger.info(f"  Row filtering: {filter_time:.4f} seconds")
        
        # Step 5: Create DataFrame from filtered dict (faster than filtering after DataFrame creation)
        step_start = time.time()
        df_filtered = pd.DataFrame(filtered_rows)
        df_creation_time = time.time() - step_start  
        perf_logger.info(f"  DataFrame creation: {df_creation_time:.4f} seconds")
        
        total_time = time.time() - start_time
        logger.info(f"OPTIMIZED: Filtered all symbols data in {total_time:.2f} seconds")
        
        # Log optimization breakdown
        perf_logger.info(f"*** FILTER OPTIMIZATION BREAKDOWN ***")
        perf_logger.info(f"  Input rows: {len(rows)}")
        perf_logger.info(f"  Output rows: {len(filtered_rows)}")
        perf_logger.info(f"  Filtering efficiency: {len(filtered_rows)/len(rows)*100:.2f}%")
        perf_logger.info(f"  Processing rate: {len(rows)/total_time:.0f} rows/second")
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error in filter_all_symbols_data: {e}")
        raise

@profile_time("subset_for_nearest_expiry")
def subset_for_nearest_expiry(df, nearest_date):
    try:
        target = datetime.strptime(nearest_date, "%d%m%Y").strftime("%d-%m-%Y")
        return df[df["expirydate"] == target]
    except Exception as e:
        logger.error(f"Error in subset_for_nearest_expiry for date {nearest_date}: {e}")
        raise

@profile_time("pivot_calls_and_puts")
def pivot_calls_and_puts(df):
    try:
        calls = df[df["type"] == "CE"].copy()
        puts = df[df["type"] == "PE"].copy()
        calls = calls[["id", "expirydate", "strikeprice", "type"]].rename(columns={
            "id": "call_id", "type": "call_type"
        })
        puts = puts[["id", "expirydate", "strikeprice", "type"]].rename(columns={
            "id": "put_id", "type": "put_type"
        })
        merged = pd.merge(calls, puts, on=["strikeprice", "expirydate"], how="inner")
        return merged[["call_id", "call_type", "strikeprice", "expirydate", "put_type", "put_id"]]
    except Exception as e:
        logger.error(f"Error in pivot_calls_and_puts: {e}")
        raise

def get_latest_tick_optimized(redis_conn, key):
    """OPTIMIZED: Handle both msgpack and JSON format from Redis"""
    try:
        value = redis_conn.get(key)
        if not value:
            return {"ltp": "N/A", "volume": "N/A", "oi": "N/A", "bidprice": "N/A", "bidquant": "N/A", "askprice": "N/A", "askquant": "N/A"}
        
        # Try to deserialize with optimized method
        try:
            fields = optimized_deserialize(value)
        except:
            # Fallback to JSON if msgpack fails
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            fields = JSON_LOADS(value)
        
        def safe_div(val, divisor, as_type):
            try:
                if val == "0" or val == 0:
                    return 0 if as_type == int else 0.0
                return as_type(val) / divisor
            except:
                return "N/A"
        
        return {
            "ltp": safe_div(fields.get("nLastTradedPrice", "N/A"), 100, float),
            "volume": safe_div(fields.get("nTotalQuantityTraded", "N/A"), 1, int),
            "oi": safe_div(fields.get("nOpenInterest", "N/A"), 1, float),
            "bidprice": safe_div(fields.get("nBestFiveBuyPrice1", "N/A"), 100, float),
            "bidquant": safe_div(fields.get("nBestFiveBuyQuantity1", "N/A"), 1, int),
            "askprice": safe_div(fields.get("nBestFiveSellPrice1", "N/A"), 100, float),
            "askquant": safe_div(fields.get("nBestFiveSellQuantity1", "N/A"), 1, int),
        }
    except Exception as e:
        logger.error(f"Error in get_latest_tick_optimized for key {key}: {e}")
        return {"ltp": "ERR", "volume": "ERR", "oi": "ERR", "bidprice": "ERR", "bidquant": "ERR", "askprice": "ERR", "askquant": "ERR"}

@profile_time("get_latest_ticks_batch_optimized")
def get_latest_ticks_batch_optimized(redis_conn, keys):
    """ULTRA OPTIMIZED: Batch fetch with msgpack support and connection pooling"""
    try:
        if not keys:
            return []
        
        # Use pipeline for batch operations with larger pipeline size
        pipe = redis_conn.pipeline(transaction=False)  # Disable transactions for speed
        for key in keys:
            pipe.get(key)
        
        values = pipe.execute()
        
        results = []
        for i, value in enumerate(values):
            if not value:
                results.append({"ltp": "N/A", "volume": "N/A", "oi": "N/A", "bidprice": "N/A", "bidquant": "N/A", "askprice": "N/A", "askquant": "N/A"})
                continue
            
            try:
                # Try optimized deserialization first
                try:
                    fields = optimized_deserialize(value)
                except:
                    # Fallback to JSON
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                    fields = JSON_LOADS(value)
                
                def safe_div(val, divisor, as_type):
                    try:
                        if val == "0" or val == 0:
                            return 0 if as_type == int else 0.0
                        return as_type(val) / divisor
                    except:
                        return "N/A"
                
                results.append({
                    "ltp": safe_div(fields.get("nLastTradedPrice", "N/A"), 100, float),
                    "volume": safe_div(fields.get("nTotalQuantityTraded", "N/A"), 1, int),
                    "oi": safe_div(fields.get("nOpenInterest", "N/A"), 1, float),
                    "bidprice": safe_div(fields.get("nBestFiveBuyPrice1", "N/A"), 100, float),
                    "bidquant": safe_div(fields.get("nBestFiveBuyQuantity1", "N/A"), 1, int),
                    "askprice": safe_div(fields.get("nBestFiveSellPrice1", "N/A"), 100, float),
                    "askquant": safe_div(fields.get("nBestFiveSellQuantity1", "N/A"), 1, int),
                })
            except Exception as e:
                logger.error(f"Error parsing data for key {keys[i]}: {e}")
                results.append({"ltp": "ERR", "volume": "ERR", "oi": "ERR", "bidprice": "ERR", "bidquant": "ERR", "askprice": "ERR", "askquant": "ERR"})
        
        return results
        
    except Exception as e:
        logger.error(f"Error in optimized batch Redis fetch: {e}")
        # Fallback to individual fetches
        return [get_latest_tick_optimized(redis_conn, key) for key in keys]

def get_underlying_atm(redis_conn, symbol):
    """Get ATM price for any symbol using underlying token from config - FLOAT SUPPORT"""
    try:
        underlying_token = get_underlying_token(symbol)
        if not underlying_token:
            logger.warning(f"No underlying token found for symbol {symbol}")
            return None
            
        value = redis_conn.lindex(underlying_token, -1)
        if not value:
            return None
        
        # Handle bytes from Redis
        if isinstance(value, bytes):
            value = value.decode('utf-8')
            
        fields = dict(item.split('=') for item in value.strip('|').split('|') if '=' in item)
        raw_ltp = fields.get("8", "N/A")
        if raw_ltp == "N/A":
            return None
            
        ltp = float(raw_ltp) / 100
        strike_gap = float(get_strike_gap(symbol))  # Ensure float
        
        # IMPROVED: Handle fractional strike gaps properly
        if strike_gap >= 1.0:
            # For gaps >= 1, use standard rounding
            return round(ltp / strike_gap) * strike_gap
        else:
            # For fractional gaps (like 0.5, 2.5), use proper decimal rounding
            # Round to nearest strike gap multiple
            multiplier = round(ltp / strike_gap)
            atm_strike = multiplier * strike_gap
            # Round to appropriate decimal places to avoid floating point errors
            decimal_places = len(str(strike_gap).split('.')[-1]) if '.' in str(strike_gap) else 0
            return round(atm_strike, decimal_places)
        
    except Exception as e:
        logger.error(f"Error in get_underlying_atm for symbol {symbol}: {e}")
        return None

def remaining_day(timestamp, market_open=market_open, market_close=market_close):
    try:
        if timestamp.weekday() in [5, 6]:
            return full_trading_seconds
        close_timestamp = datetime.combine(timestamp.date(), market_close)
        open_timestamp = datetime.combine(timestamp.date(), market_open)
        if timestamp.time() < market_open:
            return (close_timestamp - open_timestamp).total_seconds()
        elif market_open <= timestamp.time() <= market_close:
            return (close_timestamp - timestamp).total_seconds()
        return 0
    except Exception as e:
        logger.error(f"Error in remaining_day: {e}")
        raise

def calculate_trading_time_to_expiry(timestamp, exp_timestamp, holidays_set=None):
    """
    Calculate trading time to expiry excluding holidays from HolidayMaster.csv
    
    Args:
        timestamp: Current timestamp
        exp_timestamp: Expiry timestamp
        holidays_set: Set of holiday dates to exclude (uses global HOLIDAYS_SET if None)
    
    Returns:
        Trading time to expiry as fraction of year
    """
    try:
        timestamp = pd.Timestamp(timestamp)
        exp_timestamp = pd.Timestamp(exp_timestamp)
        
        if holidays_set is None:
            holidays_set = HOLIDAYS_SET
        
        # If same date, return remaining time for today
        if timestamp.date() == exp_timestamp.date():
            return remaining_day(timestamp) / (full_trading_seconds * 252)
        
        # Generate business days between start and end
        business_days = pd.bdate_range(
            start=timestamp.date(), 
            end=exp_timestamp.date(),
            freq='C', 
            weekmask='Mon Tue Wed Thu Fri'
        )
        
        # Convert to dates for comparison with holidays
        business_dates = [bd.date() for bd in business_days]
        
        # Filter out holidays
        trading_dates = [bd for bd in business_dates if bd not in holidays_set]
        
        # Subtract 1 because we don't count the start date fully
        total_trading_days = len(trading_dates) - 1
        
        # Calculate total trading seconds
        total_trading_seconds = total_trading_days * full_trading_seconds + remaining_day(timestamp)
        
        # Convert to fraction of year (252 trading days per year)
        tte = total_trading_seconds / (full_trading_seconds * 252)
        
        # Log holiday exclusions if any
        excluded_holidays = [bd for bd in business_dates if bd in holidays_set]
        if excluded_holidays:
            logger.info(f"TTE CALCULATION: Excluded {len(excluded_holidays)} holidays between {timestamp.date()} and {exp_timestamp.date()}")
            logger.debug(f"Excluded holidays: {excluded_holidays}")
        
        logger.debug(f"TTE CALCULATION: Business days: {len(business_dates)}, Trading days: {len(trading_dates)}, TTE: {tte:.6f}")
        
        return tte
        
    except Exception as e:
        logger.error(f"Error in calculate_trading_time_to_expiry: {e}")
        # Fallback to original calculation without holiday exclusion
        timestamp = pd.Timestamp(timestamp)
        exp_timestamp = pd.Timestamp(exp_timestamp)
        if timestamp.date() == exp_timestamp.date():
            return remaining_day(timestamp) / (full_trading_seconds * 252)
        total_trading_days = pd.bdate_range(start=timestamp.date(), end=exp_timestamp.date(),
                                            freq='C', weekmask='Mon Tue Wed Thu Fri').size - 1
        total_trading_seconds = total_trading_days * full_trading_seconds + remaining_day(timestamp)
        return total_trading_seconds / (full_trading_seconds * 252)

def parse_config_filename(filename):
    try:
        base = os.path.basename(filename)
        parts = base.split('_')
        if len(parts) != 3:
            return None, None
        symbol = parts[1]
        date_part = parts[2].replace('.txt', '')
        return symbol, date_part
    except Exception as e:
        logger.error(f"Error in parse_config_filename for {filename}: {e}")
        return None, None

@profile_time("get_filtered_config_files")
def get_filtered_config_files():
    """Get config files only for target symbols"""
    try:
        filtered_config_files = []
        if os.path.exists(config_dir):
            for item in os.listdir(config_dir):
                item_path = os.path.join(config_dir, item)
                if os.path.isdir(item_path) and not item.startswith('.'):
                    # Check if this symbol is in our target list
                    if item.upper() in TARGET_SYMBOLS:
                        symbol_config_dir = item_path
                        if os.path.exists(symbol_config_dir):
                            config_files = [os.path.join(symbol_config_dir, f) 
                                           for f in os.listdir(symbol_config_dir)
                                           if f.startswith("config_") and f.endswith(".txt")]
                            filtered_config_files.extend(config_files)
                            logger.info(f"Added {len(config_files)} config files for symbol: {item}")
                    else:
                        logger.info(f"Skipping symbol not in target list: {item}")
        
        logger.info(f"Found {len(filtered_config_files)} config files for target symbols: {TARGET_SYMBOLS}")
        return filtered_config_files
    except Exception as e:
        logger.error(f"Error getting filtered config files: {e}")
        return []

def load_config_tokens(config_path):
    """Load all tokens from a config file"""
    try:
        with open(config_path, 'r') as f:
            content = f.read().strip()
            if content:
                return content.split(',')
            return []
    except Exception as e:
        logger.error(f"Error loading config tokens from {config_path}: {e}")
        return []

@profile_time("filter_strikes_by_iv_range")
def filter_strikes_by_iv_range(df, synthetic_atm, tte):
    """
    Filter strikes based on IV range formula: IV * sqrt(TTE) * synthetic_price * 2
    Rounded to nearest 10 for cleaner ranges
    """
    try:
        if df.empty:
            logger.warning("DataFrame is empty, cannot filter strikes")
            return df
        
        # Calculate ATM IV (average of call and put IV at ATM strike)
        df_sorted = df.sort_values('strikeprice').reset_index(drop=True)
        
        # Find ATM strike row
        df_sorted['distance_from_atm'] = abs(df_sorted['strikeprice'] - synthetic_atm)
        atm_idx = df_sorted['distance_from_atm'].idxmin()
        atm_row = df_sorted.iloc[atm_idx]
        
        # Get ATM IV (average of call and put IV, handle N/A values)
        call_iv = atm_row['call_iv'] if atm_row['call_iv'] not in ["N/A", "ERR"] else 20.0  # Default 20%
        put_iv = atm_row['put_iv'] if atm_row['put_iv'] not in ["N/A", "ERR"] else 20.0   # Default 20%
        
        # Convert to float and handle any remaining issues
        try:
            call_iv = float(call_iv)
            put_iv = float(put_iv)
        except (ValueError, TypeError):
            call_iv = 20.0
            put_iv = 20.0
        
        # Calculate ATM IV as average of call and put IV
        atm_iv = (call_iv + put_iv) / 2
        
        # Convert IV from percentage to decimal (e.g., 50% -> 0.5)
        iv_decimal = atm_iv / 100
        
        # Calculate range using the formula: IV * sqrt(TTE) * synthetic_price * 2
        range_value = iv_decimal * np.sqrt(tte) * synthetic_atm * 2
        
        # Round to nearest 10 to make it cleaner
        range_value = round(range_value / 10) * 10
        
        # Ensure minimum range of 0 points
        range_value = max(range_value, 0)
        
        # Calculate strike bounds
        lower_bound = synthetic_atm - range_value
        upper_bound = synthetic_atm + range_value
        
        # Filter strikes within the calculated range
        filtered_df = df_sorted[
            (df_sorted['strikeprice'] >= lower_bound) & 
            (df_sorted['strikeprice'] <= upper_bound)
        ].copy()
        
        filtered_df = filtered_df.drop('distance_from_atm', axis=1)
        
        logger.info(f"IV-BASED STRIKE FILTERING:")
        logger.info(f"  Synthetic ATM: {synthetic_atm:.2f}")
        logger.info(f"  ATM IV: {atm_iv:.2f}% (Call: {call_iv:.2f}%, Put: {put_iv:.2f}%)")
        logger.info(f"  TTE: {tte:.6f}")
        logger.info(f"  Calculated range: {range_value:.0f} points")
        logger.info(f"  Strike bounds: {lower_bound:.2f} to {upper_bound:.2f}")
        logger.info(f"  Strikes filtered: {len(filtered_df)}/{len(df_sorted)} (from {df_sorted['strikeprice'].min():.2f} to {df_sorted['strikeprice'].max():.2f})")
        
        if filtered_df.empty:
            logger.warning(f"No strikes found in calculated range, using wider range")
            # Fallback: use 2x the calculated range
            fallback_range = range_value * 2
            lower_bound = synthetic_atm - fallback_range
            upper_bound = synthetic_atm + fallback_range
            
            filtered_df = df_sorted[
                (df_sorted['strikeprice'] >= lower_bound) & 
                (df_sorted['strikeprice'] <= upper_bound)
            ].copy()
            
            if not filtered_df.empty:
                filtered_df = filtered_df.drop('distance_from_atm', axis=1)
                logger.info(f"FALLBACK: Using 2x range ({fallback_range:.0f} points), found {len(filtered_df)} strikes")
            else:
                # Final fallback: return all strikes
                logger.warning("Even fallback range yielded no results, returning all strikes")
                return df_sorted.drop('distance_from_atm', axis=1).reset_index(drop=True)
        
        return filtered_df.reset_index(drop=True)
        
    except Exception as e:
        logger.error(f"Error in IV-based strike filtering: {e}")
        logger.warning("Falling back to returning all strikes")
        return df

def should_update_all_intervals(now):
    """Check if we're in the first 10 minutes after app start"""
    time_since_start = (now - app_start_time).total_seconds()
    return time_since_start <= 600  # 10 minutes = 600 seconds

@profile_time("save_all_configs_to_redis_ultra_batch_optimized")
def save_all_configs_to_redis_ultra_batch_optimized(processed_configs, now):
    """ULTRA OPTIMIZED: Vectorized data preparation + batch Redis save with msgpack"""
    try:
        mega_start = time.time()
        mega_pipe = redis_storage.pipeline(transaction=False)  # Disable transactions for speed
        total_strikes = 0
        
        ist_now = now.astimezone(IST) if now.tzinfo else IST.localize(now)
        timestamp_iso = ist_now.replace(tzinfo=None).isoformat()
        timestamp_formatted = ist_now.strftime("%M:%S:") + f"{ist_now.microsecond // 1000:03d}"
        
        logger.info(f"ULTRA BATCH OPTIMIZED: Preparing {len(processed_configs)} configs for vectorized Redis save...")
        
        prep_start = time.time()
        
        # Pre-allocate lists for better performance
        redis_operations = []
        
        # Process all configs with vectorized operations
        for config_result in processed_configs:
            try:
                symbol = config_result['symbol']
                expiry_date = config_result['expiry_date']
                df = config_result['df_processed']
                dividend_amount = config_result.get('dividend_amount', 0.0)
                
                # VECTORIZED: Create all Redis keys at once
                strike_keys = df['strikeprice'].apply(
                    lambda x: str(int(x)) if x == int(x) else f"{x:.2f}".replace('.', '_')
                )
                redis_keys = f"{symbol}_{expiry_date}_" + strike_keys
                
                # VECTORIZED: Create base metadata for all strikes (avoid repeated dict creation)
                base_metadata = {
                    "timestamp": timestamp_iso,
                    "timestamp_formatted": timestamp_formatted,
                    "timezone": "IST",
                    "symbol": symbol,
                    "expiry_date": expiry_date,
                    "dividend": dividend_amount  # Add dividend to metadata
                }
                
                # OPTIMIZED: Process all strikes with minimal dict operations
                for idx, row in df.iterrows():
                    redis_key = redis_keys.iloc[idx]
                    
                    # Pre-build the data structure efficiently
                    strike_data = {
                        **base_metadata,
                        "strike_price": float(row['strikeprice']),
                        "tte": float(row.get('tte', 0)),
                        "synthetic_price": float(row.get('synthetic', 0)),
                        "call": {
                            "id": str(row['call_id']),
                            "type": str(row['call_type']),
                            "ltp": float(row['call_ltp']) if row['call_ltp'] != "N/A" else None,
                            "iv": float(row['call_iv']) if row['call_iv'] != "N/A" else None,
                            "delta": float(row['call_delta']) if row['call_delta'] != "N/A" else None,
                            "gamma": float(row['call_gamma']) if row['call_gamma'] != "N/A" else None,
                            "vega": float(row['call_vega']) if row['call_vega'] != "N/A" else None,
                            "theta": float(row['call_theta']) if row['call_theta'] != "N/A" else None,
                            "oi": float(row['call_oi']) if row['call_oi'] != "N/A" else None,
                            "volume": float(row['call_volume']) if row['call_volume'] != "N/A" else None,
                            "bid_price": float(row['call_bidprice']) if row['call_bidprice'] != "N/A" else None,
                            "ask_price": float(row['call_askprice']) if row['call_askprice'] != "N/A" else None,
                            "bid_qty": int(row['call_bidquant']) if row['call_bidquant'] != "N/A" else None,
                            "ask_qty": int(row['call_askquant']) if row['call_askquant'] != "N/A" else None,
                            "moneyness": float(row['call_moneyness']) if row['call_moneyness'] != "N/A" else None
                        },
                        "put": {
                            "id": str(row['put_id']),
                            "type": str(row['put_type']),
                            "ltp": float(row['put_ltp']) if row['put_ltp'] != "N/A" else None,
                            "iv": float(row['put_iv']) if row['put_iv'] != "N/A" else None,
                            "delta": float(row['put_delta']) if row['put_delta'] != "N/A" else None,
                            "gamma": float(row['put_gamma']) if row['put_gamma'] != "N/A" else None,
                            "vega": float(row['put_vega']) if row['put_vega'] != "N/A" else None,
                            "theta": float(row['put_theta']) if row['put_theta'] != "N/A" else None,
                            "oi": float(row['put_oi']) if row['put_oi'] != "N/A" else None,
                            "volume": float(row['put_volume']) if row['put_volume'] != "N/A" else None,
                            "bid_price": float(row['put_bidprice']) if row['put_bidprice'] != "N/A" else None,
                            "ask_price": float(row['put_askprice']) if row['put_askprice'] != "N/A" else None,
                            "bid_qty": int(row['put_bidquant']) if row['put_bidquant'] != "N/A" else None,
                            "ask_qty": int(row['put_askquant']) if row['put_askquant'] != "N/A" else None,
                            "moneyness": float(row['put_moneyness']) if row['put_moneyness'] != "N/A" else None
                        }
                    }
                    
                    # Use optimized serialization (msgpack if available)
                    serialized_data = optimized_serialize(strike_data)
                    mega_pipe.setex(redis_key, 3600, serialized_data)
                    total_strikes += 1
                    
            except Exception as e:
                logger.error(f"Error preparing config for ultra batch: {e}")
                continue
        
        prep_time = time.time() - prep_start
        
        # Execute pipeline
        execute_start = time.time()
        mega_pipe.execute()
        execute_time = time.time() - execute_start
        
        total_time = time.time() - mega_start
        
        logger.info(f"*** ULTRA BATCH REDIS SAVE OPTIMIZED COMPLETE ***")
        logger.info(f"  Total strikes saved: {total_strikes}")
        logger.info(f"  Data preparation: {prep_time:.3f}s")
        logger.info(f"  Pipeline execution: {execute_time:.3f}s")
        logger.info(f"  TOTAL TIME: {total_time:.3f}s")
        logger.info(f"  Throughput: {total_strikes/total_time:.0f} strikes/second")
        logger.info(f"  Serialization: {logger_serialization_lib}")
        
        # Log to performance summary
        perf_summary_logger.info(f"ULTRA_REDIS_SAVE_OPT - Total: {total_time:.3f}s, Prep: {prep_time:.3f}s, Execute: {execute_time:.3f}s, Strikes: {total_strikes}, Throughput: {total_strikes/total_time:.0f}/s, Serialization: {logger_serialization_lib}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in ultra batch Redis save optimized: {e}")
        return False

@profile_time("mega_iv_calculation_with_dividends")
def mega_iv_calculation_with_dividends(all_configs_data, now, nse_data):
    """MEGA IV calculation with dividend adjustment support"""
    try:
        mega_start = time.time()
        
        # Step 1: Process dividends for all configs
        dividend_start = time.time()
        logger.info("Processing dividends for all configs...")
        
        for config_data in all_configs_data:
            symbol = config_data['symbol']
            expiry_date = config_data['expiry_date']
            
            # Process dividend data
            dividend_amount, ex_date_ist, is_applicable = process_dividend_data(nse_data, symbol, expiry_date)
            
            # Store dividend info in config
            config_data['dividend_amount'] = dividend_amount if is_applicable else 0.0
            config_data['dividend_ex_date'] = ex_date_ist
            config_data['dividend_applicable'] = is_applicable
            
            # ENSURE SYNTHETIC PRICE IS ADJUSTED BY SUBTRACTING DIVIDEND
            if is_applicable and dividend_amount > 0:
                df = config_data['df_final']
                original_synthetic = df['synthetic'].iloc[0] if not df.empty else 0
                adjusted_synthetic = original_synthetic - dividend_amount
                
                # Update synthetic price in DataFrame 
                df['synthetic'] = adjusted_synthetic
                logger.info(f"DIVIDEND ADJUSTMENT APPLIED: {symbol} - Original synthetic: {original_synthetic:.2f}, Adjusted: {adjusted_synthetic:.2f}, Dividend: {dividend_amount}")
            else:
                logger.info(f"NO DIVIDEND ADJUSTMENT: {symbol} - Amount: {dividend_amount}")
        
        dividend_time = time.time() - dividend_start
        logger.info(f"Dividend processing completed in {dividend_time:.2f}s")
        
        # Step 2: Combine all DataFrames into one mega DataFrame
        combine_start = time.time()
        mega_df_list = []
        config_indices = {}  # Track which rows belong to which config
        current_index = 0
        
        for i, config_data in enumerate(all_configs_data):
            df = config_data['df_final']
            if df is not None and not df.empty:
                # Add config identifier
                df = df.copy()
                df['config_id'] = i
                mega_df_list.append(df)
                
                # Track indices for splitting back later
                config_indices[i] = {
                    'start': current_index,
                    'end': current_index + len(df),
                    'symbol': config_data['symbol'],
                    'expiry_date': config_data['expiry_date'],
                    'dividend_amount': config_data.get('dividend_amount', 0.0)
                }
                current_index += len(df)
        
        if not mega_df_list:
            logger.warning("No valid DataFrames to process in mega IV calculation")
            return []
        
        mega_df = pd.concat(mega_df_list, ignore_index=True)
        combine_time = time.time() - combine_start
        logger.info(f"MEGA IV: Combined {len(all_configs_data)} configs into mega DataFrame with {len(mega_df)} total rows in {combine_time:.2f}s")
        
        # Step 3: Data cleaning
        clean_start = time.time()
        original_len = len(mega_df)
        
        # Only remove rows where essential data is missing (not LTP=0)
        essential_cols = ['call_bidprice', 'call_askprice', 'put_bidprice', 'put_askprice', 
                         'call_bidquant', 'call_askquant', 'put_bidquant', 'put_askquant']
        
        # Create mask for rows with essential missing data
        essential_missing_mask = mega_df[essential_cols].isin(["N/A", "ERR"]).any(axis=1)
        
        # Keep rows even if LTP is 0 or "N/A" - we'll handle this in price selection
        mega_df = mega_df[~essential_missing_mask].reset_index(drop=True)
        
        clean_time = time.time() - clean_start
        logger.info(f"MEGA IV: Cleaned data - removed {original_len - len(mega_df)} rows with essential missing data (kept rows with LTP=0) in {clean_time:.2f}s")
        
        if mega_df.empty:
            logger.warning("Mega DataFrame is empty after cleaning")
            return []
        
        # Step 4: Price selection logic
        price_start = time.time()
        
        # Calculate weighted mid prices
        call_weighted_mid = (
            (mega_df["call_bidprice"] * mega_df["call_bidquant"] + mega_df["call_askprice"] * mega_df["call_askquant"]) /
            (mega_df["call_bidquant"] + mega_df["call_askquant"]).replace(0, float("nan"))
        )
        put_weighted_mid = (
            (mega_df["put_bidprice"] * mega_df["put_bidquant"] + mega_df["put_askprice"] * mega_df["put_askquant"]) /
            (mega_df["put_bidquant"] + mega_df["put_askquant"]).replace(0, float("nan"))
        )
        
        # For far OTM options (>500 points from ATM), use weighted mid instead of LTP
        far_otm_mask = (mega_df["strikeprice"] - mega_df["synthetic_atm"]).abs() > 0
        
        # Initialize with LTP
        call_chosen_price = mega_df["call_ltp"].copy()
        put_chosen_price = mega_df["put_ltp"].copy()
        
        # For far OTM, use weighted mid if available
        call_chosen_price[far_otm_mask] = call_weighted_mid[far_otm_mask]
        put_chosen_price[far_otm_mask] = put_weighted_mid[far_otm_mask]
        
        # Handle zero/missing LTP properly
        call_ltp_missing = (mega_df["call_ltp"] == 0) | (mega_df["call_ltp"] == "N/A") | mega_df["call_ltp"].isna()
        put_ltp_missing = (mega_df["put_ltp"] == 0) | (mega_df["put_ltp"] == "N/A") | mega_df["put_ltp"].isna()
        
        # Use weighted mid for missing LTP if available
        call_chosen_price[call_ltp_missing] = call_weighted_mid[call_ltp_missing]
        put_chosen_price[put_ltp_missing] = put_weighted_mid[put_ltp_missing]
        
        mega_df["call_flag"] = 'c'
        mega_df["put_flag"] = 'p'
        price_time = time.time() - price_start
        logger.info(f"MEGA IV: Price selection completed in {price_time:.2f}s")
        
        # Step 5: MEGA IV CALCULATION
        iv_start = time.time()
        logger.info(f"MEGA IV: Starting IV calculation for {len(mega_df)} total strikes across all configs...")
        
        # Calculate Call IVs for ALL configs at once
        call_iv_start = time.time()
        mega_df["call_iv"] = vectorized_implied_volatility(
            call_chosen_price, 
            mega_df["synthetic"], 
            mega_df["strikeprice"], 
            mega_df["tte"], 
            rate, 
            mega_df["call_flag"]
        )
        call_iv_time = time.time() - call_iv_start
        
        # Calculate Put IVs for ALL configs at once
        put_iv_start = time.time()
        mega_df["put_iv"] = vectorized_implied_volatility(
            put_chosen_price, 
            mega_df["synthetic"], 
            mega_df["strikeprice"], 
            mega_df["tte"], 
            rate, 
            mega_df["put_flag"]
        )
        put_iv_time = time.time() - put_iv_start
        
        total_iv_time = time.time() - iv_start
        logger.info(f"*** MEGA IV CALCULATION COMPLETE ***")
        logger.info(f"  Call IV: {call_iv_time:.2f}s for {len(mega_df)} strikes")
        logger.info(f"  Put IV: {put_iv_time:.2f}s for {len(mega_df)} strikes")
        logger.info(f"  Total IV time: {total_iv_time:.2f}s")
        logger.info(f"  Processing rate: {len(mega_df)/total_iv_time:.0f} strikes/second")
        
        # Step 6: IV Interpolation
        interp_start = time.time()
        logger.info("MEGA IV: Starting IV interpolation...")
        
        # Group by config_id for batch processing
        config_groups = mega_df.groupby('config_id')
        interpolation_stats = {'call_interpolated': 0, 'put_interpolated': 0, 'call_failed': 0, 'put_failed': 0}
        
        for config_id, config_df in config_groups:
            config_indices = config_df.index
            
            # Call IV interpolation
            call_iv_nan = config_df["call_iv"].isna()
            if call_iv_nan.any():
                try:
                    valid_call_mask = ~call_iv_nan
                    if valid_call_mask.sum() >= 3:
                        valid_strikes = config_df.loc[valid_call_mask, "strikeprice"].values
                        valid_ivs = config_df.loc[valid_call_mask, "call_iv"].values
                        
                        cs_call = CubicSpline(valid_strikes, valid_ivs, extrapolate=True)
                        
                        nan_strikes = config_df.loc[call_iv_nan, "strikeprice"].values
                        interpolated_ivs = cs_call(nan_strikes)
                        
                        mega_df.loc[config_indices[call_iv_nan], "call_iv"] = interpolated_ivs
                        interpolation_stats['call_interpolated'] += len(nan_strikes)
                        
                    else:
                        interpolation_stats['call_failed'] += call_iv_nan.sum()
                        
                except Exception as e:
                    logger.error(f"Call IV interpolation failed for config {config_id}: {e}")
                    interpolation_stats['call_failed'] += call_iv_nan.sum()
            
            # Put IV interpolation
            put_iv_nan = config_df["put_iv"].isna()
            if put_iv_nan.any():
                try:
                    valid_put_mask = ~put_iv_nan
                    if valid_put_mask.sum() >= 3:
                        valid_strikes = config_df.loc[valid_put_mask, "strikeprice"].values
                        valid_ivs = config_df.loc[valid_put_mask, "put_iv"].values
                        
                        cs_put = CubicSpline(valid_strikes, valid_ivs, extrapolate=True)
                        
                        nan_strikes = config_df.loc[put_iv_nan, "strikeprice"].values
                        interpolated_ivs = cs_put(nan_strikes)
                        
                        mega_df.loc[config_indices[put_iv_nan], "put_iv"] = interpolated_ivs
                        interpolation_stats['put_interpolated'] += len(nan_strikes)
                        
                    else:
                        interpolation_stats['put_failed'] += put_iv_nan.sum()
                        
                except Exception as e:
                    logger.error(f"Put IV interpolation failed for config {config_id}: {e}")
                    interpolation_stats['put_failed'] += put_iv_nan.sum()
        
        # Final cleanup
        mega_df["call_iv"] = mega_df["call_iv"].fillna(0.01).clip(lower=0.01)
        mega_df["put_iv"] = mega_df["put_iv"].fillna(0.01).clip(lower=0.01)
        
        interp_time = time.time() - interp_start
        logger.info(f"*** IV INTERPOLATION COMPLETE ***")
        logger.info(f"  Call IV interpolated: {interpolation_stats['call_interpolated']} values")
        logger.info(f"  Put IV interpolated: {interpolation_stats['put_interpolated']} values")
        logger.info(f"  Interpolation time: {interp_time:.2f}s")
        
        # Step 7: MEGA GREEKS CALCULATION
        greeks_start = time.time()
        logger.info(f"MEGA IV: Starting Greeks calculation for {len(mega_df)} strikes...")
        
        mega_df["tte_adj"] = mega_df["tte"].clip(lower=1e-6)
        
        # Calculate all Greeks for ALL configs at once
        mega_df["call_delta"] = delta("c", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["call_iv"])
        mega_df["call_gamma"] = gamma("c", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["call_iv"])
        mega_df["call_vega"] = vega("c", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["call_iv"])
        mega_df["call_theta"] = theta("c", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["call_iv"])
        
        mega_df["put_delta"] = delta("p", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["put_iv"])
        mega_df["put_gamma"] = gamma("p", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["put_iv"])
        mega_df["put_vega"] = vega("p", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["put_iv"])
        mega_df["put_theta"] = theta("p", mega_df["synthetic"], mega_df["strikeprice"], mega_df["tte_adj"], rate, mega_df["put_iv"])
        
        greeks_time = time.time() - greeks_start
        logger.info(f"MEGA IV: Greeks calculation completed in {greeks_time:.2f}s")
        
        # Step 8: Final scaling
        scaling_start = time.time()
        mega_df["call_iv"] = mega_df["call_iv"] * 100
        mega_df["put_iv"] = mega_df["put_iv"] * 100
        mega_df["call_delta"] = mega_df["call_delta"] * 100
        mega_df["put_delta"] = mega_df["put_delta"] * 100
        mega_df["call_oi"] = mega_df["call_oi"] * 100
        mega_df["put_oi"] = mega_df["put_oi"] * 100
        scaling_time = time.time() - scaling_start
        
        # Step 9: Split back into individual config DataFrames and apply IV-based strike filter
        split_start = time.time()
        processed_configs = []
        
        for config_id in mega_df['config_id'].unique():
            config_mask = mega_df['config_id'] == config_id
            config_df = mega_df[config_mask].drop('config_id', axis=1).reset_index(drop=True)
            
            # Find original config data
            original_config = all_configs_data[config_id]
            
            # Apply IV-based strike range filter
            synthetic_atm = config_df['synthetic_atm'].iloc[0] if not config_df.empty else 0
            tte = config_df['tte'].iloc[0] if not config_df.empty else 0.1
            config_df_filtered = filter_strikes_by_iv_range(config_df, synthetic_atm, tte)
            
            processed_configs.append({
                'symbol': original_config['symbol'],
                'expiry_date': original_config['expiry_date'],
                'df_processed': config_df_filtered,
                'dividend_amount': original_config.get('dividend_amount', 0.0)
            })
        
        split_time = time.time() - split_start
        
        total_mega_time = time.time() - mega_start
        
        logger.info(f"*** MEGA IV PROCESSING WITH DIVIDENDS COMPLETE ***")
        logger.info(f"  Total configs processed: {len(processed_configs)}")
        logger.info(f"  Total strikes processed: {len(mega_df)}")
        logger.info(f"  Dividend processing time: {dividend_time:.2f}s")
        logger.info(f"  Combine time: {combine_time:.2f}s")
        logger.info(f"  IV calculation time: {total_iv_time:.2f}s")
        logger.info(f"  Interpolation time: {interp_time:.2f}s")
        logger.info(f"  Greeks calculation time: {greeks_time:.2f}s")
        logger.info(f"  Split time: {split_time:.2f}s")
        logger.info(f"  TOTAL MEGA TIME: {total_mega_time:.2f}s")
        logger.info(f"  Average per config: {total_mega_time/len(processed_configs):.2f}s")
        
        # Log to performance summary
        perf_summary_logger.info(f"MEGA_IV_CALC_DIVIDENDS - Total: {total_mega_time:.2f}s, Dividend: {dividend_time:.2f}s, IV: {total_iv_time:.2f}s, Interp: {interp_time:.2f}s, Greeks: {greeks_time:.2f}s, Configs: {len(processed_configs)}, Strikes: {len(mega_df)}")
        
        return processed_configs
        
    except Exception as e:
        logger.error(f"Error in mega IV calculation with dividends: {e}")
        return []

@profile_time("process_option_metrics_basic")
def process_option_metrics_basic(df_final, symbol, expiry_date, now):
    """Basic option metrics processing without IV/Greeks (done in mega function)"""
    try:
        start_time = time.time()
        
        # Step 1: Data cleaning - don't remove rows with LTP=0
        original_len = len(df_final)
        
        # Only remove rows where essential bid/ask data is missing
        essential_cols = ['call_bidprice', 'call_askprice', 'put_bidprice', 'put_askprice', 
                         'call_bidquant', 'call_askquant', 'put_bidquant', 'put_bidquant']
        
        essential_missing_mask = df_final[essential_cols].isin(["N/A", "ERR"]).any(axis=1)
        df_final = df_final[~essential_missing_mask].reset_index(drop=True)
        
        logger.info(f"Basic processing for {symbol}: Kept {len(df_final)}/{original_len} rows (removed only essential missing data)")
        
        if df_final.empty:
            return None
        
        # Step 2: ATM calculation
        atm_strike = get_underlying_atm(r, symbol)
        synthetic_price = None
        
        def find_nearest_strike(available_strikes, target_strike, tolerance=0.01):
            """Find the nearest available strike to target, with tolerance for float precision"""
            try:
                available_strikes = [float(s) for s in available_strikes]
                target_strike = float(target_strike)
                
                # Find closest strike within tolerance
                closest_strike = min(available_strikes, key=lambda x: abs(x - target_strike))
                
                if abs(closest_strike - target_strike) <= tolerance:
                    return closest_strike
                else:
                    return target_strike  # Return target if no close match found
                    
            except Exception as e:
                logger.error(f"Error in find_nearest_strike: {e}")
                return target_strike
        
        if atm_strike is not None:
            # Use nearest strike matching for float precision
            available_strikes = df_final['strikeprice'].unique()
            nearest_atm = find_nearest_strike(available_strikes, atm_strike)
            
            df_final['strike_diff'] = abs(df_final['strikeprice'] - nearest_atm)
            min_diff = df_final['strike_diff'].min()
            min_diff_rows = df_final[df_final['strike_diff'] == min_diff]
            
            if len(min_diff_rows) > 1 and any(min_diff_rows['strikeprice'] >= nearest_atm):
                atm_row = min_diff_rows[min_diff_rows['strikeprice'] >= nearest_atm].iloc[[0]]
            else:
                atm_row = min_diff_rows.iloc[[0]]
            
            try:
                # Handle zero LTP in synthetic calculation
                call_ltp = float(atm_row.iloc[0]['call_ltp']) if atm_row.iloc[0]['call_ltp'] not in [0, "N/A"] else 0.0
                put_ltp = float(atm_row.iloc[0]['put_ltp']) if atm_row.iloc[0]['put_ltp'] not in [0, "N/A"] else 0.0
                strike = atm_row.iloc[0]['strikeprice']
                
                # If both LTPs are zero, try to use bid/ask midpoint
                if call_ltp == 0.0:
                    call_bid = float(atm_row.iloc[0]['call_bidprice']) if atm_row.iloc[0]['call_bidprice'] != "N/A" else 0.0
                    call_ask = float(atm_row.iloc[0]['call_askprice']) if atm_row.iloc[0]['call_askprice'] != "N/A" else 0.0
                    if call_bid > 0 and call_ask > 0:
                        call_ltp = (call_bid + call_ask) / 2
                
                if put_ltp == 0.0: 
                    put_bid = float(atm_row.iloc[0]['put_bidprice']) if atm_row.iloc[0]['put_bidprice'] != "N/A" else 0.0
                    put_ask = float(atm_row.iloc[0]['put_askprice']) if atm_row.iloc[0]['put_askprice'] != "N/A" else 0.0
                    if put_bid > 0 and put_ask > 0:
                        put_ltp = (put_bid + put_ask) / 2
                
                synthetic_price = call_ltp - put_ltp + strike
                logger.info(f"Synthetic price for {symbol}: {synthetic_price:.2f} (Call: {call_ltp}, Put: {put_ltp}, Strike: {strike})")
                
            except Exception as e:
                logger.error(f"Error calculating synthetic price: {e}")
            
            df_final = df_final.drop('strike_diff', axis=1)
        
        if synthetic_price is None:
            logger.warning(f"Could not calculate synthetic price for {symbol}")
            return None
        
        # Step 3: TTE and moneyness calculation - NOW WITH HOLIDAY EXCLUSION
        expiry = datetime.strptime(df_final.loc[0, 'expirydate'], "%d-%m-%Y")
        tte = calculate_trading_time_to_expiry(now, expiry, HOLIDAYS_SET)  # Pass holidays set
        
        strike_gap = float(get_strike_gap(symbol))  # Ensure float
        
        if strike_gap >= 1.0:
            synthetic_atm = round(synthetic_price / strike_gap) * strike_gap
        else:
            # Handle fractional gaps properly
            multiplier = round(synthetic_price / strike_gap)
            synthetic_atm = multiplier * strike_gap
            # Round to appropriate decimal places
            decimal_places = len(str(strike_gap).split('.')[-1]) if '.' in str(strike_gap) else 0
            synthetic_atm = round(synthetic_atm, decimal_places)
        
        df_final["call_moneyness"] = synthetic_atm - df_final["strikeprice"]
        df_final["put_moneyness"] = df_final["strikeprice"] - synthetic_atm
        df_final["tte"] = tte
        df_final["synthetic"] = synthetic_price
        df_final["synthetic_atm"] = synthetic_atm
        
        logger.info(f"Basic processing complete for {symbol}: {len(df_final)} strikes, TTE: {tte:.6f}, Synthetic ATM: {synthetic_atm}")
        
        return df_final
        
    except Exception as e:
        logger.error(f"Error processing basic option metrics: {e}")
        return None

def get_formatted_timestamp(dt):
    """Get timestamp in mm:ss:msms format in IST without timezone"""
    ist_dt = dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    return ist_dt.strftime("%M:%S:") + f"{ist_dt.microsecond // 1000:03d}"

@profile_time("save_snapshots_optimized")
def save_snapshots_optimized(df_final, symbol, expiry_date, now, dividend_amount=0.0, force_all_intervals=False):
    """OPTIMIZED: Save snapshots with 10-minute rule and improved performance"""
    try:
        expiry_str = datetime.strptime(expiry_date, "%d%m%Y").strftime("%Y-%m-%d")
        snapshot_dir = os.path.join(snapshot_root_dir, symbol, expiry_str)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        display_df = df_final[[
            "call_id", "call_oi", "call_volume", "call_type", "call_iv",
            "call_delta", "call_gamma", "call_vega", "call_theta",
            "call_ltp", "call_bidprice", "call_bidquant", "call_askprice", "call_askquant", "call_moneyness",
            "strikeprice", "expirydate",
            "put_bidprice", "put_bidquant", "put_askprice", "put_askquant", "put_iv",
            "put_delta", "put_gamma", "put_vega", "put_theta",
            "put_ltp", "put_type", "put_moneyness", "put_volume", "put_oi", "put_id"
        ]]
        
        ist_now = now.astimezone(IST) if now.tzinfo else IST.localize(now)
        
        # Check if we're in the first 10 minutes
        update_all_intervals = should_update_all_intervals(now) or force_all_intervals
        
        for interval_key, interval_seconds in intervals.items():
            with data_lock:
                last = last_update[symbol][expiry_date].get(interval_key)
                should_update = update_all_intervals or last is None or (now - last).total_seconds() >= interval_seconds
            
            if should_update:
                snapshot_data = {
                    "metadata": {
                        "symbol": symbol,
                        "expiry_date": expiry_date,
                        "last_update": ist_now.replace(tzinfo=None).isoformat(),
                        "interval": interval_key,
                        "total_strikes": len(display_df),
                        "timestamp": ist_now.strftime("%M:%S:") + f"{ist_now.microsecond // 1000:03d}",
                        "timezone": "IST",
                        "dividend": dividend_amount,
                        "holidays_excluded": len(HOLIDAYS_SET)
                    },
                    "data": display_df.to_dict(orient="records")
                }
                
                with data_lock:
                    snapshots[symbol][expiry_date][interval_key] = snapshot_data
                    last_update[symbol][expiry_date][interval_key] = now
                
                filename = f"{symbol}_{expiry_date}_latest_{interval_key}.json"
                filepath = os.path.join(snapshot_dir, filename)
                
                # Use optimized JSON writing
                with open(filepath, "w") as f:
                    if MSGPACK_AVAILABLE:
                        # Convert to JSON for file storage (for compatibility)
                        json.dump(snapshot_data, f, indent=2)
                    else:
                        f.write(JSON_DUMPS(snapshot_data, indent=2))
                
                # Log snapshot creation to separate file
                update_reason = "10min_rule" if update_all_intervals else "interval"
                snapshot_logger.info(f"SNAPSHOT CREATED: {symbol} {expiry_date} interval {interval_key} with {len(display_df)} rows (dividend: {dividend_amount}, holidays: {len(HOLIDAYS_SET)}, reason: {update_reason}) - {get_formatted_timestamp(now)}")
                
    except Exception as e:
        logger.error(f"Error saving snapshots: {e}")

@profile_time("process_single_config_optimized")
def process_single_config_optimized(config_path, all_symbols_data):
    """Optimized: Process a single config using pre-filtered data"""
    try:
        symbol, expiry_date = parse_config_filename(config_path)
        if symbol is None or expiry_date is None:
            logger.warning(f"Skipping invalid config filename: {config_path}")
            return None
        
        # Load all tokens from config (no filtering)
        tokens = load_config_tokens(config_path)
        if not tokens:
            logger.warning(f"No tokens found in config: {config_path}")
            return None
        
        # Filter pre-processed data for this symbol
        df_symbol = all_symbols_data[all_symbols_data["symbol"] == symbol.upper()].copy()
        if df_symbol.empty:
            logger.warning(f"No data found for symbol {symbol}")
            return None
            
        df_expiry = subset_for_nearest_expiry(df_symbol, expiry_date)
        
        if df_expiry.empty:
            logger.warning(f"No data found for symbol {symbol} and expiry {expiry_date}")
            return None
        
        df_expiry = df_expiry.sort_values("strikeprice")
        df_merged = pivot_calls_and_puts(df_expiry).reset_index(drop=True)
        
        return {
            'symbol': symbol,
            'expiry_date': expiry_date,
            'df_merged': df_merged,
            'config_path': config_path,
            'tokens': tokens
        }
        
    except Exception as e:
        logger.error(f"Error processing config {config_path}: {e}")
        return None

@profile_time("prepare_configs_for_mega_processing_optimized")
def prepare_configs_for_mega_processing_optimized(target_configs, now):
    """OPTIMIZED: Prepare all configs for mega IV processing by fetching Redis data"""
    try:
        prep_start = time.time()
        all_configs_data = []
        
        # Collect all Redis keys needed
        all_keys = []
        key_mapping = {}  # Map key index to config index
        
        for i, config_data in enumerate(target_configs):
            df_merged = config_data['df_merged']
            config_keys = list(df_merged["call_id"]) + list(df_merged["put_id"])
            
            # Track which keys belong to which config
            start_idx = len(all_keys)
            all_keys.extend(config_keys)
            end_idx = len(all_keys)
            
            key_mapping[i] = {
                'start': start_idx,
                'end': end_idx,
                'n_calls': len(df_merged["call_id"])
            }
        
        # Batch fetch ALL Redis data at once with optimized function
        redis_start = time.time()
        logger.info(f"MEGA PREP OPTIMIZED: Fetching {len(all_keys)} Redis keys for all configs...")
        all_ticks = get_latest_ticks_batch_optimized(r, all_keys)
        redis_time = time.time() - redis_start
        logger.info(f"MEGA PREP OPTIMIZED: Redis batch fetch completed in {redis_time:.2f}s")
        
        # Process each config with its Redis data
        for i, config_data in enumerate(target_configs):
            try:
                symbol = config_data['symbol']
                expiry_date = config_data['expiry_date']
                df_merged = config_data['df_merged']
                
                # Extract Redis data for this config
                key_info = key_mapping[i]
                config_ticks = all_ticks[key_info['start']:key_info['end']]
                
                # Split into calls and puts
                n_calls = key_info['n_calls']
                call_ticks = config_ticks[:n_calls]
                put_ticks = config_ticks[n_calls:]
                
                # Create final DataFrame
                call_df = pd.DataFrame(call_ticks).add_prefix("call_")
                put_df = pd.DataFrame(put_ticks).add_prefix("put_")
                df_final = pd.concat([df_merged, call_df, put_df], axis=1)
                
                # Basic processing (without IV/Greeks)
                df_processed = process_option_metrics_basic(df_final, symbol, expiry_date, now)
                
                if df_processed is not None and not df_processed.empty:
                    all_configs_data.append({
                        'symbol': symbol,
                        'expiry_date': expiry_date,
                        'df_final': df_processed
                    })
                
            except Exception as e:
                logger.error(f"Error preparing config {i}: {e}")
                continue
        
        prep_time = time.time() - prep_start
        logger.info(f"MEGA PREP OPTIMIZED: Prepared {len(all_configs_data)} configs in {prep_time:.2f}s")
        
        return all_configs_data
        
    except Exception as e:
        logger.error(f"Error in prepare_configs_for_mega_processing_optimized: {e}")
        return []

def cleanup_old_redis_data(max_age_hours=24):
    """Clean up old option chain data from Redis"""
    try:
        pattern = "*_*_*"
        keys = redis_storage.keys(pattern)
        
        deleted_count = 0
        current_time = datetime.now()
        
        for key in keys:
            try:
                data = redis_storage.get(key)
                if data:
                    try:
                        strike_data = optimized_deserialize(data)
                    except:
                        # Fallback to JSON
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        strike_data = JSON_LOADS(data)
                    
                    timestamp = datetime.fromisoformat(strike_data['timestamp'])
                    
                    if (current_time - timestamp).total_seconds() > (max_age_hours * 3600):
                        redis_storage.delete(key)
                        deleted_count += 1
                        
            except Exception as e:
                logger.error(f"Error processing key {key} for cleanup: {e}")
                continue
        
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old Redis keys")
            
    except Exception as e:
        logger.error(f"Error during Redis cleanup: {e}")

def run_flask_app():
    """Run Flask app in a separate thread with better error handling"""
    try:
        logger.info("Starting Flask app on 0.0.0.0:5000")
        logger.info("Available routes:")
        for rule in app.url_map.iter_rules():
            logger.info(f"  {rule.methods} {rule.rule}")
        
        # Add a small delay to ensure everything is initialized
        time.sleep(2)
        
        # Start Flask with better error handling
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True, use_reloader=False)
    except Exception as e:
        logger.error(f"Flask app failed to start: {e}")
        import traceback
        logger.error(f"Flask startup traceback: {traceback.format_exc()}")

@profile_time("process_target_configs_continuously_optimized")
def process_target_configs_continuously_optimized(config_files, data):
    """ULTRA OPTIMIZED: Process target configs with DIVIDENDS, HOLIDAYS and ULTRA BATCH REDIS SAVES"""
    try:
        setup_start = time.time()
        
        # OPTIMIZATION: Process all symbols data once
        step_start = time.time()
        logger.info("OPTIMIZATION: Processing all symbols data once...")
        symbols_in_configs = []
        for config_path in config_files:
            symbol, _ = parse_config_filename(config_path)
            if symbol:
                symbols_in_configs.append(symbol)
        
        unique_symbols = list(set(symbols_in_configs))
        logger.info(f"Unique symbols found: {unique_symbols}")
        symbol_extraction_time = time.time() - step_start
        perf_logger.info(f"Symbol extraction: {symbol_extraction_time:.4f} seconds")
        
        # Filter data for all symbols at once (MAJOR OPTIMIZATION)
        step_start = time.time()
        all_symbols_data = filter_all_symbols_data(data, unique_symbols)
        data_filtering_time = time.time() - step_start
        logger.info(f"PERFORMANCE: All symbols data filtering took {data_filtering_time:.2f} seconds")
        
        # Prepare all target configs using pre-filtered data
        step_start = time.time()
        target_configs = []
        logger.info("Processing individual configs with pre-filtered data...")
        
        config_processing_times = []
        for i, config_path in enumerate(config_files):
            config_start = time.time()
            config_data = process_single_config_optimized(config_path, all_symbols_data)
            config_time = time.time() - config_start
            config_processing_times.append(config_time)
            
            if config_data:
                target_configs.append(config_data)
                perf_logger.info(f"Config {i+1}/{len(config_files)} ({config_data['symbol']}): {config_time:.4f} seconds")
        
        total_config_time = time.time() - step_start
        avg_config_time = sum(config_processing_times) / len(config_processing_times) if config_processing_times else 0
        
        perf_logger.info(f"*** CONFIG PROCESSING BREAKDOWN ***")
        perf_logger.info(f"  Total config processing: {total_config_time:.4f} seconds")
        perf_logger.info(f"  Average per config: {avg_config_time:.4f} seconds")
        perf_logger.info(f"  Configs processed: {len(target_configs)}/{len(config_files)}")
        
        total_setup_time = time.time() - setup_start
        logger.info(f"*** SETUP COMPLETE: {total_setup_time:.2f} seconds total ***")
        perf_logger.info(f"*** COMPLETE SETUP BREAKDOWN ***")
        perf_logger.info(f"  Symbol extraction: {symbol_extraction_time:.4f}s")
        perf_logger.info(f"  Data filtering: {data_filtering_time:.4f}s") 
        perf_logger.info(f"  Config processing: {total_config_time:.4f}s")
        perf_logger.info(f"  TOTAL SETUP TIME: {total_setup_time:.4f}s")
        
        # Log setup summary
        perf_summary_logger.info(f"SETUP_OPT - Total: {total_setup_time:.2f}s, Filtering: {data_filtering_time:.2f}s, Configs: {total_config_time:.2f}s, ConfigCount: {len(target_configs)}, Holidays: {len(HOLIDAYS_SET)}")
        
        if not target_configs:
            logger.warning(f"No valid configs found for target symbols")
            return
        
        logger.info(f"Processing {len(target_configs)} target configs continuously together with DIVIDENDS, HOLIDAYS ({len(HOLIDAYS_SET)} loaded) and OPTIMIZATIONS")
        
        # Log which symbols are being processed
        symbols_being_processed = list(set([config['symbol'] for config in target_configs]))
        logger.info(f"Symbols being processed: {symbols_being_processed}")
        
        # Process all target configs simultaneously and continuously with DIVIDENDS and HOLIDAYS
        while True:
            now = datetime.now()
            iteration_start = time.time()
            
            logger.info(f"[{get_formatted_timestamp(now)}] Starting ULTRA OPTIMIZED processing with DIVIDENDS and HOLIDAYS for all {len(target_configs)} configs")
            
            # Step 1: Prepare all configs (fetch Redis data) - OPTIMIZED
            all_configs_data = prepare_configs_for_mega_processing_optimized(target_configs, now)
            
            if not all_configs_data:
                logger.warning("No configs prepared for mega processing")
                time.sleep(PROCESSING_INTERVAL)
                continue
            
            # Step 2: ULTRA OPTIMIZED MEGA IV CALCULATION WITH DIVIDENDS
            processed_configs = mega_iv_calculation_with_dividends(all_configs_data, now, data)
            
            # Step 3: Save results - ULTRA BATCH APPROACH
            save_start = time.time()
            
            # Save snapshots (with dividend and holiday support and 10-minute rule) - OPTIMIZED
            snapshot_save_start = time.time()
            for config_result in processed_configs:
                try:
                    symbol = config_result['symbol']
                    expiry_date = config_result['expiry_date']
                    df_processed = config_result['df_processed']
                    dividend_amount = config_result.get('dividend_amount', 0.0)
                    save_snapshots_optimized(df_processed, symbol, expiry_date, now, dividend_amount)
                except Exception as e:
                    logger.error(f"Error saving snapshots: {e}")
            snapshot_save_time = time.time() - snapshot_save_start

            # Save ALL configs to Redis in ONE ultra batch - OPTIMIZED
            redis_save_start = time.time()
            redis_success = save_all_configs_to_redis_ultra_batch_optimized(processed_configs, now)
            redis_save_time = time.time() - redis_save_start
            completed_count = len(processed_configs) if redis_success else 0
            
            save_time = time.time() - save_start
            iteration_time = time.time() - iteration_start
            
            logger.info(f"[{get_formatted_timestamp(now)}] ULTRA ITERATION OPTIMIZED WITH DIVIDENDS AND HOLIDAYS COMPLETE:")
            logger.info(f"  Configs processed: {completed_count}/{len(target_configs)}")
            logger.info(f"  Holidays excluded: {len(HOLIDAYS_SET)}")
            logger.info(f"  Snapshot save time: {snapshot_save_time:.2f}s")
            logger.info(f"  Redis save time: {redis_save_time:.2f}s")
            logger.info(f"  Total save time: {save_time:.2f}s")
            logger.info(f"  Total iteration time: {iteration_time:.2f}s")
            logger.info(f"  Serialization: {logger_serialization_lib}")
            
            # Log iteration summary
            perf_summary_logger.info(f"ITERATION_OPT - Total: {iteration_time:.2f}s, Save: {save_time:.2f}s, Redis: {redis_save_time:.2f}s, Snapshots: {snapshot_save_time:.2f}s, Configs: {completed_count}, Holidays: {len(HOLIDAYS_SET)}, Serialization: {logger_serialization_lib}")
            
            # Sleep for 1 minute before next iteration
            time.sleep(PROCESSING_INTERVAL)
            
    except Exception as e:
        logger.error(f"Error in continuous target processing optimized with dividends and holidays: {e}")

@profile_time("main_execution")
def main():
    # Use relative path in same directory
    json_path = "NSE_FO.json"
    
    # Check if JSON file exists
    if not os.path.exists(json_path):
        logger.error(f"NSE_FO.json not found in current directory: {os.getcwd()}")
        return
    
    # Test Redis connections
    logger.info("Testing Redis connections...")
    try:
        r.ping()
        logger.info("Connected to market data Redis (db=0)")
        redis_storage.ping()
        logger.info("Connected to option chain storage Redis (db=1)")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return
    
    # Start Flask app in a separate thread with better error handling
    logger.info("Starting Flask web interface on port 5000...")
    try:
        flask_thread = threading.Thread(target=run_flask_app, daemon=False, name="FlaskThread")
        flask_thread.start()
        
        # Give Flask time to start and check if it's running
        time.sleep(5)
        if flask_thread.is_alive():
            logger.info("Flask thread started successfully")
        else:
            logger.error("Flask thread failed to start or died immediately")
        
    except Exception as e:
        logger.error(f"Failed to start Flask thread: {e}")
    
    last_cleanup = datetime.now()
    
    # Log startup summary
    perf_summary_logger.info(f"STARTUP_OPT - JSON_LIB: {logger_json_lib}, SERIALIZATION: {logger_serialization_lib}, TARGET_SYMBOLS: {len(TARGET_SYMBOLS)}, PROCESSING_INTERVAL: {PROCESSING_INTERVAL}s, MSGPACK: {MSGPACK_AVAILABLE}, HOLIDAYS: {len(HOLIDAYS_SET)}")
    
    while True:
        try:
            logger.info(f"=== ULTRA OPTIMIZED PROCESSING WITH DIVIDENDS AND HOLIDAYS FOR TARGET SYMBOLS: {TARGET_SYMBOLS} ===")
            logger.info(f"=== HOLIDAYS LOADED: {len(HOLIDAYS_SET)} ===")
            
            # Load data
            start_time = time.time()
            logger.info("Loading NSE_FO.json data...")
            data = load_json(json_path)
            load_time = time.time() - start_time
            logger.info(f"PERFORMANCE: JSON loading took {load_time:.2f} seconds")
            
            # Get filtered config files (only target symbols)
            target_config_files = get_filtered_config_files()
            
            if not target_config_files:
                logger.warning(f"No config files found for target symbols: {TARGET_SYMBOLS}. Sleeping for 30 seconds.")
                time.sleep(30)
                continue
            
            logger.info(f"Processing {len(target_config_files)} config files for target symbols with OPTIMIZED DIVIDENDS and HOLIDAYS approach")
            
            # Process all target configs as one batch continuously with OPTIMIZED DIVIDENDS and HOLIDAYS
            process_target_configs_continuously_optimized(target_config_files, data)
            
            # Cleanup old Redis data every hour
            now = datetime.now()
            if (now - last_cleanup).total_seconds() > 3600:
                cleanup_old_redis_data()
                last_cleanup = now
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
