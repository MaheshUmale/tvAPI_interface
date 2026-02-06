# PRODESK Simplified Terminal

A minimal, high-performance trading terminal featuring TradingView charting, real-time WebSocket data, and candle-by-candle replay.

## Features

- **Minimal UI**: Clean interface with only a search bar and a full-screen chart.
- **TradingView Charts**: Powered by TradingView Lightweight Charts (v4.1.1) for professional-grade charting with native zoom and pan.
- **Zoom Controls**: Dedicated (+), (-), and RESET buttons for easy timescale management.
- **Candle-by-Candle Replay**:
  - Enter Replay mode to analyze historical price action.
  - **Select Start**: Click anywhere on the chart to set the starting point for replay.
  - **Playback**: Use Play/Pause, Next, and Previous buttons to step through candles one by one.
- **Real-time Data**: Live quote streaming via TradingView WebSocket (WSS) protocol.
- **Universal Search**: Search for any symbol across exchanges supported by TradingView.
- **Advanced Technical Indicators**:
    - **Colored Candles**: Volume-based candle coloring for trend and strength analysis.
    - **Volume Bubbles**: Visualizes significant volume spikes directly on price action.
    - **MTF S/R Dots**: Support and Resistance levels derived from high-volume bars.
    - **Dynamic Pivot**: Real-time trend detection using Force/Pivot logic.
    - **EVWMA**: Elastic Volume Weighted Moving Average.
    - **Swing Breaks**: Automated Swing High/Low detection.
- **Customizable Themes**: Toggle between professional Dark and Light themes.
- **Efficient Backend**: Built with FastAPI and Socket.IO for low-latency data delivery.

## Architecture

- **Frontend**: Single Page Application (SPA) using Tailwind CSS, Socket.IO client, and TradingView Lightweight Charts.
- **Backend**:
  - `FastAPI`: REST API for symbol search and historical data.
  - `Socket.IO`: Real-time bi-directional communication.
  - `TradingViewWSS`: Custom implementation of the TradingView WebSocket protocol for live feeds.
  - `Data Engine`: Aggregates and broadcasts market data to connected clients.
- **Database**: `DuckDB` used for local storage of tick data and instrument metadata.

## Setup & Running

### Prerequisites

- Python 3.10+
- Dependencies listed in `requirements.txt`

### Installation

1. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Optional: Set TradingView credentials in environment variables for authenticated access:
```bash
export TV_USERNAME='your_username'
export TV_PASSWORD='your_password'
```

### Running the Server

Start the application from the project root:

```bash
python3 backend/api_server.py
```

The terminal will be available at `http://localhost:5051`.

## Usage

1. **Search**: Use the centered search bar at the top to find any symbol.
2. **Zoom**: Use the (+) and (-) buttons on the top right to zoom the chart.
3. **Replay**:
   - Click the **REPLAY** button.
   - Click a candle on the chart to set the start point.
   - Use the controls to play or step through candles.
   - Click **EXIT** to return to live mode.
