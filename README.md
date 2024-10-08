# FobaBacktestEngine


**FobaBacktestEngine** is a high-performance, Numba-powered backtesting engine designed for processing full order book and market-by-order (MBO) data - originally developed for HKEX. The engine supports a variety of configurations, allowing users to analyze and simulate market behavior in various modes. The 3 modes implemented thusfar include **Training**, **Simulation**, and **PassiveAnalysis**.

---

### 1. Training Mode

In **Training Mode**, the engine processes market data from the HKEX feed in Market-by-Order (MBO) format, including broker queue information. This mode is primarily used to generate the necessary features for machine learning models, such as XGBoost. It provides the following functionality:

- **True Packet Receive Delay**: The actual delay of packet receipt from the exchange is captured and recorded.
- **Internal Feature Generation Delay**: The internal system delay during feature generation is modeled to reflect real-world performance conditions.

Two key training datasets are produced:
- **THRESHOLD Training DataFrame**: A threshold-based dataset for training.
- **CONTINUOUS_VALUATION Training DataFrame**: A continuously generated valuation dataset with attached delays for accurate feature alignment.

---

### 2. Simulation Mode

In **Simulation Mode**, the engine reconstructs the order book using the MBO data feed while maintaining a distinct internal order book for simulation purposes. The strategy logic interacts with this internal order book through two specialized components:

- **HitterEngine**: Manages the execution of aggressive orders with logic for fast-hitting, order pyramids, and dynamic order placement.
- **QuoterEngine**: Handles passive order placement, incorporating features such as quoting priority offsets and order pyramids.

These components are integrated with the following:

- **ValuationEngine**: Receives order book data and applies real-time valuations based on machine learning models (e.g., XGBoost in JSON format). This valuation is then used to guide both the Hitter and Quoter engines.
- **RiskManager**: Monitors overall exposure and manages risk based on real-time trade confirmations and order execution. It ensures compliance with the strategy's risk profile.

Both the Hitter and Quoter engines submit orders through the **Gateway** component, which simulates exchange interaction, including:

- **Exchange Throttling**: Simulates delays due to exchange throughput limitations.
- **Short-Selling Restrictions**: Implements market-specific constraints on order execution.

The Simulation Mode is used to evaluate strategy performance by simulating real market conditions and allowing for fine-grained strategy testing, including smart order routing, diming, and matching against passive liquidity.

---

### 3. PassiveAnalysis Mode

The **PassiveAnalysis Mode** focuses on processing and analyzing historical MBO data. This mode reconstructs and outputs the following datasets:

- **TradedOrder DataFrame**: A detailed record of all traded orders.
- **PulledOrder DataFrame**: A record of all orders that were placed but later pulled from the order book.

The analysis extends to identifying behavioral patterns among market participants, including:
- **Smart Participant Behavior**: Investigates the actions of market participants who strategically trade and pull orders, analyzing their priority, queue position, and other behavioral characteristics. We also quantify PnL for such market participants.
---
---
## Configuration

The **FobaBacktestEngine** offers extensive customization options, allowing users to configure the specific fields to be recorded during simulations, such as order placement, trade execution, and market depth. These configurations allow for detailed assessment and analysis of strategy performance under various market conditions.


---

## Contact

For inquiries, support, or contributions, please contact Kartikeya Bisht - Kartikeya.Bisht@optiver.au

