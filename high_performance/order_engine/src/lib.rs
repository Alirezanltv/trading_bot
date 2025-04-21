use mimalloc::MiMalloc;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// High-performance order matching engine implemented in Rust
/// 
/// This module provides ultra-fast order matching capabilities for the trading system.
#[pymodule]
fn order_engine(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<OrderBook>()?;
    m.add_class::<Order>()?;
    m.add_class::<OrderFill>()?;
    m.add_function(wrap_pyfunction!(create_order_book, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}

/// Get the version of the order engine
#[pyfunction]
fn version() -> String {
    "0.1.0".to_string()
}

/// Create a new order book for a given symbol
#[pyfunction]
fn create_order_book(symbol: String) -> OrderBook {
    OrderBook::new(symbol)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderStatus {
    Created,
    Pending,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

/// An order in the order book
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub side: String,
    #[pyo3(get)]
    pub order_type: String,
    #[pyo3(get)]
    pub price: Option<f64>,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub filled_quantity: f64,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub timestamp: u64,
    priority: u64,
}

/// A fill generated from order matching
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFill {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub order_id: String,
    #[pyo3(get)]
    pub price: f64,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pymethods]
impl Order {
    /// Create a new order
    #[new]
    pub fn new(
        symbol: String,
        side: String,
        order_type: String,
        quantity: f64,
        price: Option<f64>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            id: Uuid::new_v4().to_string(),
            symbol,
            side,
            order_type,
            price,
            quantity,
            filled_quantity: 0.0,
            status: "Created".to_string(),
            timestamp: now,
            priority: now,
        }
    }

    /// Convert order to a dictionary for Python
    pub fn to_dict(&self) -> PyResult<HashMap<String, PyObject>> {
        Python::with_gil(|py| {
            let mut dict = HashMap::new();
            dict.insert("id".to_string(), self.id.clone().into_py(py));
            dict.insert("symbol".to_string(), self.symbol.clone().into_py(py));
            dict.insert("side".to_string(), self.side.clone().into_py(py));
            dict.insert("order_type".to_string(), self.order_type.clone().into_py(py));
            if let Some(price) = self.price {
                dict.insert("price".to_string(), price.into_py(py));
            } else {
                dict.insert("price".to_string(), py.None());
            }
            dict.insert("quantity".to_string(), self.quantity.into_py(py));
            dict.insert("filled_quantity".to_string(), self.filled_quantity.into_py(py));
            dict.insert("status".to_string(), self.status.clone().into_py(py));
            dict.insert("timestamp".to_string(), self.timestamp.into_py(py));
            Ok(dict)
        })
    }
}

/// Implements ordering for orders in the priority queue
impl Ord for Order {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.side, &other.side) {
            (s1, s2) if s1 == s2 => {
                // Same side - compare based on price and time priority
                if s1 == "Buy" {
                    // For buy orders, higher prices come first
                    match (self.price, other.price) {
                        (Some(p1), Some(p2)) => {
                            if (p1 - p2).abs() < 0.0000001 {
                                // If prices are essentially equal, use time priority
                                other.priority.cmp(&self.priority)
                            } else {
                                p1.partial_cmp(&p2).unwrap_or(Ordering::Equal).reverse()
                            }
                        }
                        _ => other.priority.cmp(&self.priority), // Market orders get time priority
                    }
                } else {
                    // For sell orders, lower prices come first
                    match (self.price, other.price) {
                        (Some(p1), Some(p2)) => {
                            if (p1 - p2).abs() < 0.0000001 {
                                // If prices are essentially equal, use time priority
                                other.priority.cmp(&self.priority)
                            } else {
                                p1.partial_cmp(&p2).unwrap_or(Ordering::Equal)
                            }
                        }
                        _ => other.priority.cmp(&self.priority), // Market orders get time priority
                    }
                }
            }
            _ => Ordering::Equal, // Different sides shouldn't be compared
        }
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Order {}

/// An ultra-fast order book implementation
#[pyclass]
pub struct OrderBook {
    #[pyo3(get)]
    symbol: String,
    buy_orders: Arc<RwLock<BinaryHeap<Order>>>,
    sell_orders: Arc<RwLock<BinaryHeap<Order>>>,
    order_map: Arc<RwLock<HashMap<String, Order>>>,
    #[pyo3(get)]
    last_price: Option<f64>,
    #[pyo3(get)]
    last_updated: u64,
    fill_queue: Arc<RwLock<VecDeque<OrderFill>>>,
}

#[pymethods]
impl OrderBook {
    /// Create a new order book
    #[new]
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            buy_orders: Arc::new(RwLock::new(BinaryHeap::new())),
            sell_orders: Arc::new(RwLock::new(BinaryHeap::new())),
            order_map: Arc::new(RwLock::new(HashMap::new())),
            last_price: None,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            fill_queue: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Add an order to the order book and process it
    pub fn add_order(&mut self, order: Order) -> PyResult<Vec<OrderFill>> {
        let mut fills = Vec::new();
        
        // Process market orders immediately
        if order.order_type == "Market" {
            fills = self.match_order(order.clone());
            return Ok(fills);
        }

        // Add the order to the appropriate queue
        if order.side == "Buy" {
            let mut buy_orders = self.buy_orders.write().unwrap();
            buy_orders.push(order.clone());
        } else {
            let mut sell_orders = self.sell_orders.write().unwrap();
            sell_orders.push(order.clone());
        }

        // Add to order map for quick lookup
        let mut order_map = self.order_map.write().unwrap();
        order_map.insert(order.id.clone(), order.clone());

        // Try to match orders
        fills = self.match_orders();
        
        // Update timestamp
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        Ok(fills)
    }

    /// Cancel an order in the order book
    pub fn cancel_order(&mut self, order_id: String) -> PyResult<bool> {
        let mut order_map = self.order_map.write().unwrap();
        
        match order_map.get_mut(&order_id) {
            Some(order) => {
                // Mark as canceled
                order.status = "Canceled".to_string();
                
                // We'll just mark it as canceled and remove it on the next match attempt
                // This is more efficient than removing from the binary heap
                
                // Update timestamp
                self.last_updated = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                
                Ok(true)
            }
            None => Ok(false)
        }
    }

    /// Get order by ID
    pub fn get_order(&self, order_id: String) -> PyResult<Option<Order>> {
        let order_map = self.order_map.read().unwrap();
        Ok(order_map.get(&order_id).cloned())
    }

    /// Get the current order book state
    pub fn get_book_state(&self) -> PyResult<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
        // Create price aggregated views
        let mut bids: HashMap<f64, f64> = HashMap::new();
        let mut asks: HashMap<f64, f64> = HashMap::new();
        
        // Process buy orders
        let buy_orders = self.buy_orders.read().unwrap();
        for order in buy_orders.iter() {
            if order.status != "Canceled" && order.price.is_some() {
                let price = order.price.unwrap();
                let remaining = order.quantity - order.filled_quantity;
                *bids.entry(price).or_insert(0.0) += remaining;
            }
        }
        
        // Process sell orders
        let sell_orders = self.sell_orders.read().unwrap();
        for order in sell_orders.iter() {
            if order.status != "Canceled" && order.price.is_some() {
                let price = order.price.unwrap();
                let remaining = order.quantity - order.filled_quantity;
                *asks.entry(price).or_insert(0.0) += remaining;
            }
        }
        
        // Convert to sorted vectors
        let mut bid_vec: Vec<(f64, f64)> = bids.into_iter().collect();
        let mut ask_vec: Vec<(f64, f64)> = asks.into_iter().collect();
        
        // Sort bids in descending order
        bid_vec.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        
        // Sort asks in ascending order
        ask_vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        
        Ok((bid_vec, ask_vec))
    }

    /// Get all fills that haven't been processed yet
    pub fn get_pending_fills(&self) -> PyResult<Vec<OrderFill>> {
        let mut fill_queue = self.fill_queue.write().unwrap();
        let mut fills = Vec::new();
        
        while let Some(fill) = fill_queue.pop_front() {
            fills.push(fill);
        }
        
        Ok(fills)
    }
    
    /// Internal function to match orders
    fn match_orders(&mut self) -> Vec<OrderFill> {
        let mut fills = Vec::new();
        
        loop {
            // Get best buy and sell orders
            let best_buy = {
                let mut buy_orders = self.buy_orders.write().unwrap();
                
                // Remove canceled orders from the top
                while let Some(order) = buy_orders.peek() {
                    if order.status == "Canceled" {
                        buy_orders.pop();
                    } else {
                        break;
                    }
                }
                
                buy_orders.peek().cloned()
            };
            
            let best_sell = {
                let mut sell_orders = self.sell_orders.write().unwrap();
                
                // Remove canceled orders from the top
                while let Some(order) = sell_orders.peek() {
                    if order.status == "Canceled" {
                        sell_orders.pop();
                    } else {
                        break;
                    }
                }
                
                sell_orders.peek().cloned()
            };
            
            // Check if we have orders to match
            match (best_buy, best_sell) {
                (Some(buy), Some(sell)) => {
                    // Check if they can be matched
                    let can_match = match (buy.price, sell.price) {
                        // Market orders always match
                        (None, _) | (_, None) => true,
                        // Limit orders match if buy price >= sell price
                        (Some(buy_price), Some(sell_price)) => buy_price >= sell_price,
                    };
                    
                    if can_match {
                        // Match these orders
                        let mut new_fills = self.execute_match(buy, sell);
                        fills.append(&mut new_fills);
                    } else {
                        // No more matches possible
                        break;
                    }
                }
                _ => break, // Not enough orders to match
            }
        }
        
        fills
    }
    
    /// Match a single order against the order book
    fn match_order(&mut self, order: Order) -> Vec<OrderFill> {
        let mut fills = Vec::new();
        let mut remaining_qty = order.quantity;
        
        // Add to order map
        {
            let mut order_map = self.order_map.write().unwrap();
            order_map.insert(order.id.clone(), order.clone());
        }
        
        if order.side == "Buy" {
            // Match against sell orders
            loop {
                if remaining_qty <= 0.0 {
                    break;
                }
                
                // Get best sell order
                let best_sell = {
                    let mut sell_orders = self.sell_orders.write().unwrap();
                    
                    // Remove canceled orders from the top
                    while let Some(order) = sell_orders.peek() {
                        if order.status == "Canceled" {
                            sell_orders.pop();
                        } else {
                            break;
                        }
                    }
                    
                    sell_orders.peek().cloned()
                };
                
                match best_sell {
                    Some(sell) => {
                        // Check if they can be matched
                        let can_match = match (order.price, sell.price) {
                            // Market buy matches any sell
                            (None, _) => true,
                            // Limit buy matches if buy price >= sell price
                            (Some(buy_price), Some(sell_price)) => buy_price >= sell_price,
                            // Market sell doesn't match market buy (prevent zero price trades)
                            (Some(_), None) => false,
                        };
                        
                        if can_match {
                            // Calculate match quantity
                            let match_qty = remaining_qty.min(sell.quantity - sell.filled_quantity);
                            remaining_qty -= match_qty;
                            
                            // Create a fill
                            let fill_price = sell.price.unwrap_or(self.last_price.unwrap_or(0.0));
                            
                            let fill = OrderFill {
                                id: Uuid::new_v4().to_string(),
                                order_id: order.id.clone(),
                                price: fill_price,
                                quantity: match_qty,
                                timestamp: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64,
                            };
                            
                            // Update order statuses
                            self.update_order_status(&order.id, match_qty, fill_price);
                            self.update_order_status(&sell.id, match_qty, fill_price);
                            
                            // Remove fully filled sell order
                            let sell_filled = {
                                let sell_order = self.order_map.read().unwrap().get(&sell.id).cloned();
                                match sell_order {
                                    Some(o) => o.filled_quantity >= o.quantity,
                                    None => false,
                                }
                            };
                            
                            if sell_filled {
                                let mut sell_orders = self.sell_orders.write().unwrap();
                                sell_orders.pop();
                            }
                            
                            // Add fill to results
                            fills.push(fill.clone());
                            
                            // Add to fill queue
                            let mut fill_queue = self.fill_queue.write().unwrap();
                            fill_queue.push_back(fill);
                            
                            // Update last price
                            self.last_price = Some(fill_price);
                        } else {
                            // Cannot match anymore
                            break;
                        }
                    }
                    None => break, // No more sell orders
                }
            }
        } else {
            // Match against buy orders
            loop {
                if remaining_qty <= 0.0 {
                    break;
                }
                
                // Get best buy order
                let best_buy = {
                    let mut buy_orders = self.buy_orders.write().unwrap();
                    
                    // Remove canceled orders from the top
                    while let Some(order) = buy_orders.peek() {
                        if order.status == "Canceled" {
                            buy_orders.pop();
                        } else {
                            break;
                        }
                    }
                    
                    buy_orders.peek().cloned()
                };
                
                match best_buy {
                    Some(buy) => {
                        // Check if they can be matched
                        let can_match = match (buy.price, order.price) {
                            // Market sell matches any buy
                            (_, None) => true,
                            // Limit sell matches if buy price >= sell price
                            (Some(buy_price), Some(sell_price)) => buy_price >= sell_price,
                            // Market buy doesn't match limit sell
                            (None, Some(_)) => false,
                        };
                        
                        if can_match {
                            // Calculate match quantity
                            let match_qty = remaining_qty.min(buy.quantity - buy.filled_quantity);
                            remaining_qty -= match_qty;
                            
                            // Create a fill - use buy price for market sells
                            let fill_price = buy.price.unwrap_or(self.last_price.unwrap_or(0.0));
                            
                            let fill = OrderFill {
                                id: Uuid::new_v4().to_string(),
                                order_id: order.id.clone(),
                                price: fill_price,
                                quantity: match_qty,
                                timestamp: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64,
                            };
                            
                            // Update order statuses
                            self.update_order_status(&order.id, match_qty, fill_price);
                            self.update_order_status(&buy.id, match_qty, fill_price);
                            
                            // Remove fully filled buy order
                            let buy_filled = {
                                let buy_order = self.order_map.read().unwrap().get(&buy.id).cloned();
                                match buy_order {
                                    Some(o) => o.filled_quantity >= o.quantity,
                                    None => false,
                                }
                            };
                            
                            if buy_filled {
                                let mut buy_orders = self.buy_orders.write().unwrap();
                                buy_orders.pop();
                            }
                            
                            // Add fill to results
                            fills.push(fill.clone());
                            
                            // Add to fill queue
                            let mut fill_queue = self.fill_queue.write().unwrap();
                            fill_queue.push_back(fill);
                            
                            // Update last price
                            self.last_price = Some(fill_price);
                        } else {
                            // Cannot match anymore
                            break;
                        }
                    }
                    None => break, // No more buy orders
                }
            }
        }
        
        fills
    }
    
    /// Execute a match between two orders
    fn execute_match(&mut self, buy: Order, sell: Order) -> Vec<OrderFill> {
        let mut fills = Vec::new();
        
        // Calculate match quantity
        let buy_remaining = buy.quantity - buy.filled_quantity;
        let sell_remaining = sell.quantity - sell.filled_quantity;
        let match_qty = buy_remaining.min(sell_remaining);
        
        // Determine price (use limit price from the resting order)
        let match_price = match (buy.price, sell.price) {
            (Some(p), None) => p, // Market sell matches limit buy
            (None, Some(p)) => p, // Market buy matches limit sell
            (Some(_), Some(p)) => p, // Both limit orders, use seller's price
            (None, None) => self.last_price.unwrap_or(0.0), // Both market, use last price
        };
        
        // Create fills
        let buy_fill = OrderFill {
            id: Uuid::new_v4().to_string(),
            order_id: buy.id.clone(),
            price: match_price,
            quantity: match_qty,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        
        let sell_fill = OrderFill {
            id: Uuid::new_v4().to_string(),
            order_id: sell.id.clone(),
            price: match_price,
            quantity: match_qty,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        
        // Update order statuses
        self.update_order_status(&buy.id, match_qty, match_price);
        self.update_order_status(&sell.id, match_qty, match_price);
        
        // Remove filled orders from queues
        let buy_filled = buy_remaining <= match_qty;
        let sell_filled = sell_remaining <= match_qty;
        
        if buy_filled {
            let mut buy_orders = self.buy_orders.write().unwrap();
            buy_orders.pop();
        }
        
        if sell_filled {
            let mut sell_orders = self.sell_orders.write().unwrap();
            sell_orders.pop();
        }
        
        // Add to fill queue
        {
            let mut fill_queue = self.fill_queue.write().unwrap();
            fill_queue.push_back(buy_fill.clone());
            fill_queue.push_back(sell_fill.clone());
        }
        
        // Add fills to results
        fills.push(buy_fill);
        fills.push(sell_fill);
        
        // Update last price
        self.last_price = Some(match_price);
        
        fills
    }
    
    /// Update an order's status after a match
    fn update_order_status(&mut self, order_id: &str, match_qty: f64, match_price: f64) {
        let mut order_map = self.order_map.write().unwrap();
        
        if let Some(order) = order_map.get_mut(order_id) {
            // Update filled quantity
            order.filled_quantity += match_qty;
            
            // Update status
            if order.filled_quantity >= order.quantity {
                order.status = "Filled".to_string();
            } else {
                order.status = "PartiallyFilled".to_string();
            }
        }
    }
} 