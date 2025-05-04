/*!
# Resources Module

This module defines resource management abstractions for ChopFlow.

Resource management is essential for:
- Expressing task requirements (CPU, GPU, memory, etc.)
- Tracking worker capabilities and available resources
- Making intelligent scheduling decisions
- Preventing resource overcommitment
- Monitoring resource utilization

Key components include:
- `ResourceRequirements` - specifies what resources a task needs
- `ResourceAvailability` - tracks a worker's current resource state
- `ResourceUsageMetrics` - provides utilization statistics

The module enables ChopFlow to make resource-aware scheduling decisions,
ensuring tasks are only assigned to workers capable of executing them
and preventing resource contention.
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Resource requirements for a task
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    /// Map of resource name to required amount
    pub resources: HashMap<String, u32>,
}

impl ResourceRequirements {
    /// Create a new empty set of resource requirements
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    /// Add a resource requirement
    pub fn add(&mut self, resource: impl Into<String>, amount: u32) {
        self.resources.insert(resource.into(), amount);
    }

    /// Create from resource requirements
    pub fn with_resource(resource: impl Into<String>, amount: u32) -> Self {
        let mut requirements = Self::new();
        requirements.add(resource, amount);
        requirements
    }

    /// Check if these requirements can be satisfied by the given availability
    pub fn can_be_satisfied_by(&self, availability: &ResourceAvailability) -> bool {
        for (resource, required) in &self.resources {
            if let Some(available) = availability.available.get(resource) {
                if *required > *available {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

/// Available resources on a worker
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceAvailability {
    /// Map of resource name to available amount
    pub available: HashMap<String, u32>,

    /// Map of resource name to total amount
    pub total: HashMap<String, u32>,
}

impl ResourceAvailability {
    /// Create a new empty resource availability
    pub fn new() -> Self {
        Self {
            available: HashMap::new(),
            total: HashMap::new(),
        }
    }

    /// Add a resource
    pub fn add_resource(&mut self, resource: impl Into<String>, amount: u32) {
        let resource = resource.into();
        self.available.insert(resource.clone(), amount);
        self.total.insert(resource, amount);
    }

    /// Allocate resources
    pub fn allocate(&mut self, requirements: &ResourceRequirements) -> bool {
        // First check if we can satisfy the requirements
        if !requirements.can_be_satisfied_by(self) {
            return false;
        }

        // Then allocate the resources
        for (resource, required) in &requirements.resources {
            if let Some(available) = self.available.get_mut(resource) {
                *available -= *required;
            }
        }

        true
    }

    /// Release resources
    pub fn release(&mut self, requirements: &ResourceRequirements) {
        for (resource, amount) in &requirements.resources {
            if let Some(available) = self.available.get_mut(resource) {
                let total = self.total.get(resource).unwrap_or(&0);
                *available = std::cmp::min(*available + *amount, *total);
            }
        }
    }
}

/// Resource usage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageMetrics {
    /// Map of resource name to usage percentage (0.0 to 1.0)
    pub usage: HashMap<String, f64>,

    /// Timestamp of the metrics
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ResourceUsageMetrics {
    /// Create a new set of resource usage metrics
    pub fn new() -> Self {
        Self {
            usage: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }
    }

    /// Calculate usage metrics from availability
    pub fn from_availability(availability: &ResourceAvailability) -> Self {
        let mut usage = HashMap::new();

        for (resource, total) in &availability.total {
            if let Some(available) = availability.available.get(resource) {
                if *total > 0 {
                    let usage_value = 1.0 - (*available as f64 / *total as f64);
                    usage.insert(resource.clone(), usage_value);
                }
            }
        }

        Self {
            usage,
            timestamp: chrono::Utc::now(),
        }
    }
}
