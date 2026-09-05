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

impl Default for ResourceUsageMetrics {
    fn default() -> Self {
        Self::new()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn avail(cpu: u32, gpu: u32) -> ResourceAvailability {
        let mut a = ResourceAvailability::new();
        a.add_resource("cpu", cpu);
        a.add_resource("gpu", gpu);
        a
    }

    #[test]
    fn can_be_satisfied_by_checks_each_resource() {
        let a = avail(4, 1);
        let ok = ResourceRequirements {
            resources: [("cpu".to_string(), 4), ("gpu".to_string(), 1)].into(),
        };
        assert!(ok.can_be_satisfied_by(&a));

        let too_much = ResourceRequirements {
            resources: [("cpu".to_string(), 8)].into(),
        };
        assert!(!too_much.can_be_satisfied_by(&a));

        // Missing resource entirely -> not satisfied.
        let missing = ResourceRequirements {
            resources: [("tpu".to_string(), 1)].into(),
        };
        assert!(!missing.can_be_satisfied_by(&a));
    }

    #[test]
    fn allocate_then_release_restores_available() {
        let mut a = avail(4, 1);
        let req = ResourceRequirements {
            resources: [("cpu".to_string(), 3)].into(),
        };

        assert!(a.allocate(&req));
        assert_eq!(a.available.get("cpu"), Some(&1));

        a.release(&req);
        assert_eq!(a.available.get("cpu"), Some(&4)); // back to total
    }

    #[test]
    fn allocate_fails_without_starving() {
        let mut a = avail(2, 0);
        let req = ResourceRequirements {
            resources: [("cpu".to_string(), 5)].into(),
        };
        // Failing allocation must not mutate available.
        assert!(!a.allocate(&req));
        assert_eq!(a.available.get("cpu"), Some(&2));
    }

    #[test]
    fn release_clamps_to_total() {
        let mut a = avail(4, 1);
        // Release more than was ever allocated; must not exceed total.
        let req = ResourceRequirements {
            resources: [("cpu".to_string(), 10)].into(),
        };
        a.release(&req);
        assert_eq!(a.available.get("cpu"), Some(&4));
    }

    #[test]
    fn usage_metrics_from_availability() {
        let mut a = avail(4, 1);
        let req = ResourceRequirements {
            resources: [("cpu".to_string(), 3)].into(),
        };
        a.allocate(&req);
        let m = ResourceUsageMetrics::from_availability(&a);
        // cpu: 3/4 used -> 0.75
        assert!((m.usage["cpu"] - 0.75).abs() < 1e-9);
        // gpu: 0/1 used -> 0.0
        assert!((m.usage["gpu"] - 0.0).abs() < 1e-9);
    }
}
