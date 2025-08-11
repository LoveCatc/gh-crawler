"""Performance monitoring and metrics collection."""

import time
import threading
import psutil
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from loguru import logger

from .config import ENABLE_PERFORMANCE_MONITORING, PERFORMANCE_LOG_INTERVAL, CPU_USAGE_THRESHOLD, QUIET_MODE


@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""
    start_time: float = field(default_factory=time.time)
    repositories_processed: int = 0
    prs_scraped: int = 0
    requests_made: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    errors_encountered: int = 0
    
    # CPU and memory metrics
    cpu_usage_samples: List[float] = field(default_factory=list)
    memory_usage_samples: List[float] = field(default_factory=list)
    
    # Throughput metrics
    repositories_per_minute: float = 0.0
    prs_per_minute: float = 0.0
    requests_per_minute: float = 0.0
    
    def update_throughput(self):
        """Update throughput metrics based on elapsed time."""
        elapsed_minutes = (time.time() - self.start_time) / 60.0
        if elapsed_minutes > 0:
            self.repositories_per_minute = self.repositories_processed / elapsed_minutes
            self.prs_per_minute = self.prs_scraped / elapsed_minutes
            self.requests_per_minute = self.requests_made / elapsed_minutes


class PerformanceMonitor:
    """Monitor and log performance metrics."""
    
    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.monitoring_active = ENABLE_PERFORMANCE_MONITORING
        self.log_interval = PERFORMANCE_LOG_INTERVAL
        self.cpu_threshold = CPU_USAGE_THRESHOLD
        
        self._lock = threading.Lock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        
        if self.monitoring_active:
            self.start_monitoring()
    
    def start_monitoring(self):
        """Start the performance monitoring thread."""
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._stop_monitoring.clear()
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()
            logger.debug("Performance monitoring started")
    
    def stop_monitoring(self):
        """Stop the performance monitoring thread."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._stop_monitoring.set()
            self._monitor_thread.join(timeout=5)
            logger.debug("Performance monitoring stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop that runs in a separate thread."""
        while not self._stop_monitoring.wait(self.log_interval):
            try:
                self._collect_system_metrics()
                self._log_performance_summary()
            except Exception as e:
                logger.warning(f"Error in performance monitoring: {e}")
    
    def _collect_system_metrics(self):
        """Collect system-level performance metrics."""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            with self._lock:
                self.metrics.cpu_usage_samples.append(cpu_percent)
                self.metrics.memory_usage_samples.append(memory_percent)
                
                # Keep only recent samples (last 10 minutes)
                max_samples = 600 // self.log_interval
                if len(self.metrics.cpu_usage_samples) > max_samples:
                    self.metrics.cpu_usage_samples = self.metrics.cpu_usage_samples[-max_samples:]
                if len(self.metrics.memory_usage_samples) > max_samples:
                    self.metrics.memory_usage_samples = self.metrics.memory_usage_samples[-max_samples:]
                
        except Exception as e:
            logger.debug(f"Failed to collect system metrics: {e}")
    
    def _log_performance_summary(self):
        """Log a summary of current performance metrics."""
        # Skip logging in quiet mode unless there are significant metrics
        if QUIET_MODE:
            with self._lock:
                # Only log if we have meaningful activity
                if self.metrics.repositories_processed == 0 and self.metrics.prs_scraped == 0:
                    return

        with self._lock:
            self.metrics.update_throughput()

            # Calculate averages
            avg_cpu = sum(self.metrics.cpu_usage_samples) / len(self.metrics.cpu_usage_samples) if self.metrics.cpu_usage_samples else 0
            avg_memory = sum(self.metrics.memory_usage_samples) / len(self.metrics.memory_usage_samples) if self.metrics.memory_usage_samples else 0

            # Calculate cache hit rate
            total_cache_ops = self.metrics.cache_hits + self.metrics.cache_misses
            cache_hit_rate = (self.metrics.cache_hits / total_cache_ops * 100) if total_cache_ops > 0 else 0

            elapsed_time = time.time() - self.metrics.start_time

            # Use more concise logging format
            logger.info(f"📊 Progress: {self.metrics.repositories_processed} repos, {self.metrics.prs_scraped:,} PRs, {self.metrics.requests_per_minute:.0f} req/min, CPU: {avg_cpu:.0f}%")

            # Only show detailed metrics if not in quiet mode
            if not QUIET_MODE:
                logger.info(f"   ⏱️  Runtime: {elapsed_time/60:.1f} minutes")
                logger.info(f"   💾 Cache Hit Rate: {cache_hit_rate:.1f}%")
                logger.info(f"   🧠 Memory Usage: {avg_memory:.1f}% (avg)")

            # Always show CPU warnings
            if avg_cpu < self.cpu_threshold:
                logger.warning(f"⚠️  CPU usage ({avg_cpu:.1f}%) is below threshold ({self.cpu_threshold}%) - consider increasing workers")
    
    def increment_repositories(self, count: int = 1):
        """Increment the repositories processed counter."""
        with self._lock:
            self.metrics.repositories_processed += count
    
    def increment_prs(self, count: int = 1):
        """Increment the PRs scraped counter."""
        with self._lock:
            self.metrics.prs_scraped += count
    
    def increment_requests(self, count: int = 1):
        """Increment the requests made counter."""
        with self._lock:
            self.metrics.requests_made += count
    
    def increment_cache_hits(self, count: int = 1):
        """Increment the cache hits counter."""
        with self._lock:
            self.metrics.cache_hits += count
    
    def increment_cache_misses(self, count: int = 1):
        """Increment the cache misses counter."""
        with self._lock:
            self.metrics.cache_misses += count
    
    def increment_errors(self, count: int = 1):
        """Increment the errors encountered counter."""
        with self._lock:
            self.metrics.errors_encountered += count
    
    def get_metrics_summary(self) -> Dict:
        """Get a summary of current metrics."""
        with self._lock:
            self.metrics.update_throughput()
            
            avg_cpu = sum(self.metrics.cpu_usage_samples) / len(self.metrics.cpu_usage_samples) if self.metrics.cpu_usage_samples else 0
            avg_memory = sum(self.metrics.memory_usage_samples) / len(self.metrics.memory_usage_samples) if self.metrics.memory_usage_samples else 0
            
            total_cache_ops = self.metrics.cache_hits + self.metrics.cache_misses
            cache_hit_rate = (self.metrics.cache_hits / total_cache_ops * 100) if total_cache_ops > 0 else 0
            
            return {
                'runtime_minutes': (time.time() - self.metrics.start_time) / 60.0,
                'repositories_processed': self.metrics.repositories_processed,
                'prs_scraped': self.metrics.prs_scraped,
                'requests_made': self.metrics.requests_made,
                'repositories_per_minute': self.metrics.repositories_per_minute,
                'prs_per_minute': self.metrics.prs_per_minute,
                'requests_per_minute': self.metrics.requests_per_minute,
                'cache_hit_rate': cache_hit_rate,
                'avg_cpu_usage': avg_cpu,
                'avg_memory_usage': avg_memory,
                'errors_encountered': self.metrics.errors_encountered
            }


# Global performance monitor instance
_performance_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    return _performance_monitor


def log_final_performance_summary():
    """Log a final performance summary."""
    monitor = get_performance_monitor()
    summary = monitor.get_metrics_summary()
    
    logger.info("🎯 FINAL PERFORMANCE SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total Runtime: {summary['runtime_minutes']:.1f} minutes")
    logger.info(f"Repositories Processed: {summary['repositories_processed']}")
    logger.info(f"PRs Scraped: {summary['prs_scraped']:,}")
    logger.info(f"Total Requests: {summary['requests_made']:,}")
    logger.info(f"Average Throughput:")
    logger.info(f"  - {summary['repositories_per_minute']:.1f} repositories/minute")
    logger.info(f"  - {summary['prs_per_minute']:.1f} PRs/minute")
    logger.info(f"  - {summary['requests_per_minute']:.1f} requests/minute")
    logger.info(f"Cache Hit Rate: {summary['cache_hit_rate']:.1f}%")
    logger.info(f"Average CPU Usage: {summary['avg_cpu_usage']:.1f}%")
    logger.info(f"Average Memory Usage: {summary['avg_memory_usage']:.1f}%")
    logger.info(f"Errors Encountered: {summary['errors_encountered']}")
    logger.info("=" * 50)
