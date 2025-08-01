# 🔧 PR Crawling Configuration Guide

This guide explains how to configure the number of PRs to crawl for each repository.

## 📍 Configuration Options

### **Option 1: Global Default (Simplest)**

**File**: `src/config.py`
**Line**: 24

```python
MAX_CLOSED_PRS_TO_CRAWL = 2000  # Default for all repositories
```

**Usage**: This sets the default limit for ALL repositories.

---

### **Option 2: Per-Repository Configuration (Recommended)**

**File**: `src/config.py`
**Lines**: 29-36

```python
REPOSITORY_PR_LIMITS = {
    "https://github.com/apache/tvm": 3000,           # Large active project
    "https://github.com/facebook/react": 1000,      # Very active, recent PRs most important
    "https://github.com/microsoft/vscode": 5000,    # Huge project, need more history
    "https://github.com/small/project": 500,        # Small project, fewer PRs needed
}
```

**Usage**: 
- Add entries for specific repositories that need different limits
- Uses exact repository URLs as keys
- Overrides the global default for specified repositories

---

### **Option 3: Command Line Override**

**Command**: 
```bash
python main.py -i input.json -s 1000 --max-closed-prs 1500
```

**Usage**: Overrides both global and per-repository settings for this run.

---

## 🎯 Recommended Configurations

### **By Repository Size**

| Repository Type | Recommended Limit | Reasoning |
|----------------|-------------------|-----------|
| **Small Projects** | 500-1,000 | Limited history, recent PRs most relevant |
| **Medium Projects** | 1,500-2,500 | Good balance of history and efficiency |
| **Large Projects** | 3,000-5,000 | Rich history needed for analysis |
| **Mega Projects** | 5,000+ | Comprehensive historical data |

### **By Activity Level**

| Activity Level | Recommended Limit | Reasoning |
|---------------|-------------------|-----------|
| **Very Active** | 1,000-2,000 | Recent PRs change rapidly, focus on latest |
| **Moderately Active** | 2,000-3,000 | Balanced approach |
| **Less Active** | 3,000+ | Longer history needed for patterns |

### **By Analysis Purpose**

| Purpose | Recommended Limit | Reasoning |
|---------|-------------------|-----------|
| **Recent Trends** | 1,000-1,500 | Focus on current development patterns |
| **Historical Analysis** | 3,000-5,000 | Need broader historical context |
| **Complete Archive** | 10,000+ | Comprehensive data collection |

---

## 📝 Configuration Examples

### **Example 1: Mixed Project Types**

```python
# In src/config.py
MAX_CLOSED_PRS_TO_CRAWL = 2000  # Default

REPOSITORY_PR_LIMITS = {
    # Large, very active projects - focus on recent
    "https://github.com/facebook/react": 1500,
    "https://github.com/microsoft/vscode": 1500,
    
    # Medium projects - balanced approach
    "https://github.com/apache/tvm": 2500,
    "https://github.com/pytorch/pytorch": 2500,
    
    # Research projects - need more history
    "https://github.com/tensorflow/tensorflow": 4000,
    
    # Small projects - fewer PRs needed
    "https://github.com/small/utility": 800,
}
```

### **Example 2: Research Focus**

```python
# For comprehensive research - more history
MAX_CLOSED_PRS_TO_CRAWL = 3000

REPOSITORY_PR_LIMITS = {
    # Key projects need extensive history
    "https://github.com/apache/spark": 5000,
    "https://github.com/kubernetes/kubernetes": 4000,
    
    # Supporting projects - standard amount
    # (will use default 3000)
}
```

### **Example 3: Quick Analysis**

```python
# For quick analysis - focus on recent activity
MAX_CLOSED_PRS_TO_CRAWL = 1000

REPOSITORY_PR_LIMITS = {
    # Only very important projects get more
    "https://github.com/critical/project": 2000,
}
```

---

## 🚀 Quick Setup Instructions

### **Step 1: Choose Your Strategy**
- **Quick Analysis**: Set global default to 1,000-1,500
- **Balanced Research**: Set global default to 2,000-2,500  
- **Comprehensive Study**: Set global default to 3,000+

### **Step 2: Configure Specific Repositories**
1. Open `src/config.py`
2. Find the `REPOSITORY_PR_LIMITS` dictionary (line ~29)
3. Add entries for repositories that need different limits:
   ```python
   REPOSITORY_PR_LIMITS = {
       "https://github.com/your/repo": 1500,
       "https://github.com/another/repo": 3000,
   }
   ```

### **Step 3: Test Your Configuration**
```bash
# Test with a small repository first
python main.py -i test_input.json -s 1000

# Check the logs to see the applied limits:
# "Strategy: Latest X closed PRs + all open PRs"
```

---

## 📊 Performance Impact

| PR Limit | Estimated Time | Storage | Use Case |
|----------|---------------|---------|----------|
| 500 | ~10 minutes | ~50MB | Quick analysis |
| 1,000 | ~20 minutes | ~100MB | Standard research |
| 2,000 | ~40 minutes | ~200MB | **Recommended default** |
| 3,000 | ~60 minutes | ~300MB | Comprehensive analysis |
| 5,000+ | ~100+ minutes | ~500MB+ | Full historical study |

*Times are estimates for medium-sized repositories with the aggressive crawler*

---

## 🔄 Dynamic Configuration

You can also modify limits during runtime by updating the config and restarting the crawler. The resume functionality will pick up where it left off with the new limits.

---

## ❓ FAQ

**Q: What happens if I set the limit higher than the total PRs?**
A: The crawler will simply get all available PRs (no error).

**Q: Can I set different limits for open vs closed PRs?**
A: Currently, the limit only applies to closed PRs. ALL open PRs are always crawled.

**Q: How do I disable PR crawling entirely?**
A: Set `CRAWL_CLOSED_PRS = False` and `CRAWL_OPEN_PRS = False` in config.py.

**Q: Can I change limits mid-crawl?**
A: Yes! Update the config and restart. The resume system will continue with new limits.
