# Quick Reference: Opening Dates

## 🎯 Best Approach (One Command)

```python
from src.unified_opening_date_pipeline import unified_opening_date_pipeline

result = unified_opening_date_pipeline("data/raw/london_pois.csv")
```

**This combines:**
- Wikidata (for POIs with names)
- Building age (for properties via UPRN)
- Planning (for new builds)
- Heritage (for listed buildings)

**Result:** 70-90% coverage with confidence scores

## 📊 Method Comparison

| Method | Best For | Coverage | Accuracy |
|--------|----------|----------|----------|
| **Unified Pipeline** | Everything | 70-90% | High-Medium |
| Wikidata only | POIs with names | 30-40% | High |
| Building age only | Properties | 60-80% | Medium |

## 🚀 Quick Start

```bash
# Unified pipeline (best)
python -m src.unified_opening_date_pipeline --input data/raw/london_pois.csv

# Wikidata only (fast)
python -m src.pipeline

# Building age only (broad coverage)
python -c "
from src.building_opening_date_estimator import BuildingOpeningDateEstimator
estimator = BuildingOpeningDateEstimator(use_wikidata=True)
result = estimator.estimate_opening_dates(df, poi_name_col='name')
"
```

## 📚 Full Guides

- [Best Approach](docs/BEST_APPROACH_OPENING_DATES.md) - Complete guide
- [All Methods](docs/OPENING_DATE_METHODS.md) - 10 methods analyzed
- [Best Practices](docs/OPENING_DATE_BEST_PRACTICES.md) - Best practices

