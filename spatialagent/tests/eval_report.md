# NL2Spatial Evaluation Report

**Overall: 41/47 passed (87%)**

## Results by Tier

| Tier | Total | Passed | Failed | Rate |
|------|-------|--------|--------|------|
| 1 | 15 | 12 | 3 | 80% |
| 2 | 15 | 13 | 2 | 87% |
| 3 | 17 | 16 | 1 | 94% |

## Results by Category

| Category | Total | Passed | Failed | Rate |
|----------|-------|--------|--------|------|
| aggregation | 4 | 2 | 2 | 50% |
| ambiguous | 3 | 3 | 0 | 100% |
| buffer | 3 | 3 | 0 | 100% |
| complex | 3 | 3 | 0 | 100% |
| compound | 2 | 2 | 0 | 100% |
| discovery | 8 | 5 | 3 | 62% |
| export | 2 | 2 | 0 | 100% |
| materialization | 2 | 1 | 1 | 50% |
| preview | 2 | 2 | 0 | 100% |
| proximity | 4 | 4 | 0 | 100% |
| spatial_filter | 1 | 1 | 0 | 100% |
| spatial_join | 4 | 4 | 0 | 100% |
| stats | 5 | 5 | 0 | 100% |
| temporal | 4 | 4 | 0 | 100% |

## Per-Query Detail

| ID | Tier | Category | Intent | Tool | SQL | Exec | Status |
|----|------|----------|--------|------|-----|------|--------|
| Q1 | 1 | discovery | ok | ok | - | ok | PASS |
| Q2 | 1 | discovery | ok | ok | - | ok | PASS |
| Q3 | 1 | discovery | ok | ok | - | ok | PASS |
| Q4 | 1 | discovery | ok | MISS | - | ok | FAIL |
| Q5 | 1 | discovery | ok | MISS | - | ok | FAIL |
| Q6 | 1 | discovery | ok | MISS | - | ok | FAIL |
| Q7 | 1 | discovery | ok | ok | - | ok | PASS |
| Q8 | 1 | preview | ok | ok | - | ok | PASS |
| Q9 | 1 | stats | ok | ok | - | ok | PASS |
| Q10 | 1 | stats | ok | ok | - | ok | PASS |
| Q11 | 1 | preview | ok | ok | - | ok | PASS |
| Q12 | 1 | stats | ok | ok | - | ok | PASS |
| Q13 | 1 | stats | ok | ok | - | ok | PASS |
| Q14 | 1 | stats | ok | ok | - | ok | PASS |
| Q17 | 1 | spatial_filter | ok | - | ok | ok | PASS |
| Q20 | 2 | proximity | ok | - | ok | ok | PASS |
| Q21 | 2 | proximity | ok | - | ok | ok | PASS |
| Q22 | 2 | proximity | ok | - | ok | ok | PASS |
| Q24 | 2 | proximity | ok | - | ok | ok | PASS |
| Q25 | 2 | spatial_join | ok | - | ok | ok | PASS |
| Q26 | 2 | spatial_join | ok | - | ok | ok | PASS |
| Q27 | 2 | spatial_join | ok | - | ok | ok | PASS |
| Q28 | 2 | spatial_join | ok | - | ok | ok | PASS |
| Q29 | 2 | buffer | ok | - | ok | ok | PASS |
| Q30 | 2 | buffer | ok | - | ok | ok | PASS |
| Q31 | 2 | buffer | ok | - | ok | ok | PASS |
| Q32 | 2 | aggregation | ok | - | ok | ok | PASS |
| Q33 | 2 | aggregation | ok | - | ok | ok | PASS |
| Q34 | 2 | aggregation | ok | - | MISS | ok | FAIL |
| Q35 | 2 | aggregation | ok | - | ok | FAIL | FAIL |
| Q36 | 3 | complex | ok | - | ok | ok | PASS |
| Q37 | 3 | complex | ok | - | ok | ok | PASS |
| Q38 | 3 | complex | ok | - | ok | ok | PASS |
| Q39 | 3 | temporal | ok | - | - | ok | PASS |
| Q40 | 3 | temporal | ok | ok | - | ok | PASS |
| Q41 | 3 | temporal | ok | ok | - | ok | PASS |
| Q42 | 3 | temporal | ok | ok | - | ok | PASS |
| Q43 | 3 | export | ok | ok | - | ok | PASS |
| Q44 | 3 | export | ok | ok | - | ok | PASS |
| Q45 | 3 | materialization | ok | - | ok | ok | PASS |
| Q46 | 3 | materialization | ok | - | MISS | ok | FAIL |
| Q47 | 3 | ambiguous | ok | - | ok | ok | PASS |
| Q48 | 3 | ambiguous | ok | - | ok | ok | PASS |
| Q49 | 3 | ambiguous | ok | - | ok | ok | PASS |
| Q50 | 3 | discovery | ok | ok | - | ok | PASS |
| Q51 | 3 | compound | ok | - | ok | ok | PASS |
| Q52 | 3 | compound | ok | - | ok | ok | PASS |